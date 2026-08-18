"""Account Sync Daemon: consumes ib:account:stream:v1 and persists to PostgreSQL.

FSM: IDLE → CONNECTING → RUNNING → STOPPING → STOPPED
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Any, Optional

import psycopg2
import redis as redis_lib

from bifrost_worker.daemon.account_sync.diff_engine import AccountSyncDiffEngine
from bifrost_worker.daemon.account_sync.redis_keys import ACCOUNT_SYNC_HEALTH_KEY

logger = logging.getLogger(__name__)


class AccountSyncDaemon:
    """Independent daemon that syncs Account/Position/Execution data from Redis Stream to PG."""

    def __init__(self, cfg: dict) -> None:
        self._cfg = cfg
        self.redis: Any = None
        self.pg_conn: Any = None
        self.golden_conn: Any = None
        self.diff_engine = AccountSyncDiffEngine()
        self.running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    def _connect_redis(self) -> Any:
        from bifrost_core.core.redis_url import effective_ib_redis_dict, format_redis_url

        url = format_redis_url(effective_ib_redis_dict(self._cfg, default_db=0))
        r = redis_lib.from_url(url, decode_responses=True)
        r.ping()
        logger.info("[AccountSync] Redis connected: %s", url.split("@")[-1] if "@" in url else url)
        return r

    def _connect_pg(self) -> Any:
        from bifrost_core.persistence.postgres.connection import (
            _get_conn_params,
            _is_lock_timeout_error,
            release_pg_locks_for_tables,
        )
        from bifrost_core.persistence.postgres.ddl import _ensure_tables

        params = _get_conn_params(self._cfg)
        last_err: Exception | None = None
        for attempt in (1, 2, 3):
            conn: Any = None
            try:
                conn = psycopg2.connect(**params)
                with conn.cursor() as cur:
                    cur.execute("SET lock_timeout = '5s'")
                    cur.execute("SET idle_in_transaction_session_timeout = '60s'")
                conn.commit()
                try:
                    _ensure_tables(conn)
                except Exception as ddl_err:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    err_s = str(ddl_err).lower()
                    if _is_lock_timeout_error(ddl_err):
                        # Another service (e.g. trading daemon) may be running DDL; schema usually exists.
                        logger.warning(
                            "[AccountSync] DDL ensure skipped (lock timeout); continuing with existing schema"
                        )
                    elif "must be owner" in err_s or "permission denied" in err_s:
                        # Legacy public tables may still be owned by postgres during cutover.
                        logger.warning(
                            "[AccountSync] DDL ensure skipped (insufficient privilege); continuing with existing schema: %s",
                            ddl_err,
                        )
                    else:
                        raise
                logger.info(
                    "[AccountSync] PostgreSQL connected: %s@%s:%s/%s",
                    params["user"], params["host"], params["port"], params["dbname"],
                )
                return conn
            except Exception as e:
                last_err = e
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                if attempt < 3 and _is_lock_timeout_error(e):
                    release_pg_locks_for_tables(self._cfg)
                    time.sleep(0.5 * attempt)
                    continue
                raise
        if last_err is not None:
            raise last_err
        raise RuntimeError("[AccountSync] PG connect failed without exception")

    def _connect_golden(self) -> Any:
        from bifrost_core.persistence.postgres.connection import _get_golden_source_conn_params

        params = _get_golden_source_conn_params(self._cfg)
        conn = psycopg2.connect(**{**params, "connect_timeout": 10})
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET idle_in_transaction_session_timeout = '60s'")
        conn.commit()
        try:
            from bifrost_core.persistence.postgres.brokerage_ddl import ensure_brokerage_schema

            ensure_brokerage_schema(conn)
            conn.commit()
        except Exception as ddl_err:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.debug("[AccountSync] ensure_brokerage_schema (best-effort): %s", ddl_err)
        logger.info(
            "[AccountSync] golden_source connected: %s@%s:%s/%s",
            params["user"], params["host"], params["port"], params["dbname"],
        )
        return conn

    def _ensure_pg(self) -> bool:
        if self.pg_conn is not None:
            try:
                if getattr(self.pg_conn, "closed", 0):
                    self.pg_conn = None
                else:
                    self.pg_conn.rollback()
                    return True
            except Exception:
                try:
                    self.pg_conn.close()
                except Exception:
                    pass
                self.pg_conn = None
        try:
            self.pg_conn = self._connect_pg()
            return True
        except Exception as e:
            logger.error("[AccountSync] PG reconnect failed: %s", e)
            return False

    def _ensure_golden(self) -> bool:
        if self.golden_conn is not None:
            try:
                if getattr(self.golden_conn, "closed", 0):
                    self.golden_conn = None
                else:
                    self.golden_conn.rollback()
                    return True
            except Exception:
                try:
                    self.golden_conn.close()
                except Exception:
                    pass
                self.golden_conn = None
        try:
            self.golden_conn = self._connect_golden()
            return True
        except Exception as e:
            logger.error("[AccountSync] golden_source reconnect failed: %s", e)
            return False

    def _seed_run_status(self) -> None:
        """Ensure account_sync_run_status has its single row."""
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO account_sync_run_status (id, suspended, heartbeat_interval_sec, updated_at) "
                    "VALUES (1, false, 5.0, now()) ON CONFLICT (id) DO NOTHING"
                )
            self.pg_conn.commit()
        except Exception as e:
            logger.debug("seed_run_status: %s", e)
            try:
                self.pg_conn.rollback()
            except Exception:
                pass

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._request_stop)
            except NotImplementedError:
                pass

        logger.info("[AccountSync] IDLE → CONNECTING")
        try:
            self.redis = self._connect_redis()
        except Exception as e:
            logger.error("[AccountSync] Redis connect failed: %s", e)
            return
        pg_connected = False
        for pg_attempt in range(1, 16):
            try:
                self.pg_conn = self._connect_pg()
                pg_connected = True
                break
            except Exception as e:
                logger.warning(
                    "[AccountSync] PG connect attempt %s/15 failed: %s",
                    pg_attempt,
                    e,
                )
                if pg_attempt >= 15:
                    logger.error("[AccountSync] PG connect failed after retries: %s", e)
                    self._write_health(alive=False)
                    return
                await asyncio.sleep(min(2.0 * pg_attempt, 10.0))
        if not pg_connected:
            self._write_health(alive=False)
            return

        try:
            self.golden_conn = self._connect_golden()
        except Exception as e:
            logger.error("[AccountSync] golden_source connect failed: %s", e)
            self._write_health(alive=False)
            return

        self._seed_run_status()

        logger.info("[AccountSync] CONNECTING → RUNNING")
        self.running = True

        self._write_health(alive=True)

        from bifrost_worker.daemon.account_sync.heartbeat import heartbeat_loop

        self._heartbeat_task = asyncio.create_task(heartbeat_loop(self))

        try:
            while self.running:
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

        logger.info("[AccountSync] RUNNING → STOPPING")
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        self._write_health(alive=False)

        if self.pg_conn is not None:
            try:
                self.pg_conn.close()
            except Exception:
                pass
        if self.golden_conn is not None:
            try:
                self.golden_conn.close()
            except Exception:
                pass
        if self.redis is not None:
            try:
                self.redis.close()
            except Exception:
                pass
        logger.info("[AccountSync] STOPPED")

    def _request_stop(self) -> None:
        logger.info("[AccountSync] stop requested")
        self.running = False

    def _write_health(self, *, alive: bool) -> None:
        try:
            self.redis.hset(
                ACCOUNT_SYNC_HEALTH_KEY,
                mapping={
                    "alive": "1" if alive else "0",
                    "updated_at": str(time.time()),
                },
            )
        except Exception as e:
            # Common cause: redis-ib ACL missing ~bifrost:health:daemon_* (trade-dev is read-only).
            logger.warning("[AccountSync] write_health failed: %s", e)
