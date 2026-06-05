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
        self.diff_engine = AccountSyncDiffEngine()
        self.running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    def _connect_redis(self) -> Any:
        from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url

        url = format_redis_url(effective_redis_dict(self._cfg, default_db=0))
        r = redis_lib.from_url(url, decode_responses=True)
        r.ping()
        logger.info("[AccountSync] Redis connected: %s", url.split("@")[-1] if "@" in url else url)
        return r

    def _connect_pg(self) -> Any:
        from bifrost_core.persistence.postgres.connection import _get_conn_params
        from bifrost_core.persistence.postgres.ddl import _ensure_tables

        params = _get_conn_params(self._cfg)
        conn = psycopg2.connect(**params)
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET idle_in_transaction_session_timeout = '60s'")
        conn.commit()
        _ensure_tables(conn)
        logger.info(
            "[AccountSync] PostgreSQL connected: %s@%s:%s/%s",
            params["user"], params["host"], params["port"], params["dbname"],
        )
        return conn

    def _ensure_pg(self) -> bool:
        if self.pg_conn is not None:
            try:
                self.pg_conn.rollback()
                return True
            except Exception:
                self.pg_conn = None
        try:
            self.pg_conn = self._connect_pg()
            return True
        except Exception as e:
            logger.error("[AccountSync] PG reconnect failed: %s", e)
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
        try:
            self.pg_conn = self._connect_pg()
        except Exception as e:
            logger.error("[AccountSync] PG connect failed: %s", e)
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
        except Exception:
            pass
