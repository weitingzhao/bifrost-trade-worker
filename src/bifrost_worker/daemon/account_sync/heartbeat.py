"""Heartbeat loop for Account Sync Daemon: consume stream → diff → write heartbeat."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Capped XREADGROUP block so `stop` in PG is visible within ~1s, not full heartbeat interval.
ACCOUNT_SYNC_MAX_BLOCK_MS = 1000
ACCOUNT_SYNC_SLEEP_CHUNK_SEC = 1.0


def _poll_control(conn: Any) -> Optional[str]:
    """Read and consume one pending command from account_sync_control."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, command FROM account_sync_control WHERE consumed_at IS NULL ORDER BY id LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return None
            cmd_id, cmd = row
            cur.execute("UPDATE account_sync_control SET consumed_at = now() WHERE id = %s", (cmd_id,))
        conn.commit()
        return cmd
    except Exception as e:
        logger.warning("poll_control: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def _poll_run_status(conn: Any) -> tuple[bool, float]:
    """Read account_sync_run_status: (suspended, heartbeat_interval_sec). Defaults: (False, 5.0)."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT suspended, heartbeat_interval_sec FROM account_sync_run_status WHERE id = 1")
            row = cur.fetchone()
            if row:
                return bool(row[0]), float(row[1] or 5.0)
    except Exception as e:
        logger.debug("poll_run_status: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    return False, 5.0


def _write_heartbeat(
    conn: Any,
    *,
    last_sync_version: int = 0,
    accounts_synced: int = 0,
    positions_synced: int = 0,
    executions_synced: int = 0,
    open_orders_synced: int = 0,
    stream_lag: int = 0,
) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO account_sync_heartbeat (id, last_ts, last_sync_version, accounts_synced, positions_synced, executions_synced, open_orders_synced, stream_lag, updated_at)
                VALUES (1, now(), %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    last_ts = now(),
                    last_sync_version = EXCLUDED.last_sync_version,
                    accounts_synced = EXCLUDED.accounts_synced,
                    positions_synced = EXCLUDED.positions_synced,
                    executions_synced = EXCLUDED.executions_synced,
                    open_orders_synced = EXCLUDED.open_orders_synced,
                    stream_lag = EXCLUDED.stream_lag,
                    updated_at = now()
                """,
                (last_sync_version, accounts_synced, positions_synced, executions_synced, open_orders_synced, stream_lag),
            )
        conn.commit()
    except Exception as e:
        logger.warning("write_heartbeat: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass


def _apply_consumed_control(
    app: Any, cmd: Optional[str], diff: Any
) -> bool:
    """Handle a consumed control row. Returns True if heartbeat_loop should exit."""
    if cmd == "stop":
        logger.info("[AccountSync] control: stop → requesting shutdown")
        app.running = False
        return True
    if cmd == "force_sync":
        logger.info("[AccountSync] control: force_sync → clearing diff cache")
        diff._account_cache.clear()
        diff._position_cache.clear()
        diff._seen_exec_ids.clear()
    return False


async def _sleep_account_sync_interruptible(app: Any, total_sec: float, diff: Any) -> bool:
    """Sleep up to ``total_sec`` in chunks; poll control after each chunk. Returns True to exit loop."""
    remaining = float(total_sec)
    chunk = ACCOUNT_SYNC_SLEEP_CHUNK_SEC
    while remaining > 0 and app.running:
        await asyncio.sleep(min(chunk, remaining))
        remaining -= min(chunk, remaining)
        cmd = _poll_control(app.pg_conn)
        if _apply_consumed_control(app, cmd, diff):
            return True
    return False


def _write_redis_health(
    r: Any, *, alive: bool, last_sync_version: int, stream_lag: int, ops_profile: Any = None
) -> None:
    from bifrost_worker.daemon.account_sync.redis_keys import ACCOUNT_SYNC_HEALTH_KEY
    from bifrost_core.core.ops_lease import maintain_health_host

    try:
        # HSET only updates the specified fields; other fields (bifrost_ops_control_*) are
        # preserved automatically — no manual read-back needed.
        r.hset(ACCOUNT_SYNC_HEALTH_KEY, mapping={
            "alive": "1" if alive else "0",
            "last_sync_version": str(last_sync_version),
            "stream_lag": str(stream_lag),
            "updated_at": str(time.time()),
        })
        # Restore HOST if lost (e.g. after Redis restart); no-op when HOST is present.
        maintain_health_host(r, ACCOUNT_SYNC_HEALTH_KEY, ops_profile)
    except Exception as e:
        logger.debug("write_redis_health: %s", e)


async def heartbeat_loop(app: Any) -> None:
    """Main heartbeat: XREADGROUP → diff → write heartbeat + health."""
    from bifrost_worker.daemon.account_sync.stream_consumer import AccountStreamConsumer
    from bifrost_core.core.ops_lease import ops_profile_from_config

    consumer = AccountStreamConsumer(app.redis)
    consumer.ensure_group()
    diff = app.diff_engine
    last_version = 0
    ops_profile = ops_profile_from_config(getattr(app, "_cfg", {}))

    while app.running:
        cmd = _poll_control(app.pg_conn)
        if _apply_consumed_control(app, cmd, diff):
            return

        suspended, interval_sec = _poll_run_status(app.pg_conn)
        interval_sec = max(2.0, min(60.0, interval_sec))

        if suspended:
            logger.info("[AccountSync] suspended — sleeping up to %.0fs (interruptible)", interval_sec)
            if await _sleep_account_sync_interruptible(app, interval_sec, diff):
                return
            _write_heartbeat(app.pg_conn, last_sync_version=last_version, stream_lag=0)
            _write_redis_health(app.redis, alive=True, last_sync_version=last_version, stream_lag=0, ops_profile=ops_profile)
            continue

        remaining_sec = float(interval_sec)
        entries: List[Dict[str, Any]] = []
        while remaining_sec > 0 and app.running:
            cmd = _poll_control(app.pg_conn)
            if _apply_consumed_control(app, cmd, diff):
                return
            cap_ms = min(ACCOUNT_SYNC_MAX_BLOCK_MS, int(remaining_sec * 1000))
            block_ms = max(1, cap_ms)
            entries = consumer.read(count=10, block_ms=block_ms)
            remaining_sec -= block_ms / 1000.0
            if entries:
                break

        latest = consumer.merge_latest(entries)

        if latest is not None:
            try:
                diff.sync_all(app.pg_conn, latest)
                last_version = int(latest.get("version") or 0)
            except Exception as e:
                logger.error("[AccountSync] sync_all failed: %s", e, exc_info=True)
                try:
                    app.pg_conn.rollback()
                except Exception:
                    pass

        stream_lag = consumer.pending_count()
        _write_heartbeat(
            app.pg_conn,
            last_sync_version=last_version,
            accounts_synced=diff.accounts_synced,
            positions_synced=diff.positions_synced,
            executions_synced=diff.executions_synced,
            open_orders_synced=diff.open_orders_synced,
            stream_lag=stream_lag,
        )
        _write_redis_health(app.redis, alive=True, last_sync_version=last_version, stream_lag=stream_lag, ops_profile=ops_profile)
