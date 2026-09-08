"""Heartbeat loop for Account Sync Daemon: consume stream → diff → Redis IPC state."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from bifrost_core.persistence import redis_daemon_state as rds

logger = logging.getLogger(__name__)

# Capped XREADGROUP block so `stop` is visible within ~1s, not full heartbeat interval.
ACCOUNT_SYNC_MAX_BLOCK_MS = 1000
ACCOUNT_SYNC_SLEEP_CHUNK_SEC = 1.0

# A sync that fails on a schema or permission problem fails identically every
# time. Without a backoff the loop retried at the heartbeat interval for five
# hours on 2026-09-07 and wrote ~50k lines, evicting the whole 2000-entry
# console ring buffer — the daemon destroyed the evidence of its own failure.
SYNC_BACKOFF_BASE_SEC = 5.0
SYNC_BACKOFF_MAX_SEC = 300.0
SYNC_BACKOFF_MAX_SHIFT = 6
# Full traceback on the first failure and every Nth after it; one line between.
SYNC_TRACEBACK_EVERY = 100
# Redis HASH values are strings; keep the stored error short.
SYNC_ERROR_MAX_CHARS = 300


def _sync_backoff_sec(consecutive_failures: int) -> float:
    """Exponential backoff, capped. 0 failures means no extra delay."""
    if consecutive_failures <= 0:
        return 0.0
    shift = min(consecutive_failures - 1, SYNC_BACKOFF_MAX_SHIFT)
    return min(SYNC_BACKOFF_MAX_SEC, SYNC_BACKOFF_BASE_SEC * (2 ** shift))


def _poll_control(app: Any) -> Optional[str]:
    """Consume one pending command from Redis STREAM."""
    r = getattr(app, "redis_state", None)
    if r is None:
        return None
    return rds.consume_account_sync_control(r, block_ms=0)


def _poll_run_status(app: Any) -> tuple[bool, float]:
    """Read suspended / heartbeat_interval_sec from Redis state. Defaults: (False, 5.0)."""
    r = getattr(app, "redis_state", None)
    if r is None:
        return False, 5.0
    state = rds.read_account_sync_state(r) or {}
    suspended = bool(state.get("suspended", False))
    try:
        interval = float(state.get("heartbeat_interval_sec") or 5.0)
    except (TypeError, ValueError):
        interval = 5.0
    return suspended, interval


def _write_heartbeat(
    app: Any,
    *,
    last_sync_version: int = 0,
    accounts_synced: int = 0,
    positions_synced: int = 0,
    executions_synced: int = 0,
    open_orders_synced: int = 0,
    stream_lag: int = 0,
    alive: bool = True,
    sync_failures: int = 0,
    last_error: str = "",
    last_ok_ts: float = 0.0,
) -> None:
    r = getattr(app, "redis_state", None)
    if r is None:
        return
    now = time.time()
    fields = {
        "last_ts": now,
        "last_sync_version": last_sync_version,
        "accounts_synced": accounts_synced,
        "positions_synced": positions_synced,
        "executions_synced": executions_synced,
        "open_orders_synced": open_orders_synced,
        "stream_lag": stream_lag,
        "alive": alive,
        # Why it is not alive. Without these, "the loop is turning" and "the
        # sync is working" were the same bit, and 2000 straight failures read
        # as healthy on every status surface.
        "sync_failures": sync_failures,
        "last_error": last_error[:SYNC_ERROR_MAX_CHARS],
        "updated_at": now,
    }
    # Only a real sync sets this — a quiet loop is not a successful sync.
    if last_ok_ts:
        fields["last_ok_ts"] = last_ok_ts
    rds.write_account_sync_state(r, fields)
    # Keep legacy health hash for Ops lease / older readers
    _write_legacy_health(
        app.redis,
        alive=alive,
        last_sync_version=last_sync_version,
        stream_lag=stream_lag,
        ops_profile=getattr(app, "_ops_profile", None),
    )


def _apply_consumed_control(app: Any, cmd: Optional[str], diff: Any) -> bool:
    """Handle a consumed control. Returns True if heartbeat_loop should exit."""
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
        cmd = _poll_control(app)
        if _apply_consumed_control(app, cmd, diff):
            return True
    return False


def _write_legacy_health(
    r: Any, *, alive: bool, last_sync_version: int, stream_lag: int, ops_profile: Any = None
) -> None:
    from bifrost_worker.daemon.account_sync.redis_keys import ACCOUNT_SYNC_HEALTH_KEY
    from bifrost_core.core.ops_lease import maintain_health_host

    if r is None:
        return
    try:
        r.hset(
            ACCOUNT_SYNC_HEALTH_KEY,
            mapping={
                "alive": "1" if alive else "0",
                "last_sync_version": str(last_sync_version),
                "stream_lag": str(stream_lag),
                "updated_at": str(time.time()),
            },
        )
        maintain_health_host(r, ACCOUNT_SYNC_HEALTH_KEY, ops_profile)
    except Exception as e:
        logger.debug("write_legacy_health: %s", e)


async def heartbeat_loop(app: Any) -> None:
    """Main heartbeat: XREADGROUP → diff → write Redis state."""
    from bifrost_worker.daemon.account_sync.stream_consumer import AccountStreamConsumer
    from bifrost_core.core.ops_lease import ops_profile_from_config

    consumer = AccountStreamConsumer(app.redis)
    consumer.ensure_group()
    diff = app.diff_engine
    last_version = 0
    sync_failures = 0
    last_error = ""
    last_ok_ts = 0.0
    ops_profile = ops_profile_from_config(getattr(app, "_cfg", {}))
    app._ops_profile = ops_profile

    # Seed run defaults once if missing
    if app.redis_state is not None:
        state = rds.read_account_sync_state(app.redis_state) or {}
        if "suspended" not in state:
            rds.set_account_sync_run_status(app.redis_state, suspended=False, heartbeat_interval_sec=5.0)

    while app.running:
        if not app._ensure_pg():
            _write_heartbeat(app, last_sync_version=last_version, stream_lag=0, alive=False,
                             sync_failures=sync_failures, last_error=last_error,
                             last_ok_ts=last_ok_ts)
            await asyncio.sleep(2.0)
            continue

        cmd = _poll_control(app)
        if _apply_consumed_control(app, cmd, diff):
            return

        suspended, interval_sec = _poll_run_status(app)
        interval_sec = max(2.0, min(60.0, interval_sec))

        if suspended:
            logger.info("[AccountSync] suspended — sleeping up to %.0fs (interruptible)", interval_sec)
            if await _sleep_account_sync_interruptible(app, interval_sec, diff):
                return
            if not app._ensure_pg():
                _write_heartbeat(app, last_sync_version=last_version, stream_lag=0, alive=False,
                                 sync_failures=sync_failures, last_error=last_error,
                                 last_ok_ts=last_ok_ts)
                continue
            _write_heartbeat(app, last_sync_version=last_version, stream_lag=0,
                             alive=sync_failures == 0,
                             sync_failures=sync_failures, last_error=last_error,
                             last_ok_ts=last_ok_ts)
            continue

        remaining_sec = float(interval_sec)
        entries: List[Dict[str, Any]] = []
        while remaining_sec > 0 and app.running:
            if not app._ensure_pg():
                break
            cmd = _poll_control(app)
            if _apply_consumed_control(app, cmd, diff):
                return
            cap_ms = min(ACCOUNT_SYNC_MAX_BLOCK_MS, int(remaining_sec * 1000))
            block_ms = max(1, cap_ms)
            entries = consumer.read(count=10, block_ms=block_ms)
            remaining_sec -= block_ms / 1000.0
            if entries:
                break

        if not app._ensure_pg():
            _write_heartbeat(app, last_sync_version=last_version, stream_lag=0, alive=False,
                             sync_failures=sync_failures, last_error=last_error,
                             last_ok_ts=last_ok_ts)
            await asyncio.sleep(2.0)
            continue

        latest = consumer.merge_latest(entries)

        if latest is not None:
            try:
                if not app._ensure_golden():
                    raise RuntimeError("golden_source connection unavailable")
                diff.sync_all(app.golden_conn, latest)
                last_version = int(latest.get("version") or 0)
                if sync_failures:
                    logger.info(
                        "[AccountSync] sync_all recovered after %d consecutive failures",
                        sync_failures,
                    )
                sync_failures = 0
                last_error = ""
                last_ok_ts = time.time()
            except Exception as e:
                sync_failures += 1
                last_error = f"{type(e).__name__}: {e}"
                # One traceback is enough to diagnose; the rest is noise that
                # evicts the ring buffer.
                trace = sync_failures == 1 or sync_failures % SYNC_TRACEBACK_EVERY == 0
                logger.error(
                    "[AccountSync] sync_all failed (%d consecutive): %s",
                    sync_failures,
                    e,
                    exc_info=trace,
                )
                try:
                    if app.golden_conn is not None:
                        app.golden_conn.rollback()
                except Exception:
                    pass

        stream_lag = consumer.pending_count()
        _write_heartbeat(
            app,
            last_sync_version=last_version,
            accounts_synced=diff.accounts_synced,
            positions_synced=diff.positions_synced,
            executions_synced=diff.executions_synced,
            open_orders_synced=diff.open_orders_synced,
            stream_lag=stream_lag,
            # `alive` now means "the sync is working", not "the loop is turning".
            # It stays false until a sync succeeds again.
            alive=sync_failures == 0,
            sync_failures=sync_failures,
            last_error=last_error,
            last_ok_ts=last_ok_ts,
        )

        backoff = _sync_backoff_sec(sync_failures)
        if backoff:
            logger.debug("[AccountSync] backing off %.0fs after %d failures", backoff, sync_failures)
            if await _sleep_account_sync_interruptible(app, backoff, diff):
                return
