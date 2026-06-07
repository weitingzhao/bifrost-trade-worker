"""Control poll, run_status, and heartbeat loop. Used by GsTrading."""

import asyncio
import logging
import time
from typing import Any, Optional

from bifrost_core.core.ops_lease import maintain_health_host, ops_profile_from_config
from bifrost_core.core.redis_health_keys import BIFROST_HEALTH_DAEMON_TRADING_ENGINE
from bifrost_worker.daemon.fsm.daemon_fsm import DaemonState

logger = logging.getLogger(__name__)

# Chunked sleep so DB-written `stop` is visible within ~1s instead of up to full heartbeat interval.
HEARTBEAT_SLEEP_CHUNK_SEC = 1.0


def _maintain_trading_engine_host(app: Any) -> None:
    """Restore bifrost_ops_control_host on the trading engine health hash if lost (e.g. Redis restart)."""
    rq = getattr(app, "_redis_quotes_reader", None)
    if rq is None or not rq.available:
        return
    r = rq.redis_client
    if r is None:
        return
    cfg = getattr(app, "config", None) or getattr(app, "_config", None) or {}
    ops_profile = ops_profile_from_config(cfg)
    maintain_health_host(r, BIFROST_HEALTH_DAEMON_TRADING_ENGINE, ops_profile)


def poll_control(app: Any) -> Optional[str]:
    """Poll control command from sink (PostgreSQL daemon_control table when sink is postgres). Return stop/flatten or None."""
    if app._status_sink is None:
        return None
    if hasattr(app._status_sink, "poll_and_consume_control"):
        return app._status_sink.poll_and_consume_control()
    return None


def poll_run_status(app: Any) -> tuple[bool, Optional[float]]:
    """Poll daemon_run_status from sink (suspended, heartbeat_interval_sec). interval None => use config default.
    Default suspended=True when no sink."""
    if app._status_sink is None:
        logger.debug("[Daemon] poll_run_status: no _status_sink → suspended=True, interval=None (default)")
        return True, None
    if hasattr(app._status_sink, "poll_run_status"):
        out = app._status_sink.poll_run_status()
        logger.debug("[Daemon] poll_run_status (from sink) → suspended=%s, interval=%s", out[0], out[1])
        return out
    logger.debug("[Daemon] poll_run_status: sink has no poll_run_status → suspended=True, interval=None")
    return True, None


def effective_heartbeat_interval(app: Any) -> float:
    """Heartbeat interval in seconds (from DB if set via monitoring, else config); clamped to [5, 120]."""
    raw = (
        app._heartbeat_interval_from_db
        if app._heartbeat_interval_from_db is not None
        else app._heartbeat_interval
    )
    return max(5.0, min(120.0, float(raw)))


def redis_quotes_connected(app: Any) -> bool:
    """Whether the daemon's Redis quotes *reader* is up. Live ticks are written by IB Ingestor; the
    daemon reads ``ib:ingester:tick:*`` (and related) via this client. DB column name unchanged."""
    return bool(
        getattr(app, "_redis_quotes_reader", None)
        and getattr(app._redis_quotes_reader, "available", False)
    )


def ib_edge_heartbeat_fields(app: Any) -> dict:
    """``ib_connected`` / ``ib_client_id`` from Socket IB Redis health (no in-process IB socket)."""
    rq = getattr(app, "_redis_quotes_reader", None)
    if rq is None or not getattr(rq, "available", False):
        return {"ib_connected": False, "ib_client_id": None}
    r = getattr(rq, "redis_client", None)
    if r is None:
        return {"ib_connected": False, "ib_client_id": None}
    cfg = getattr(app, "config", None) or getattr(app, "_config", None) or {}
    try:
        from bifrost_core.config.yaml_config import get_effective_ib_config
        from bifrost_core.monitor.integrations.daemon_ib_edge import (
            derive_daemon_ib_heartbeat_from_redis,
        )

        ib_cfg = get_effective_ib_config(cfg)
        return derive_daemon_ib_heartbeat_from_redis(r, ib_cfg)
    except Exception as e:
        logger.debug("ib_edge_heartbeat_fields: %s", e)
        return {"ib_connected": False, "ib_client_id": None}


def listener_heartbeat_kwargs(app: Any) -> dict:
    """Legacy heartbeat columns; no in-process Listener."""
    return {
        "listener_connected": False,
        "listener_client_id": None,
        "listener_2_connected": False,
        "listener_2_client_id": None,
    }


def apply_run_status_transition(app: Any) -> bool:
    """Sync Daemon FSM with daemon_run_status: RUNNING <-> RUNNING_SUSPENDED. Returns True if suspended (skip hedge)."""
    suspended, interval = poll_run_status(app)
    app._heartbeat_interval_from_db = interval
    cur = app._fsm_daemon.current
    logger.debug(
        "[Daemon] heartbeat | poll_run_status → suspended=%s, interval=%s, current=%s",
        suspended,
        interval,
        cur.value if cur else None,
    )
    if suspended and cur == DaemonState.RUNNING:
        app._fsm_daemon.transition(DaemonState.RUNNING_SUSPENDED)
        logger.info(
            "[Daemon] state=RUNNING → RUNNING_SUSPENDED (daemon_run_status.suspended=true)"
        )
    elif not suspended and cur == DaemonState.RUNNING_SUSPENDED:
        app._fsm_daemon.transition(DaemonState.RUNNING)
        logger.info(
            "[Daemon] state=RUNNING_SUSPENDED → RUNNING (daemon_run_status.suspended=false)"
        )
    return suspended


async def _consume_one_control_command(app: Any, cmd: Optional[str]) -> bool:
    """Apply effects of one consumed command. Returns True if heartbeat loop should exit (stop)."""
    if cmd is None:
        return False
    if cmd == "retry_ib":
        logger.debug(
            "[Daemon] control (db): retry_ib consumed (legacy no-op; engine has no IB socket)"
        )
        return False
    if cmd == "stop":
        logger.info("[Daemon] control (db): stop → requesting stop")
        app._fsm_daemon.request_stop()
        return True
    if cmd == "flatten":
        logger.warning("[Daemon] control (db): flatten (not implemented yet)")
        return False
    if cmd == "release_ib":
        logger.debug(
            "[Daemon] control (db): release_ib consumed (legacy no-op; no IB connections in daemon)"
        )
        return False
    if cmd == "refresh_accounts" and app._status_sink:
        logger.info(
            "[Daemon] control (db): refresh_accounts → loading from Redis snapshot"
        )
        await app._refresh_accounts_data()
        app._last_accounts_refresh_ts = time.time()
        minimal = app._build_heartbeat_minimal_dict()
        app._status_sink.write_snapshot(minimal, append_history=False)
        if not getattr(app, "mock_hedging", True):
            await app._refresh_position_prices()
            app._contract_quote_live_initialized = True
        return False
    if cmd == "refresh_replay" and app._status_sink:
        logger.info(
            "[Daemon] control (db): refresh_replay → loading executions from Redis snapshot"
        )
        await app._refresh_executions_only()
        return False
    if cmd == "refresh_ticker_subscriptions":
        logger.info(
            "[Daemon] control (db): refresh_ticker_subscriptions → report from Redis"
        )
        await app._refresh_ticker_subscriptions()
        return False
    if cmd == "release_ticker_subscriptions":
        logger.info(
            "[Daemon] control (db): release_ticker_subscriptions → clear reported list"
        )
        await app._release_ticker_subscriptions()
        return False
    if cmd == "init_ticker_subscriptions":
        logger.info(
            "[Daemon] control (db): init_ticker_subscriptions (no-op; use IB Ingestor)"
        )
        await app._init_ticker_subscriptions()
        return False
    return False


async def sleep_until_heartbeat_or_stop(app: Any, total_sec: float) -> bool:
    """Sleep for ``total_sec`` in small slices; poll control after each slice.

    Returns True if the heartbeat loop should exit (stop consumed, FSM no longer running, etc.).
    """
    remaining = float(total_sec)
    chunk = HEARTBEAT_SLEEP_CHUNK_SEC
    while remaining > 0:
        await asyncio.sleep(min(chunk, remaining))
        remaining -= min(chunk, remaining)
        if not app._fsm_daemon.is_running():
            return True
        cmd = poll_control(app)
        if await _consume_one_control_command(app, cmd):
            return True
    return False


async def heartbeat(app: Any) -> None:
    """Periodic heartbeat: snapshot, optional hedge, status writes."""
    while app._fsm_daemon.is_running():
        cmd = poll_control(app)
        if await _consume_one_control_command(app, cmd):
            return

        suspended = apply_run_status_transition(app)
        interval_sec = effective_heartbeat_interval(app)
        state_label = app._fsm_daemon.current.value
        if suspended:
            logger.info(
                "[Daemon] state=%s | heartbeat: sleep up to %.0fs (interruptible), skip maybe_hedge (suspended)",
                state_label,
                interval_sec,
            )
        else:
            logger.info(
                "[Daemon] state=%s | heartbeat: sleep up to %.0fs (interruptible), then maybe_hedge",
                state_label,
                interval_sec,
            )
        if await sleep_until_heartbeat_or_stop(app, interval_sec):
            return

        suspended = apply_run_status_transition(app)
        now_ts = time.time()
        if (
            now_ts - app._last_accounts_refresh_ts
            >= app._accounts_refresh_interval_sec
        ):
            await app._refresh_accounts_data()
            app._last_accounts_refresh_ts = now_ts

        if app._status_sink:
            result = await app._refresh_and_build_snapshot()
            if result is not None:
                snapshot, spot, cs, data_lag_ms = result
                snap_dict = app._build_snapshot_dict(
                    snapshot, spot, cs, data_lag_ms
                )
                app._status_sink.write_snapshot(snap_dict, append_history=False)
            else:
                logger.debug(
                    "Heartbeat: no full snapshot (spot unavailable), writing minimal status"
                )
                minimal = app._build_heartbeat_minimal_dict()
                app._status_sink.write_snapshot(minimal, append_history=False)
            if not getattr(app, "mock_hedging", True):
                if not getattr(app, "_contract_quote_live_initialized", False):
                    try:
                        await app._refresh_position_prices()
                        app._contract_quote_live_initialized = True
                    except Exception as e:
                        logger.debug(
                            "R-M6 initial refresh_position_prices: %s", e
                        )
                try:
                    if (
                        getattr(app, "_redis_quotes_reader", None)
                        and app._redis_quotes_reader.available
                    ):
                        app._sync_contract_quote_live_from_redis()
                    else:
                        await app._refresh_position_prices()
                except Exception as e:
                    logger.debug("R-M6 contract_quote_live sync failed: %s", e)
            if hasattr(app._status_sink, "write_daemon_heartbeat"):
                ib_kw = ib_edge_heartbeat_fields(app)
                app._status_sink.write_daemon_heartbeat(
                    hedge_running=True,
                    ib_connected=bool(ib_kw.get("ib_connected")),
                    ib_client_id=ib_kw.get("ib_client_id"),
                    heartbeat_interval_sec=effective_heartbeat_interval(app),
                    redis_quotes_connected=redis_quotes_connected(app),
                    mock_hedging=getattr(app, "mock_hedging", True),
                    **listener_heartbeat_kwargs(app),
                )
            _maintain_trading_engine_host(app)

        await app._refresh_ticker_subscriptions()
        if app._status_sink and hasattr(app._status_sink, "write_daemon_subscribed_tickers"):
            rq_read = getattr(app, "_redis_quotes_reader", None)
            if rq_read and rq_read.available:
                current = sorted(rq_read.get_subscribed_symbols())
                app._status_sink.write_daemon_subscribed_tickers(current)

        if not suspended:
            if getattr(app, "mock_hedging", True):
                logger.info(
                    "[Daemon] state=RUNNING | Mock: skip maybe_hedge (mock_hedging=true)"
                )
            else:
                logger.info(
                    "[Daemon] state=RUNNING | heartbeat: tick, running maybe_hedge"
                )
                await app._eval_hedge_sync()
