"""Daemon FSM state handlers, run() loop, and stop(). Used by GsTrading."""

import asyncio
import logging
import time
from typing import Any

from bifrost_worker.daemon.fsm.daemon_fsm import DaemonState
from bifrost_worker.daemon.fsm.events import TradingEvent

logger = logging.getLogger(__name__)


async def handle_idle(app: Any) -> DaemonState:
    """IDLE: ready to start. Always proceed to CONNECTING (suspended is applied after RUNNING via heartbeat)."""
    suspended, interval = app._poll_run_status()
    logger.debug(
        "[Daemon] state=IDLE | poll_run_status → suspended=%s, interval=%s; next state=CONNECTING",
        suspended,
        interval,
    )
    logger.info("[Daemon] state=IDLE → CONNECTING")
    return DaemonState.CONNECTING


async def handle_connecting(app: Any) -> DaemonState:
    """CONNECTING: no in-process IB; proceed to CONNECTED."""
    logger.info("[Daemon] state=CONNECTING → CONNECTED (no IB socket; Redis + Operator)")
    return DaemonState.CONNECTED


async def handle_connected(app: Any) -> DaemonState:
    """CONNECTED: fetch positions + spot, bootstrap TradingFSM (START/SYNCED). Transition to RUNNING."""
    if app._status_sink and hasattr(app._status_sink, "write_daemon_heartbeat"):
        app._status_sink.write_daemon_heartbeat(
            hedge_running=False,
            ib_connected=False,
            ib_client_id=None,
            heartbeat_interval_sec=app._effective_heartbeat_interval(),
            redis_quotes_connected=app._redis_quotes_connected(),
            mock_hedging=getattr(app, "mock_hedging", True),
            **app._listener_heartbeat_kwargs(),
        )
    logger.info(
        "[Daemon] state=CONNECTED | fetching account summary and positions, building snapshot..."
    )
    await app._refresh_accounts_data()
    app._last_accounts_refresh_ts = time.time()
    result = await app._refresh_and_build_snapshot()
    if result is not None:
        snapshot, spot, cs, data_lag_ms = result
        app._fsm_trading.apply_transition(TradingEvent.START, snapshot)
        app._fsm_trading.apply_transition(TradingEvent.SYNCED, snapshot)
        if app._status_sink:
            snap_dict = app._build_snapshot_dict(
                snapshot, spot, cs, data_lag_ms
            )
            app._status_sink.write_snapshot(snap_dict, append_history=False)
    else:
        if app._status_sink:
            app._status_sink.write_snapshot(
                app._build_heartbeat_minimal_dict(), append_history=False
            )
    logger.info("[Daemon] state=CONNECTED → RUNNING (bootstrap done)")
    return DaemonState.RUNNING


async def handle_running(app: Any) -> DaemonState:
    """RUNNING: accounts from Redis (IB Account Agent); no Listener."""
    logger.info(
        "[Daemon] state=RUNNING | Redis edge — accounts from IB Account Agent snapshot"
    )
    try:
        from bifrost_worker.daemon.ib_edge import refresh_accounts_from_redis_edge

        await refresh_accounts_from_redis_edge(app)
    except Exception as e:
        logger.warning("[Daemon] ib_edge initial refresh: %s", e)
    app._apply_run_status_transition()
    if app._status_sink:
        app._status_sink.write_snapshot(
            app._build_heartbeat_minimal_dict(), append_history=False
        )
        if hasattr(app._status_sink, "write_daemon_heartbeat"):
            listener_kw = app._listener_heartbeat_kwargs()
            app._status_sink.write_daemon_heartbeat(
                hedge_running=True,
                ib_connected=False,
                ib_client_id=None,
                heartbeat_interval_sec=app._effective_heartbeat_interval(),
                redis_quotes_connected=app._redis_quotes_connected(),
                mock_hedging=getattr(app, "mock_hedging", True),
                **listener_kw,
            )
    app._heartbeat_task = asyncio.create_task(app._heartbeat())
    app._config_reload_task = asyncio.create_task(app._reload_config_loop())
    try:
        while app._fsm_daemon.is_running():
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        pass
    return DaemonState.STOPPING


async def handle_stopping(app: Any) -> DaemonState:
    """STOPPING: cancel tasks, close Redis quotes. Transition to STOPPED."""
    logger.info(
        "[Daemon] state=STOPPING | cancelling tasks, closing Redis quotes..."
    )
    heartbeat_task = getattr(app, "_heartbeat_task", None)
    config_reload_task = getattr(app, "_config_reload_task", None)
    if heartbeat_task is not None:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("Heartbeat task raised before cancel: %s", e)
    if config_reload_task is not None:
        config_reload_task.cancel()
        try:
            await config_reload_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("Config reload task raised before cancel: %s", e)
    if getattr(app._status_sink, "close", None):
        try:
            app._status_sink.close()
        except Exception as e:
            logger.debug("Status sink close: %s", e)
    if getattr(app, "_redis_quotes_reader", None):
        try:
            app._redis_quotes_reader.close()
        except Exception as e:
            logger.debug("Redis quotes reader close: %s", e)
    logger.info("[Daemon] state=STOPPING → STOPPED (exit)")
    return DaemonState.STOPPED


def get_state_handlers(app: Any) -> dict:
    """Map state -> async handler that returns next state."""
    return {
        DaemonState.IDLE: lambda: handle_idle(app),
        DaemonState.CONNECTING: lambda: handle_connecting(app),
        DaemonState.CONNECTED: lambda: handle_connected(app),
        DaemonState.RUNNING: lambda: handle_running(app),
        DaemonState.STOPPING: lambda: handle_stopping(app),
    }


async def run(app: Any) -> None:
    """State-driven loop: run handler for current state, transition to returned state."""
    app._loop = asyncio.get_running_loop()
    handlers = get_state_handlers(app)
    logger.info(
        "[Daemon] started (state loop: IDLE → CONNECTING → CONNECTED → RUNNING → STOPPING → STOPPED)"
    )
    try:
        while app._fsm_daemon.current != DaemonState.STOPPED:
            current = app._fsm_daemon.current
            handler = handlers.get(current)
            if handler is None:
                logger.warning(
                    "[Daemon] state=%s | no handler; stopping", current.value
                )
                break
            try:
                next_state = await handler()
                if not app._fsm_daemon.transition(next_state):
                    logger.error(
                        "[Daemon] invalid transition %s → %s; stopping",
                        current.value,
                        next_state.value,
                    )
                    if app._fsm_daemon.can_transition_to(DaemonState.STOPPING):
                        app._fsm_daemon.transition(DaemonState.STOPPING)
                    break
            except Exception as e:
                logger.exception(
                    "[Daemon] state=%s handler raised: %s", current.value, e
                )
                if app._fsm_daemon.can_transition_to(DaemonState.STOPPING):
                    app._fsm_daemon.transition(DaemonState.STOPPING)
                else:
                    app._fsm_daemon.transition(DaemonState.STOPPED)
    finally:
        if app._fsm_daemon.current != DaemonState.STOPPED:
            if app._fsm_daemon.current != DaemonState.STOPPING:
                app._fsm_daemon.transition(DaemonState.STOPPING)
            try:
                await handle_stopping(app)
            except Exception as e:
                logger.exception("Cleanup (_handle_stopping) failed: %s", e)
            app._fsm_daemon.transition(DaemonState.STOPPED)


def stop(app: Any) -> None:
    app._fsm_daemon.request_stop()
