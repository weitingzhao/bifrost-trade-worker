"""Daemon entry: load config, register signals, run GsTrading. SIGTERM/SIGINT stop."""

import asyncio
import logging
import signal
from typing import Any, Optional

from bifrost_core.config.startup import read_config
from bifrost_worker.daemon.app.gs_trading import GsTrading
from bifrost_worker.daemon.lease import get_daemon_lease_settings, run_daemon_with_lease

logger = logging.getLogger(__name__)


def _inject_gates_from_db_if_configured(config: dict) -> dict:
    """When settings.active_gate_safety_strategy_id is set and postgres is configured, load gates from DB and merge into config. Returns config (possibly with config['gates'] overridden)."""
    if not config or (config.get("sink") != "postgres" and not config.get("postgres")):
        return config
    try:
        import psycopg2
        from bifrost_core.persistence.postgres.connection import _get_conn_params
        from bifrost_core.monitor.reader.gate_safety import get_active_gate_safety_strategy_id, get_gates_by_id
        params = _get_conn_params(config)
        conn = psycopg2.connect(**params)
        try:
            gid = get_active_gate_safety_strategy_id(conn)
            if gid is not None:
                gates = get_gates_by_id(conn, gid)
                if gates:
                    config = {**config, "gates": gates}
                    logger.info("[Daemon] loaded gates from DB (gate_safety_strategy_id=%s)", gid)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("[Daemon] could not load gates from DB: %s; using config file", e)
    return config


def _inject_structure_from_db_if_configured(config: dict) -> dict:
    """When settings.active_strategy_structure_id is set and postgres is configured, load structure row from DB and set config['active_strategy_structure']. Returns config (possibly with key set)."""
    if not config or (config.get("sink") != "postgres" and not config.get("postgres")):
        return config
    try:
        import psycopg2
        from bifrost_core.persistence.postgres.connection import _get_conn_params
        from bifrost_core.monitor.reader.gate_safety import get_active_strategy_structure_id
        from bifrost_core.monitor.reader.strategy import get_structure_by_id
        params = _get_conn_params(config)
        conn = psycopg2.connect(**params)
        try:
            sid = get_active_strategy_structure_id(conn)
            if sid is not None:
                row = get_structure_by_id(conn, sid)
                if row is not None:
                    config = {**config, "active_strategy_structure": row}
                    logger.info("[Daemon] loaded active structure from DB (strategy_structure_id=%s)", sid)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("[Daemon] could not load structure from DB: %s", e)
    return config


async def _run_gs_trading(app: GsTrading) -> None:
    """Run GsTrading with signal handlers wired to app.stop()."""
    loop = asyncio.get_running_loop()

    def _on_stop_signal(*_args: Any) -> None:
        logger.info(
            "[Daemon] received SIGTERM/SIGINT → requesting stop (RUNNING → STOPPING)"
        )
        loop.call_soon_threadsafe(app.stop)

    try:
        loop.add_signal_handler(signal.SIGTERM, _on_stop_signal)
    except (NotImplementedError, OSError):
        pass
    try:
        loop.add_signal_handler(signal.SIGINT, _on_stop_signal)
    except (NotImplementedError, OSError):
        pass
    try:
        await app.run()
    finally:
        if getattr(app, "_status_sink", None) and hasattr(
            app._status_sink, "write_daemon_graceful_shutdown"
        ):
            app._status_sink.write_daemon_graceful_shutdown()


async def _run_daemon_main(config_path: Optional[str] = None) -> None:
    """Load config, build GsTrading, run the FSM loop."""
    config, resolved_path = read_config(config_path)
    config = _inject_gates_from_db_if_configured(config)
    config = _inject_structure_from_db_if_configured(config)
    app = GsTrading(config, config_path=resolved_path)
    await _run_gs_trading(app)


def run_daemon(config_path: Optional[str] = None) -> None:
    """Entry: run the gamma scalping daemon (SIGTERM/SIGINT stop).

    When ``daemon.lease.enabled`` (or ``BIFROST_DAEMON_LEASE_ENABLED``) is set, only
    the K8s Lease holder runs the FSM trading loop (R-DV3 single active auto-trade).
    """
    config, _resolved = read_config(config_path)
    lease_settings = get_daemon_lease_settings(config)
    app_holder: dict[str, GsTrading] = {}

    async def _service() -> None:
        config_loaded, resolved_path = read_config(config_path)
        config_loaded = _inject_gates_from_db_if_configured(config_loaded)
        config_loaded = _inject_structure_from_db_if_configured(config_loaded)
        app = GsTrading(config_loaded, config_path=resolved_path)
        app_holder["app"] = app
        await _run_gs_trading(app)

    def _on_leadership_lost() -> None:
        app = app_holder.get("app")
        if app is not None:
            app.stop()

    run_daemon_with_lease(
        lease_settings,
        _service,
        on_stopped_leading=_on_leadership_lost,
    )
