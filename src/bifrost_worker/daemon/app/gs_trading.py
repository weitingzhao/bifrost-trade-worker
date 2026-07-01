"""Gamma Scalping strategy: connector -> state -> greeks -> scalper -> guard -> order."""

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional, Tuple

from bifrost_core.config.settings import (
    get_config_for_guards,
    get_hedge_config,
    get_structure_config,
    get_risk_config,
)
from bifrost_worker.daemon.core.metrics import get_metrics
from bifrost_worker.daemon.core.state.composite import CompositeState
from bifrost_worker.daemon.core.state.snapshot import StateSnapshot
from bifrost_worker.daemon.core.store import Store
from bifrost_worker.daemon.fsm.daemon_fsm import DaemonFSM, DaemonState
from bifrost_worker.daemon.execution.order_manager import OrderManager
from bifrost_worker.daemon.fsm.hedge_fsm import HedgeFSM
from bifrost_worker.daemon.fsm.trading_fsm import TradingFSM
from bifrost_worker.daemon.market.market_data import MarketData
from bifrost_core.portfolio.positions.position_book import PositionBook
from bifrost_worker.daemon.guards.execution_guard import ExecutionGuard
from bifrost_core.persistence.postgres.postgres_sink import PostgreSQLSink
from bifrost_core.persistence.status_sink import StatusSink
from bifrost_core.core.realtime import create_reader_from_config
from bifrost_core.config.startup import read_config
from bifrost_core.portfolio import accounts as _accounts
from bifrost_worker.daemon.app import snapshot as _snapshot
from bifrost_core.portfolio import symbol_position as _symbol_position
from bifrost_worker.daemon.app import control_heartbeat as _control_heartbeat
from bifrost_worker.daemon.app import hedge_flow as _hedge_flow
from bifrost_worker.daemon.app import daemon_handlers as _daemon_handlers
from bifrost_worker.daemon.app import contract_quote_live as _contract_quote_live
from bifrost_worker.daemon.app import ticker_redis as _ticker_redis

logger = logging.getLogger(__name__)


class GsTrading:
    """Single-process event-driven gamma scalping strategy."""

    def __init__(self, config: dict, config_path: Optional[str] = None):
        # 1.Init Config
        self.config = config
        self._config_path = config_path

        # 1.a PostgreSQL sink early (daemon_control, snapshots, settings)
        postgres_cfg = config.get("postgres", {}) or {}
        self._status_sink: Optional[StatusSink] = None
        if postgres_cfg or os.environ.get("PGHOST"):
            try:
                self._status_sink = PostgreSQLSink(config)
            except Exception as e:
                logger.warning("PostgreSQL sink init failed: %s", e)

        # 1.b No in-process IB: quotes and account state from Redis (Ingestor + Account Agent).
        self.connector = None
        self.listener_connector = None
        self.listener_connector_2 = None
        logger.info(
            "Engine: read-only IB edge — Redis quotes + account_agent; no order placement"
        )

        # Host account for hedging/market data (R-A4). Still from DB settings (not a client_id).
        self._host_account_id: Optional[str] = None
        db_ib = None
        if self._status_sink and hasattr(self._status_sink, "get_ib_connection_config"):
            db_ib = self._status_sink.get_ib_connection_config()
        if db_ib and db_ib.get("host_account_id"):
            self._host_account_id = str(db_ib["host_account_id"]).strip()
            logger.info("[R-A4] host_account_id=%s (for hedging and market data)", self._host_account_id)

        # 1.b Config sections (unified _*_cfg naming)
        self._structure_cfg = get_structure_config(config)
        self._risk_cfg = get_risk_config(config)
        self._greeks_cfg = config.get("greeks", {})

        # 1.c Active symbol is inferred from live positions; no fixed config symbol.
        self.symbol = ""
        self.paper_trade = True
        self.mock_hedging = True

        # 1.d Hedge Configuration
        self._hedge_cfg = get_hedge_config(config)
        self.guard = ExecutionGuard(
            cooldown_sec=self._hedge_cfg["cooldown_sec"],
            max_daily_hedge_count=self._hedge_cfg["max_daily_hedge_count"],
            max_position_shares=self._hedge_cfg["max_position_shares"],
            max_daily_loss_usd=self._hedge_cfg["max_daily_loss_usd"],
            max_net_delta_shares=self._hedge_cfg["max_net_delta_shares"],
            max_spread_pct=self._hedge_cfg["max_spread_pct"],
            min_price_move_pct=self._hedge_cfg["min_price_move_pct"],
            earnings_dates=self._hedge_cfg["earnings_dates"],
            blackout_days_before=self._hedge_cfg["blackout_days_before"],
            blackout_days_after=self._hedge_cfg["blackout_days_after"],
            trading_hours_only=self._hedge_cfg["trading_hours_only"],
        )

        # 1.e FSMs
        self._fsm_daemon = DaemonFSM()
        self._fsm_hedge = HedgeFSM(min_hedge_shares=self._hedge_cfg["min_hedge_shares"])
        self._fsm_trading = TradingFSM(
            config=get_config_for_guards(config),
            guard=self.guard,
            on_transition=None,
        )

        # 2. Object References
        self.store = Store()
        self._hedge_lock = asyncio.Lock()
        self._last_config_mtime: Optional[float] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._position_book = PositionBook(
            self.store,
            self.symbol,
            min_dte=self._structure_cfg.get("min_dte", 21),
            max_dte=self._structure_cfg.get("max_dte", 35),
            atm_band_pct=self._structure_cfg.get("atm_band_pct", 0.03),
        )
        self._market_data = MarketData(self.store)
        self._order_manager = OrderManager()
        # _status_sink already created in 1.a
        # Phase 2: control via PostgreSQL daemon_control table when sink is postgres (RE-5: monitoring can run on another host)
        self._order_manager.set_hedge_fsm(self._fsm_hedge)
        self._metrics = get_metrics()

        # 3. Static Defaults
        daemon_cfg = config.get("daemon") or {}
        self._heartbeat_interval = float(daemon_cfg.get("heartbeat_interval", 10.0))
        self._heartbeat_interval_from_db: Optional[float] = (
            None  # overrides when set via monitoring
        )
        self._config_reload_interval = 30.0
        # R-A1: 账户/持仓拉取（监控与对冲）不需每心跳拉取；每小时拉一次即可
        self._accounts_refresh_interval_sec = 3600.0
        self._last_accounts_refresh_ts = 0.0
        self._last_positions_refresh_ts = 0.0
        # R-M6: contract_quote_live from Redis quotes (IB Ingestor)
        self._contract_quote_live_initialized = False
        # Redis quotes: reader only — IB Ingestor writes ticks; daemon reads quote:/ib:ingester:tick:* (no daemon writer).
        self._redis_quotes_reader = create_reader_from_config(config)
        if getattr(self, "_redis_quotes_reader", None) and self._redis_quotes_reader.available:
            logger.info(
                "Redis quotes reader: connected (live ticks from IB Ingestor in Redis)"
            )
        else:
            logger.info(
                "Redis quotes reader: disabled or unavailable (redis.enabled and Redis required to read ingestor ticks)"
            )

    @staticmethod
    def _position_symbol_parts(item: Any) -> tuple[str, str]:
        """Extract (symbol, sec_type) from one position item."""
        return _symbol_position.position_symbol_parts(item)

    def _infer_active_symbol(self, positions: list[Any]) -> str:
        """Prefer option underlying symbol, then stock symbol, from current positions."""
        return _symbol_position.infer_active_symbol(self, positions)

    def _set_active_symbol(self, symbol: Optional[str]) -> None:
        """Switch the strategy symbol when live positions change."""
        _symbol_position.set_active_symbol(self, symbol)

    def _reload_config(self, config: dict) -> None:
        """Apply hot-reloadable config (IB host/port require restart)."""
        self.config = config

        self._structure_cfg = get_structure_config(config)
        self._hedge_cfg = get_hedge_config(config)
        self._greeks_cfg = config.get("greeks", self._greeks_cfg)
        self._risk_cfg = get_risk_config(config)
        self.paper_trade = True
        self.mock_hedging = True
        self.guard.update_config(
            cooldown_sec=self._hedge_cfg["cooldown_sec"],
            max_daily_hedge_count=self._hedge_cfg["max_daily_hedge_count"],
            max_position_shares=self._hedge_cfg["max_position_shares"],
            max_daily_loss_usd=self._hedge_cfg["max_daily_loss_usd"],
            max_net_delta_shares=self._hedge_cfg["max_net_delta_shares"],
            max_spread_pct=self._hedge_cfg["max_spread_pct"],
            min_price_move_pct=self._hedge_cfg["min_price_move_pct"],
            earnings_dates=self._hedge_cfg["earnings_dates"],
            blackout_days_before=self._hedge_cfg["blackout_days_before"],
            blackout_days_after=self._hedge_cfg["blackout_days_after"],
            trading_hours_only=self._hedge_cfg["trading_hours_only"],
        )
        # Recreate Redis quotes reader on config reload (e.g. Redis was down at daemon start)
        if getattr(self, "_redis_quotes_reader", None) is not None:
            try:
                self._redis_quotes_reader.close()
            except Exception:
                pass
            self._redis_quotes_reader = None
        self._redis_quotes_reader = create_reader_from_config(config)

    async def _reload_config_loop(self) -> None:
        """Periodically check config file mtime and reload if changed."""
        if not self._config_path or not Path(self._config_path).exists():
            return
        while self._fsm_daemon.is_running():
            await asyncio.sleep(self._config_reload_interval)
            if not self._fsm_daemon.is_running():
                return
            try:
                mtime = Path(self._config_path).stat().st_mtime
                if (
                    self._last_config_mtime is not None
                    and mtime > self._last_config_mtime
                ):
                    config, _ = read_config(self._config_path)
                    self._reload_config(config)
                    self._last_config_mtime = mtime
                    logger.info("Config reloaded from %s", self._config_path)
                elif self._last_config_mtime is None:
                    self._last_config_mtime = mtime
            except Exception as e:
                logger.debug("Config reload check failed: %s", e)

    async def _refresh_accounts_data(self) -> None:
        """R-A1: fetch all managed accounts' summary + positions from IB; store for monitoring and set primary account for trading."""
        await _accounts.refresh_accounts_data(self)

    async def _refresh_executions_only(self) -> None:
        """R-A2: 仅从 IB 拉取账户执行/成交并写入 account_executions，供复盘与风控 Tab 使用。"""
        await _accounts.refresh_executions_only(self)

    async def _refresh_positions(self) -> None:
        """Fetch positions from IB and update store (raw positions + stock_shares only)."""
        await _accounts.refresh_positions(self)

    def _build_snapshot(
        self,
        cs: CompositeState,
        spot: Optional[float],
        greeks: Optional[Any],
        option_legs_count: int = 0,
    ) -> StateSnapshot:
        """Build StateSnapshot from CompositeState for TradingFSM."""
        return _snapshot.build_snapshot(self, cs, spot, greeks, option_legs_count)

    def _build_snapshot_dict(
        self,
        snapshot: StateSnapshot,
        spot: float,
        cs: CompositeState,
        data_lag_ms: Optional[float],
    ) -> dict:
        """Build dict for StatusSink (daemon_auto_status_current / daemon_auto_status_history)."""
        return _snapshot.build_snapshot_dict(self, snapshot, spot, cs, data_lag_ms)

    def _build_heartbeat_minimal_dict(self) -> dict:
        """Minimal snapshot dict when spot is unavailable."""
        return _snapshot.build_heartbeat_minimal_dict(self)

    async def _refresh_and_build_snapshot(
        self,
    ) -> Optional[Tuple[StateSnapshot, float, CompositeState, Optional[float]]]:
        """Refresh positions and spot, parse legs, greeks, classify, build snapshot."""
        return await _snapshot.refresh_and_build_snapshot(self)

    def _on_ticker(self, ticker: Any) -> None:
        """Called on each ticker update from IB (may be from IB thread)."""
        _ticker_redis.on_ticker(self, ticker)

    def _on_ticker_for_symbol(self, symbol: str, ticker: Any) -> None:
        """Called on each ticker update from IB for a symbol (may be from IB thread)."""
        _ticker_redis.on_ticker_for_symbol(self, symbol, ticker)

    def _on_ticker_for_contract_key(self, contract_key: str, ticker: Any) -> None:
        """Called on each ticker update from IB for an option contract (Watchlist OPT). Writes to contract_quote_live."""
        _contract_quote_live.on_ticker_for_contract_key(self, contract_key, ticker)

    def _quote_payload_from_ticker(self, symbol: str, ticker: Any) -> Optional[dict]:
        """Build quote dict for Redis from ticker. Used for non-strategy symbols."""
        return _ticker_redis.quote_payload_from_ticker(symbol, ticker)

    def _quote_payload(self) -> Optional[dict]:
        """Build quote dict for Redis from store."""
        return _ticker_redis.quote_payload(self)

    def _eval_hedge_threadsafe(self) -> None:
        """Threadsafe: schedule _on_tick to be run safely from any thread."""
        _ticker_redis.eval_hedge_threadsafe(self)

    async def _eval_hedge_sync(self) -> None:
        """Run FSM-driven tick once (under lock)."""
        await _ticker_redis.eval_hedge_sync(self)

    async def _eval_hedge(self) -> None:
        """FSM-driven tick: refresh + snapshot -> TradingFSM (TICK) -> maybe _hedge."""
        await _hedge_flow.eval_hedge(self)

    async def _hedge(
        self,
        intent: Any,
        cs: CompositeState,
        spot: float,
        snapshot: StateSnapshot,
    ) -> None:
        """Run HedgeFSM flow and place order; fire HEDGE_DONE or HEDGE_FAILED on TradingFSM."""
        await _hedge_flow.hedge(self, intent, cs, spot, snapshot)

    def _poll_control(self) -> Optional[str]:
        """Poll control command from sink."""
        return _control_heartbeat.poll_control(self)

    def _poll_run_status(self) -> tuple[bool, Optional[float]]:
        """Poll daemon_run_status from sink."""
        return _control_heartbeat.poll_run_status(self)

    def _effective_heartbeat_interval(self) -> float:
        """Heartbeat interval in seconds (from DB if set via monitoring, else config)."""
        return _control_heartbeat.effective_heartbeat_interval(self)

    def _redis_quotes_connected(self) -> bool:
        """Whether the Redis quotes reader is connected (ingestor-fed keys; DB column redis_quotes_connected)."""
        return _control_heartbeat.redis_quotes_connected(self)

    def _listener_heartbeat_kwargs(self) -> dict:
        """Listener connection status for daemon_heartbeat."""
        return _control_heartbeat.listener_heartbeat_kwargs(self)

    def _apply_run_status_transition(self) -> bool:
        """Sync Daemon FSM with daemon_run_status: RUNNING <-> RUNNING_SUSPENDED."""
        return _control_heartbeat.apply_run_status_transition(self)

    async def _heartbeat(self) -> None:
        """Periodic heartbeat to run maybe_hedge and write status snapshot."""
        await _control_heartbeat.heartbeat(self)

    async def _refresh_ticker_subscriptions(self) -> None:
        """Sync = Release then Init: unsubscribe all, then subscribe to ideal set."""
        await _contract_quote_live.refresh_ticker_subscriptions(self)

    async def _release_ticker_subscriptions(self) -> None:
        """Unsubscribe all Real-time ticker subscriptions."""
        await _contract_quote_live.release_ticker_subscriptions(self)

    async def _init_ticker_subscriptions(self) -> None:
        """If no subscriptions: subscribe to watchlist + all positions. Else write error to last_control_message."""
        await _contract_quote_live.init_ticker_subscriptions(self)

    def _get_position_stk_instruments(self) -> dict:
        """From accounts_data aggregate STK instruments; return contract_key -> meta."""
        return _contract_quote_live.get_position_stk_instruments(self)

    async def _refresh_position_prices(self) -> None:
        """R-M6: fetch prices from IB and write contract_quote_live."""
        await _contract_quote_live.refresh_position_prices(self)

    def _sync_contract_quote_live_from_redis(self) -> None:
        """R-M6: update contract_quote_live from Redis quotes."""
        _contract_quote_live.sync_contract_quote_live_from_redis(self)

    # --- State handlers: each runs its logic and returns the next state ---

    async def _handle_idle(self) -> DaemonState:
        """IDLE: ready to start. Transition to CONNECTING."""
        return await _daemon_handlers.handle_idle(self)

    async def _handle_connecting(self) -> DaemonState:
        """CONNECTING: proceed to CONNECTED (no IB socket)."""
        return await _daemon_handlers.handle_connecting(self)

    async def _handle_connected(self) -> DaemonState:
        """CONNECTED: fetch positions + spot, bootstrap TradingFSM. Transition to RUNNING."""
        return await _daemon_handlers.handle_connected(self)

    async def _handle_running(self) -> DaemonState:
        """RUNNING: heartbeat loop until stop."""
        return await _daemon_handlers.handle_running(self)

    async def _handle_stopping(self) -> DaemonState:
        """STOPPING: cancel tasks, disconnect. Transition to STOPPED."""
        return await _daemon_handlers.handle_stopping(self)

    def _get_state_handlers(self) -> dict:
        """Map state -> async handler that returns next state."""
        return _daemon_handlers.get_state_handlers(self)

    async def run(self) -> None:
        """State-driven loop: run handler for current state, transition to returned state."""
        await _daemon_handlers.run(self)

    def stop(self) -> None:
        _daemon_handlers.stop(self)
