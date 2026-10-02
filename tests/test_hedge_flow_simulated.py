"""The hedge is simulated on one path and its decision is logged, not dropped (debt TD-66)."""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

from bifrost_worker.daemon.app import hedge_flow
from bifrost_worker.daemon.fsm.hedge_fsm import HedgeState
from bifrost_worker.daemon.fsm.trading_fsm import TradingEvent


def _app(paper: bool) -> MagicMock:
    app = MagicMock()
    app.paper_trade = paper
    app._hedge_cfg = {"min_hedge_shares": 1}
    app._fsm_hedge.state = HedgeState.SEND
    return app


def _run(app: MagicMock) -> None:
    intent = SimpleNamespace(target_shares=100, side="BUY", quantity=50)
    cs = SimpleNamespace(stock_pos=50, net_delta=48.0, D=None)
    snapshot = SimpleNamespace(data_lag_ms=0)
    asyncio.run(hedge_flow.hedge(app, intent, cs, 101.25, snapshot))


def test_both_flags_take_the_same_simulated_path(caplog) -> None:
    for paper in (True, False):
        app = _app(paper)
        with caplog.at_level(logging.INFO, logger=hedge_flow.logger.name):
            _run(app)
        assert "Simulated hedge (no IB orders): would BUY 50 shares" in caplog.text
        app._status_sink.write_operation.assert_not_called()
        assert app._fsm_trading.apply_transition.call_args.args[0] == TradingEvent.HEDGE_DONE
        app.store.inc_daily_hedge_count.assert_called_once()
        caplog.clear()
