"""Integration tests: GsTrading with TradingFSM-driven flow (Redis edge; no IB connector)."""

import pytest

from bifrost_worker.daemon.app.gs_trading import GsTrading
from bifrost_worker.daemon.core.state.enums import TradingState


@pytest.fixture
def minimal_config():
    return {
        "server": {"skip_monitor_ib": True},
        "ib": {
            "host": {
                "ip": "127.0.0.1",
                "port_type": "tws_paper",
                "client_id": {
                    "daemon": 1,
                    "listener": 2,
                    "account": 100,
                    "markets": 101,
                    "worker_market": 500,
                },
            },
        },
        "greeks": {"risk_free_rate": 0.05, "volatility": 0.35},
        "gates": {
            "structure": {"min_dte": 21, "max_dte": 35, "atm_band_pct": 0.03},
            "hedge": {
                "cooldown_sec": 60,
                "max_daily_hedge_count": 20,
                "max_position_shares": 1000,
                "max_daily_loss_usd": 5000,
                "max_net_delta_shares": 500,
                "max_spread_pct": 0.5,
                "min_price_move_pct": 0.2,
                "threshold_hedge_shares": 25,
                "max_hedge_shares_per_order": 100,
                "min_hedge_shares": 10,
                "earnings_dates": [],
                "blackout_days_before": 0,
                "blackout_days_after": 0,
                "trading_hours_only": False,
            },
            "risk": {"paper_trade": True},
        },
        "order": {"order_type": "market"},
    }


def _nvda_positions_flat():
    return [
        {
            "contract": {"symbol": "NVDA", "secType": "STK"},
            "position": 0,
            "account": "DU1",
        }
    ]


async def _seed_accounts_nvda(app: GsTrading) -> None:
    """Simulate Redis account snapshot: NVDA stock, spot for snapshot."""
    app.store.set_accounts_data(
        [
            {
                "account_id": "DU1",
                "summary": {},
                "positions": [{"symbol": "NVDA", "secType": "STK", "position": 0}],
            }
        ]
    )
    app.store.set_account_summary("DU1", {})
    flat = _nvda_positions_flat()
    app.store.set_positions(flat, 0)
    app._set_active_symbol("NVDA")
    app.store.set_underlying_price(100.0)


@pytest.mark.asyncio
async def test_handle_connected_bootstraps_trading_fsm(minimal_config):
    """After _handle_connected, TradingFSM has left BOOT (START/SYNCED applied)."""
    app = GsTrading(minimal_config)

    async def refresh() -> None:
        await _seed_accounts_nvda(app)

    app._refresh_accounts_data = refresh  # type: ignore[method-assign]

    from bifrost_worker.daemon.fsm.daemon_fsm import DaemonState

    next_state = await app._handle_connected()
    assert next_state == DaemonState.RUNNING
    assert app.symbol == "NVDA"
    assert app._fsm_trading.state != TradingState.BOOT
    assert app._fsm_trading.state in (TradingState.IDLE, TradingState.SAFE, TradingState.SYNC)


@pytest.mark.asyncio
async def test_eval_hedge_runs_without_error(minimal_config):
    """_eval_hedge runs without exception and applies TICK to TradingFSM."""
    app = GsTrading(minimal_config)

    async def refresh() -> None:
        await _seed_accounts_nvda(app)

    app._refresh_accounts_data = refresh  # type: ignore[method-assign]

    await app._handle_connected()
    await app._eval_hedge()
    assert app._fsm_trading.state in (
        TradingState.IDLE,
        TradingState.SAFE,
        TradingState.SYNC,
        TradingState.ARMED,
        TradingState.MONITOR,
        TradingState.NO_TRADE,
        TradingState.PAUSE_COST,
        TradingState.PAUSE_LIQ,
    )
