"""Snapshot and heartbeat dict building for TradingFSM and StatusSink. Used by GsTrading."""

import logging
import math
import time
from typing import Any, Optional, Tuple
from bifrost_core.config.settings import get_state_space_config
from bifrost_worker.daemon.core.state.classifier import StateClassifier
from bifrost_worker.daemon.core.state.composite import CompositeState
from bifrost_worker.daemon.core.state.snapshot import StateSnapshot, GreeksSnapshot
from bifrost_core.portfolio.positions.portfolio import get_option_legs
from bifrost_worker.daemon.pricing.greeks import Greeks

logger = logging.getLogger(__name__)


def build_snapshot(
    app: Any,
    cs: CompositeState,
    spot: Optional[float],
    greeks: Optional[Any],
    option_legs_count: int = 0,
) -> StateSnapshot:
    """Build StateSnapshot from CompositeState for TradingFSM."""
    gs = None
    if greeks is not None:
        gs = GreeksSnapshot(
            delta=getattr(greeks, "delta", 0.0),
            gamma=getattr(greeks, "gamma", 0.0),
            valid=getattr(greeks, "valid", False),
        )
    return StateSnapshot.from_composite_state(
        cs,
        spot=spot,
        greeks_snapshot=gs,
        option_legs_count=option_legs_count,
    )


def build_snapshot_dict(
    app: Any,
    snapshot: StateSnapshot,
    spot: float,
    cs: CompositeState,
    data_lag_ms: Optional[float],
) -> dict:
    """Build dict for StatusSink (daemon_auto_status_current / daemon_auto_status_history). Keys per docs/DATABASE.md §2.1. R-A1: optional account_* keys when available."""
    d = {
        "daemon_state": app._fsm_daemon.current.value,
        "trading_state": app._fsm_trading.state.value,
        "symbol": app.symbol or None,
        "spot": float(spot),
        "bid": app.store.get_bid(),
        "ask": app.store.get_ask(),
        "net_delta": float(cs.net_delta),
        "stock_position": int(cs.stock_pos),
        "option_legs_count": int(getattr(snapshot, "option_legs_count", 0)),
        "daily_hedge_count": app.store.get_daily_hedge_count(),
        "daily_pnl": float(app.store.get_daily_pnl()),
        "data_lag_ms": float(data_lag_ms) if data_lag_ms is not None else None,
        "config_summary": f"paper_trade={app.paper_trade}",
        "ts": time.time(),
    }
    # R-A1 optional: account summary
    acc = app.store.get_account_summary()
    if acc:
        d["account_id"] = app.store.get_account_id()
        try:
            d["account_net_liquidation"] = (
                float(acc.get("NetLiquidation"))
                if acc.get("NetLiquidation")
                else None
            )
        except (TypeError, ValueError):
            d["account_net_liquidation"] = None
        try:
            d["account_total_cash"] = (
                float(acc.get("TotalCashValue"))
                if acc.get("TotalCashValue")
                else None
            )
        except (TypeError, ValueError):
            d["account_total_cash"] = None
        try:
            d["account_buying_power"] = (
                float(acc.get("BuyingPower")) if acc.get("BuyingPower") else None
            )
        except (TypeError, ValueError):
            d["account_buying_power"] = None
    else:
        d["account_id"] = None
        d["account_net_liquidation"] = None
        d["account_total_cash"] = None
        d["account_buying_power"] = None
    # R-A1 multi-account: full list for monitoring (same level as 守护/对冲)
    accounts_data = app.store.get_accounts_data()
    d["accounts_snapshot"] = accounts_data if accounts_data else None
    if accounts_data:
        logger.debug(
            "[R-A1] _build_snapshot_dict accounts_snapshot len=%s",
            len(accounts_data),
        )
    return d


def build_heartbeat_minimal_dict(app: Any) -> dict:
    """Minimal snapshot dict when spot is unavailable (e.g. outside market hours). Ensures daemon_auto_status_current always has a row while daemon is running. R-A1: include account_* when available."""
    d = {
        "daemon_state": app._fsm_daemon.current.value,
        "trading_state": app._fsm_trading.state.value,
        "symbol": app.symbol or None,
        "spot": None,
        "bid": app.store.get_bid(),
        "ask": app.store.get_ask(),
        "net_delta": None,
        "stock_position": app.store.get_stock_position() or None,
        "option_legs_count": 0,
        "daily_hedge_count": app.store.get_daily_hedge_count(),
        "daily_pnl": (
            app.store.get_daily_pnl()
            if app.store.get_daily_pnl() is not None
            else None
        ),
        "data_lag_ms": None,
        "config_summary": f"paper_trade={app.paper_trade}",
        "ts": time.time(),
    }
    acc = app.store.get_account_summary()
    if acc:
        d["account_id"] = app.store.get_account_id()
        try:
            d["account_net_liquidation"] = (
                float(acc.get("NetLiquidation"))
                if acc.get("NetLiquidation")
                else None
            )
        except (TypeError, ValueError):
            d["account_net_liquidation"] = None
        try:
            d["account_total_cash"] = (
                float(acc.get("TotalCashValue"))
                if acc.get("TotalCashValue")
                else None
            )
        except (TypeError, ValueError):
            d["account_total_cash"] = None
        try:
            d["account_buying_power"] = (
                float(acc.get("BuyingPower")) if acc.get("BuyingPower") else None
            )
        except (TypeError, ValueError):
            d["account_buying_power"] = None
    else:
        d["account_id"] = None
        d["account_net_liquidation"] = None
        d["account_total_cash"] = None
        d["account_buying_power"] = None
    accounts_data = app.store.get_accounts_data()
    d["accounts_snapshot"] = accounts_data if accounts_data else None
    return d


async def refresh_and_build_snapshot(
    app: Any,
) -> Optional[Tuple[StateSnapshot, float, CompositeState, Optional[float]]]:
    """
    Refresh positions and spot, parse legs, greeks, classify, build snapshot.
    Returns (snapshot, spot, cs, data_lag_ms) or None if no valid spot.
    Shared by _handle_connected (bootstrap) and _eval_hedge (tick).
    Positions refresh on the same hourly interval as accounts to avoid requesting IB every heartbeat.
    """
    now_ts = time.time()
    if (
        now_ts - app._last_positions_refresh_ts
        >= app._accounts_refresh_interval_sec
    ):
        await app._refresh_positions()
        app._last_positions_refresh_ts = now_ts
    if not app.symbol:
        logger.debug("No active symbol in current positions, skip hedge evaluation")
        return None
    # 1.b. Get stock shares and spot price
    stock_shares = app.store.get_stock_position()
    spot = app.store.get_underlying_price()
    if spot is None or spot <= 0:
        rq = getattr(app, "_redis_quotes_reader", None)
        sym = (app.symbol or "").strip()
        if rq and getattr(rq, "available", False) and sym:
            q = rq.get_quote(sym)
            if q:
                try:
                    last = q.get("last")
                    bid = q.get("bid")
                    ask = q.get("ask")
                    last_f = float(last) if last is not None else None
                    if last_f is not None and math.isfinite(last_f) and last_f > 0:
                        spot = last_f
                        app.store.set_underlying_price(spot)
                    bid_f = float(bid) if bid is not None else None
                    ask_f = float(ask) if ask is not None else None
                    if (
                        bid_f is not None
                        and math.isfinite(bid_f)
                        and ask_f is not None
                        and math.isfinite(ask_f)
                    ):
                        app.store.set_underlying_quote(bid_f, ask_f)
                    qts = q.get("ts")
                    if qts is not None:
                        try:
                            app._market_data.set_last_ts(float(qts))
                        except (TypeError, ValueError):
                            app._market_data.touch_ts()
                    else:
                        app._market_data.touch_ts()
                except (TypeError, ValueError):
                    pass
    if spot is None or spot <= 0:
        return None
    # 1.c. Get option legs
    positions = app.store.get_positions()
    min_dte = app._structure_cfg.get("min_dte", 21)
    max_dte = app._structure_cfg.get("max_dte", 35)
    atm_band = app._structure_cfg.get("atm_band_pct", 0.03)
    legs = get_option_legs(
        positions,
        app.symbol,
        min_dte=min_dte,
        max_dte=max_dte,
        atm_band_pct=atm_band,
        spot=spot,
    )
    # 1.d. Get greeks
    r = app._greeks_cfg.get("risk_free_rate", 0.05)
    vol = app._greeks_cfg.get("volatility", 0.35)
    greeks = Greeks(legs, stock_shares, spot, r, vol)

    # 2.a. Build data lag
    data_lag_ms: Optional[float] = None
    if app._market_data.last_ts is not None:
        data_lag_ms = (time.time() - app._market_data.last_ts) * 1000.0

    # 2.b. Build Classify
    state_space_cfg = get_state_space_config(app.config)
    risk_halt = getattr(app.guard, "_circuit_breaker", False)
    cs = StateClassifier.classify(
        app._position_book,
        app._market_data,
        greeks,
        app._order_manager,
        last_hedge_price=app.store.get_last_hedge_price(),
        last_hedge_ts=app.store.get_last_hedge_time(),
        data_lag_ms=data_lag_ms,
        risk_halt=risk_halt,
        config=state_space_cfg,
    )
    # 2.c. Build snapshot
    snapshot = build_snapshot(app, cs, spot, greeks, option_legs_count=len(legs))

    # 3. Return snapshot, spot, cs, data_lag_ms
    return (snapshot, spot, cs, data_lag_ms)
