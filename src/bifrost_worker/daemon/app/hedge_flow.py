"""Hedge evaluation and execution (eval_hedge, hedge). Used by GsTrading."""

import logging
import time
from typing import Any

from bifrost_worker.daemon.core.logging_utils import (
    log_composite_state,
    log_order_status,
    log_target_position,
)
from bifrost_worker.daemon.core.state.composite import CompositeState
from bifrost_worker.daemon.core.state.snapshot import StateSnapshot
from bifrost_worker.daemon.fsm.events import TargetPositionEvent, TradingEvent
from bifrost_worker.daemon.fsm.hedge_fsm import HedgeState
from bifrost_worker.daemon.fsm.trading_fsm import TradingState
from bifrost_worker.daemon.strategy.gamma_scalper import gamma_scalper_intent
from bifrost_worker.daemon.strategy.hedge_gate import apply_hedge_gates

logger = logging.getLogger(__name__)


async def eval_hedge(app: Any) -> None:
    """FSM-driven tick: refresh + snapshot -> TradingFSM (TICK) -> maybe hedge. Skips hedge when Redis trading state is suspended."""
    if app._poll_run_status()[0]:
        logger.debug("Trading suspended (Redis trading state), skip hedge")
        return
    result = await app._refresh_and_build_snapshot()
    if result is None:
        logger.debug("No spot price, skip hedge")
        return
    snapshot, spot, cs, data_lag_ms = result
    log_composite_state(cs=cs)
    app._metrics.set_data_lag_ms(data_lag_ms)
    app._metrics.set_delta_abs(abs(cs.net_delta))
    app._metrics.set_spread_bucket(cs.L.value if cs.L else None)

    app._fsm_trading.apply_transition(TradingEvent.TICK, snapshot)
    if app._fsm_trading.state != TradingState.NEED_HEDGE:
        return

    stock_shares = app.store.get_stock_position()
    intent = gamma_scalper_intent(
        cs.net_delta,
        stock_shares,
        threshold_hedge_shares=app._hedge_cfg["threshold_hedge_shares"],
        max_hedge_shares_per_order=app._hedge_cfg["max_hedge_shares_per_order"],
        config=app._hedge_cfg,
    )
    if intent is None:
        logger.debug("No hedge intent (delta within threshold)")
        return
    approved = apply_hedge_gates(
        intent,
        cs,
        app.guard,
        now_ts=time.time(),
        spot=spot,
        last_hedge_price=app.store.get_last_hedge_price(),
        spread_pct=app.store.get_spread_pct(),
        min_hedge_shares=app._hedge_cfg["min_hedge_shares"],
    )
    if approved is None:
        logger.info(
            "Hedge blocked by gates (delta=%.1f would %s %s)",
            cs.net_delta,
            intent.side,
            intent.quantity,
        )
        return
    if not app._fsm_hedge.can_place_order():
        logger.warning(
            "Execution not IDLE (E=%s), skip order",
            app._order_manager.effective_e_state().value,
        )
        return
    log_target_position(target_shares=intent.target_shares, cs=cs)

    # 3.c. The decision goes to the daemon log (the Console's daemon stream). The sink's
    # write_operation has been a no-op since Wave 1, so the trail used to vanish (TD-66).
    logger.info(
        "hedge_intent side=%s qty=%s price=%.4f state=%s",
        approved.side,
        approved.quantity,
        spot,
        cs.D.value if cs.D else None,
    )
    if app._status_sink:
        snap_dict = app._build_snapshot_dict(snapshot, spot, cs, data_lag_ms)
        app._status_sink.write_snapshot(snap_dict)

    # 3.d. FSM apply transition to target emitted and start hedge
    app._fsm_trading.apply_transition(TradingEvent.TARGET_EMITTED, snapshot)
    await hedge(app, approved, cs, spot, snapshot)


async def hedge(
    app: Any,
    intent: Any,
    cs: CompositeState,
    spot: float,
    snapshot: StateSnapshot,
) -> None:
    """Run HedgeFSM flow and place order; fire HEDGE_DONE or HEDGE_FAILED on TradingFSM."""
    now_ts = time.time()
    target_ev = TargetPositionEvent(
        target_shares=intent.target_shares,
        reason="delta_hedge",
        ts=now_ts,
        trace_id=None,
        side=intent.side,
        quantity=intent.quantity,
    )
    app._fsm_hedge.on_target(target_ev, cs.stock_pos)
    app._fsm_hedge.on_plan_decide(
        send_order=intent.quantity >= app._hedge_cfg["min_hedge_shares"]
    )
    if app._fsm_hedge.state != HedgeState.SEND:
        app._fsm_trading.apply_transition(TradingEvent.HEDGE_DONE, snapshot)
        return

    # The hedge is always simulated: the daemon has no order path (D10), and the paper and
    # mock branches that used to sit here differed only in their log text (TD-66).
    log_order_status(
        order_status="simulated_send", side=intent.side, quantity=intent.quantity
    )
    logger.info(
        "Simulated hedge (no IB orders): would %s %s shares at %.4f (delta=%.1f)",
        intent.side,
        intent.quantity,
        spot,
        cs.net_delta,
    )
    app._fsm_hedge.on_order_placed()
    app._fsm_hedge.on_ack_ok()
    app.guard.record_hedge_sent()
    app.store.set_last_hedge_time(now_ts)
    app.store.set_last_hedge_price(spot)
    app.store.inc_daily_hedge_count()
    app._metrics.inc_hedge_count()
    app._fsm_hedge.on_full_fill()
    logger.info("Simulated hedge filled: %s %s shares", intent.side, intent.quantity)
    if app._status_sink:
        snap_dict = app._build_snapshot_dict(
            snapshot, spot, cs, snapshot.data_lag_ms
        )
        app._status_sink.write_snapshot(snap_dict)
    app._fsm_trading.apply_transition(TradingEvent.HEDGE_DONE, snapshot)
