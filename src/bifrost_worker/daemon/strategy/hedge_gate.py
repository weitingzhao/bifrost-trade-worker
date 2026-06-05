"""Hedge gates: should_output_target and apply_hedge_gates from composite state."""

import time
from typing import Optional

from bifrost_worker.daemon.core.state.composite import CompositeState
from bifrost_worker.daemon.core.state.enums import (
    DeltaDeviationState,
    ExecutionState,
    LiquidityState,
    OptionPositionState,
    SystemHealthState,
)
from bifrost_worker.daemon.guards.execution_guard import ExecutionGuard
from bifrost_worker.daemon.strategy.gamma_scalper import HedgeIntent


def should_output_target(cs: CompositeState) -> bool:
    """
    True when composite state allows outputting TargetPosition / new hedge.
    (O1 or O2) and (D2 or D3) and (L0 or L1) and E0 and S0.
    SAFE_MODE (no new hedge): L2/L3 or S1/S2/S3 or E3/E4 -> False.
    """
    if cs.L in (LiquidityState.EXTREME_WIDE, LiquidityState.NO_QUOTE):
        return False
    if cs.S != SystemHealthState.OK:
        return False
    if cs.E != ExecutionState.IDLE:
        return False
    if cs.O not in (OptionPositionState.LONG_GAMMA, OptionPositionState.SHORT_GAMMA):
        return False
    if cs.D not in (DeltaDeviationState.HEDGE_NEEDED, DeltaDeviationState.FORCE_HEDGE):
        return False
    return True


def apply_hedge_gates(
    intent: HedgeIntent,
    cs: CompositeState,
    guard: ExecutionGuard,
    now_ts: Optional[float] = None,
    spot: Optional[float] = None,
    last_hedge_price: Optional[float] = None,
    spread_pct: Optional[float] = None,
    min_hedge_shares: int = 10,
) -> Optional[HedgeIntent]:
    """
    Apply three gates + cost; D3 bypasses cooldown.
    Returns HedgeIntent if allowed, None if blocked.
    """
    now_ts = now_ts or time.time()
    if intent.quantity < min_hedge_shares:
        return None
    force = intent.force_hedge or (cs.D == DeltaDeviationState.FORCE_HEDGE)
    allowed, reason = guard.allow_hedge(
        now_ts,
        cs.stock_pos,
        intent.side,
        intent.quantity,
        portfolio_delta=cs.net_delta,
        spot=spot,
        last_hedge_price=last_hedge_price or cs.last_hedge_price,
        spread_pct=spread_pct or cs.spread,
        force_hedge=force,
    )
    if not allowed:
        return None
    return intent
