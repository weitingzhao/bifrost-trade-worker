"""Structured logging for composite state, target position, order status, FSM transitions."""

import logging
import uuid
from typing import Any, Dict, Optional

from bifrost_worker.daemon.core.state.composite import CompositeState

logger = logging.getLogger(__name__)


def _ensure_trace_id(extra: dict) -> str:
    trace_id = extra.get("trace_id")
    if not trace_id:
        trace_id = str(uuid.uuid4())[:8]
        extra["trace_id"] = trace_id
    return trace_id


def log_composite_state(
    trace_id: Optional[str] = None,
    event_id: Optional[str] = None,
    cs: Optional[CompositeState] = None,
    extra: Optional[dict] = None,
) -> None:
    """Log CompositeState as structured key-value."""
    extra = extra or {}
    if trace_id:
        extra["trace_id"] = trace_id
    _ensure_trace_id(extra)
    if event_id:
        extra["event_id"] = event_id
    if cs is not None:
        extra["O"] = cs.O.value
        extra["D"] = cs.D.value
        extra["M"] = cs.M.value
        extra["L"] = cs.L.value
        extra["E"] = cs.E.value
        extra["S"] = cs.S.value
        extra["net_delta"] = cs.net_delta
        extra["option_delta"] = cs.option_delta
        extra["stock_pos"] = cs.stock_pos
        extra["greeks_valid"] = cs.greeks_valid
        extra["ts"] = cs.ts
    msg = "composite_state " + " ".join(f"{k}={v}" for k, v in sorted(extra.items()))
    logger.info(msg)


def log_target_position(
    trace_id: Optional[str] = None,
    event_id: Optional[str] = None,
    target_shares: Optional[int] = None,
    cs: Optional[CompositeState] = None,
    extra: Optional[dict] = None,
) -> None:
    """Log TargetPosition output with optional composite state."""
    extra = extra or {}
    _ensure_trace_id(extra)
    if event_id:
        extra["event_id"] = event_id
    if target_shares is not None:
        extra["target_shares"] = target_shares
    if cs is not None:
        extra["O"] = cs.O.value
        extra["D"] = cs.D.value
        extra["M"] = cs.M.value
        extra["L"] = cs.L.value
        extra["E"] = cs.E.value
        extra["S"] = cs.S.value
        extra["net_delta"] = cs.net_delta
    msg = "target_position " + " ".join(f"{k}={v}" for k, v in sorted(extra.items()))
    logger.info(msg)


def log_order_status(
    trace_id: Optional[str] = None,
    event_id: Optional[str] = None,
    order_status: Optional[str] = None,
    side: Optional[str] = None,
    quantity: Optional[int] = None,
    extra: Optional[dict] = None,
) -> None:
    """Log order state change."""
    extra = extra or {}
    _ensure_trace_id(extra)
    if event_id:
        extra["event_id"] = event_id
    if order_status:
        extra["order_status"] = order_status
    if side:
        extra["side"] = side
    if quantity is not None:
        extra["quantity"] = quantity
    msg = "order_status " + " ".join(f"{k}={v}" for k, v in sorted(extra.items()))
    logger.info(msg)


def log_fsm_transition(
    from_state: str,
    to_state: str,
    event: str,
    trace_id: Optional[str] = None,
    guards_evaluated: Optional[Dict[str, bool]] = None,
    extra: Optional[dict] = None,
) -> None:
    """Log FSM state transition: trace_id, from_state, to_state, event, guards_evaluated."""
    extra = extra or {}
    _ensure_trace_id(extra)
    extra["from_state"] = from_state
    extra["to_state"] = to_state
    extra["event"] = event
    if guards_evaluated is not None:
        extra["guards_evaluated"] = {k: v for k, v in guards_evaluated.items() if v}
    msg = "fsm_transition " + " ".join(f"{k}={v}" for k, v in sorted(extra.items()))
    logger.info(msg)
