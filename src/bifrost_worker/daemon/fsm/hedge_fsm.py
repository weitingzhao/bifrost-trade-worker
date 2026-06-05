"""Hedge FSM: EXEC_IDLE -> PLAN -> SEND -> WAIT_ACK -> WORKING -> FILLED/PARTIAL/REPRICE/CANCEL/FAIL."""

import logging
from typing import Callable, Optional

from bifrost_worker.daemon.core.state.enums import ExecutionState, HedgeState
from bifrost_worker.daemon.fsm.events import HedgeEvent, TargetPositionEvent

logger = logging.getLogger(__name__)

# Valid transitions: from_state -> set of (event, to_state) or (event, guard_key, to_state)
# Simplified: we use a transition table (from, event) -> to_state with optional guard
_TRANSITIONS: dict[tuple[HedgeState, HedgeEvent], HedgeState] = {
    (HedgeState.EXEC_IDLE, HedgeEvent.RECV_TARGET): HedgeState.PLAN,
    (HedgeState.PLAN, HedgeEvent.PLAN_SKIP): HedgeState.EXEC_IDLE,
    (HedgeState.PLAN, HedgeEvent.PLAN_SEND): HedgeState.SEND,
    (HedgeState.SEND, HedgeEvent.PLACE_ORDER): HedgeState.WAIT_ACK,
    (HedgeState.WAIT_ACK, HedgeEvent.ACK_OK): HedgeState.WORKING,
    (HedgeState.WAIT_ACK, HedgeEvent.ACK_REJECT): HedgeState.FAIL,
    (HedgeState.WAIT_ACK, HedgeEvent.TIMEOUT_ACK): HedgeState.FAIL,
    (HedgeState.WAIT_ACK, HedgeEvent.BROKER_DOWN): HedgeState.FAIL,
    (HedgeState.WORKING, HedgeEvent.PARTIAL_FILL): HedgeState.PARTIAL,
    (HedgeState.WORKING, HedgeEvent.FULL_FILL): HedgeState.FILLED,
    (HedgeState.WORKING, HedgeEvent.TIMEOUT_WORKING): HedgeState.REPRICE,
    (HedgeState.WORKING, HedgeEvent.RISK_TRIP): HedgeState.CANCEL,
    (HedgeState.WORKING, HedgeEvent.MANUAL_CANCEL): HedgeState.CANCEL,
    (HedgeState.WORKING, HedgeEvent.BROKER_DOWN): HedgeState.CANCEL,
    (HedgeState.PARTIAL, HedgeEvent.PLAN_SEND): HedgeState.SEND,
    (HedgeState.PARTIAL, HedgeEvent.PLAN_SKIP): HedgeState.EXEC_IDLE,
    (HedgeState.REPRICE, HedgeEvent.PLACE_ORDER): HedgeState.WAIT_ACK,
    (HedgeState.CANCEL, HedgeEvent.CANCEL_SENT): HedgeState.RECOVER,
    (HedgeState.RECOVER, HedgeEvent.POSITIONS_RESYNCED): HedgeState.EXEC_IDLE,
    (HedgeState.RECOVER, HedgeEvent.CANNOT_RECOVER): HedgeState.FAIL,
    (HedgeState.FAIL, HedgeEvent.TRY_RESYNC): HedgeState.RECOVER,
    (HedgeState.FILLED, HedgeEvent.RECV_TARGET): HedgeState.PLAN,
}


def _to_execution_state(h: HedgeState, connected: bool) -> ExecutionState:
    """Map HedgeState to legacy ExecutionState (E0..E4) for composite state."""
    if not connected:
        return ExecutionState.DISCONNECTED
    if h == HedgeState.FAIL:
        return ExecutionState.BROKER_ERROR
    if h in (HedgeState.EXEC_IDLE, HedgeState.FILLED):
        return ExecutionState.IDLE
    if h == HedgeState.PARTIAL:
        return ExecutionState.PARTIAL_FILL
    if h in (
        HedgeState.PLAN,
        HedgeState.SEND,
        HedgeState.WAIT_ACK,
        HedgeState.WORKING,
        HedgeState.REPRICE,
        HedgeState.CANCEL,
        HedgeState.RECOVER,
    ):
        return ExecutionState.ORDER_WORKING
    return ExecutionState.IDLE


class HedgeFSM:
    """
    Execution sub-FSM: receives TargetPosition, plans, sends order, waits ack/fill,
    handles partial/reprice/cancel/recover.
    """

    def __init__(
        self,
        min_hedge_shares: int = 10,
        on_transition: Optional[
            Callable[[HedgeState, HedgeState, HedgeEvent], None]
        ] = None,
    ):
        self._state = HedgeState.EXEC_IDLE
        self._min_hedge_shares = min_hedge_shares
        self._on_transition = on_transition
        self._current_target: Optional[TargetPositionEvent] = None
        self._need_shares: int = 0  # signed: positive = buy, negative = sell
        self._connected = True

    @property
    def state(self) -> HedgeState:
        return self._state

    @property
    def need_shares(self) -> int:
        return self._need_shares

    @property
    def current_target(self) -> Optional[TargetPositionEvent]:
        return self._current_target

    def set_connected(self, connected: bool) -> None:
        self._connected = connected

    def effective_execution_state(self) -> ExecutionState:
        """E state for composite/snapshot (E0..E4)."""
        return _to_execution_state(self._state, self._connected)

    def can_place_order(self) -> bool:
        """True when in EXEC_IDLE or FILLED (ready to accept new target)."""
        return self._state in (HedgeState.EXEC_IDLE, HedgeState.FILLED)

    def _lookup_and_apply(self, event: HedgeEvent) -> bool:
        """Look up (state, event) in _TRANSITIONS and apply if valid. Returns True if applied."""
        to_state = _TRANSITIONS.get((self._state, event))
        if to_state is not None:
            self._transition(to_state, event)
            return True
        return False

    def _transition(self, to_state: HedgeState, event: HedgeEvent) -> bool:
        from_state = self._state
        self._state = to_state
        logger.debug(
            "HedgeExecFSM %s -> %s on %s", from_state.value, to_state.value, event.value
        )
        if self._on_transition:
            try:
                self._on_transition(from_state, to_state, event)
            except Exception as e:
                logger.debug("on_transition callback error: %s", e)
        return True

    # CANCEL ->
    def on_cancel_sent(self) -> bool:
        """Cancel sent: CANCEL -> RECOVER."""
        if self._state != HedgeState.CANCEL:
            return False
        return self._lookup_and_apply(HedgeEvent.CANCEL_SENT)

    # EXEC_IDLE & FILLED ->
    def on_target(self, target: TargetPositionEvent, current_stock_pos: int) -> bool:
        """
        Receive TargetPosition: transition to PLAN. Stores need_shares = target_shares - current_stock_pos.
        """
        if not self.can_place_order():
            logger.warning(
                "HedgeExecFSM received target in state %s", self._state.value
            )
            return False
        self._current_target = target
        self._need_shares = target.target_shares - current_stock_pos
        return self._lookup_and_apply(HedgeEvent.RECV_TARGET)

    # FAIL ->
    def on_try_resync(self) -> bool:
        """Try resync: FAIL -> RECOVER."""
        if self._state != HedgeState.FAIL:
            return False
        return self._lookup_and_apply(HedgeEvent.TRY_RESYNC)

    # PLAN & PARTIAL ->
    def on_plan_decide(self, send_order: bool) -> bool:
        """
        After PLAN: if send_order (abs(need_shares) >= min_size) -> SEND; else -> EXEC_IDLE.
        """
        if self._state != HedgeState.PLAN:
            return False
        if send_order:
            return self._lookup_and_apply(HedgeEvent.PLAN_SEND)
        self._current_target = None
        return self._lookup_and_apply(HedgeEvent.PLAN_SKIP)

    def on_partial_replan(self, send_order: bool) -> bool:
        """After PARTIAL: replan -> SEND or EXEC_IDLE."""
        if self._state != HedgeState.PARTIAL:
            return False
        if send_order:
            return self._lookup_and_apply(HedgeEvent.PLAN_SEND)
        self._current_target = None
        self._need_shares = 0
        return self._lookup_and_apply(HedgeEvent.PLAN_SKIP)

    # RECOVER ->
    def on_cannot_recover(self) -> bool:
        """Cannot recover: RECOVER -> FAIL."""
        if self._state != HedgeState.RECOVER:
            return False
        return self._lookup_and_apply(HedgeEvent.CANNOT_RECOVER)

    def on_positions_resynced(self) -> bool:
        """Positions resynced: RECOVER -> EXEC_IDLE."""
        if self._state != HedgeState.RECOVER:
            return False
        self._current_target = None
        self._need_shares = 0
        return self._lookup_and_apply(HedgeEvent.POSITIONS_RESYNCED)

    # REPRICE & SEND ->
    def on_order_placed(self) -> bool:
        """After place_order called: SEND or REPRICE -> WAIT_ACK."""
        if self._state not in (HedgeState.SEND, HedgeState.REPRICE):
            return False
        return self._lookup_and_apply(HedgeEvent.PLACE_ORDER)

    # WAIT_ACK ->
    def on_ack_ok(self) -> bool:
        """Broker ack ok: WAIT_ACK -> WORKING."""
        if self._state != HedgeState.WAIT_ACK:
            return False
        return self._lookup_and_apply(HedgeEvent.ACK_OK)

    def on_ack_reject(self) -> bool:
        """Broker ack reject: WAIT_ACK -> FAIL."""
        if self._state != HedgeState.WAIT_ACK:
            return False
        return self._lookup_and_apply(HedgeEvent.ACK_REJECT)

    def on_timeout_ack(self) -> bool:
        """Ack timeout: WAIT_ACK -> FAIL."""
        if self._state != HedgeState.WAIT_ACK:
            return False
        return self._lookup_and_apply(HedgeEvent.TIMEOUT_ACK)

    # WAIT_ACK & WORKING ->
    def on_broker_down(self) -> bool:
        """Broker down: WAIT_ACK -> FAIL, WORKING -> CANCEL."""
        if self._state in (HedgeState.WAIT_ACK, HedgeState.WORKING):
            return self._lookup_and_apply(HedgeEvent.BROKER_DOWN)
        self._connected = False
        return True

    # WORKING ->
    def on_timeout_working(self) -> bool:
        """Working timeout (reprice): WORKING -> REPRICE."""
        if self._state != HedgeState.WORKING:
            return False
        return self._lookup_and_apply(HedgeEvent.TIMEOUT_WORKING)

    def on_partial_fill(self) -> bool:
        """Partial fill: WORKING -> PARTIAL."""
        if self._state != HedgeState.WORKING:
            return False
        return self._lookup_and_apply(HedgeEvent.PARTIAL_FILL)

    def on_full_fill(self) -> bool:
        """Full fill: WORKING -> FILLED; clear target."""
        if self._state != HedgeState.WORKING:
            return False
        self._current_target = None
        self._need_shares = 0
        return self._lookup_and_apply(HedgeEvent.FULL_FILL)

    def on_manual_cancel(self) -> bool:
        """Manual cancel: WORKING -> CANCEL."""
        if self._state != HedgeState.WORKING:
            return False
        return self._lookup_and_apply(HedgeEvent.MANUAL_CANCEL)

    def on_risk_trip(self) -> bool:
        """Risk trip: WORKING -> CANCEL."""
        if self._state != HedgeState.WORKING:
            return False
        return self._lookup_and_apply(HedgeEvent.RISK_TRIP)
