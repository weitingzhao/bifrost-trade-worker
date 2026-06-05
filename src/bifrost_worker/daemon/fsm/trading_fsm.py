"""Trading FSM (macro): BOOT -> SYNC -> IDLE -> ARMED -> MONITOR -> NEED_HEDGE/HEDGING/SAFE/etc."""

import logging
from typing import Any, Callable, Dict, Optional

from bifrost_worker.daemon.core.state.enums import TradingState
from bifrost_worker.daemon.guards.trading_guard import TradingGuard
from bifrost_worker.daemon.core.state.snapshot import StateSnapshot
from bifrost_worker.daemon.fsm.events import TradingEvent

logger = logging.getLogger(__name__)


def _eval_guards(
    snapshot: StateSnapshot,
    config: Optional[Dict[str, Any]],
    guard: Any,
) -> Dict[str, bool]:
    """Evaluate all guards used by TradingFSM; return dict of guard_name -> bool."""
    return TradingGuard(snapshot, config, guard).eval_all()


def _handle_sync(
    fire: Callable,
    s: TradingState,
    event: TradingEvent,
    g: Dict[str, bool],
) -> Optional[TradingState]:
    """SYNC -> IDLE or SAFE."""
    if g["positions_ok"] and g["data_ok"]:
        fire(s, TradingState.IDLE, event, g)
        return TradingState.IDLE
    if not g["data_ok"] or g["broker_down"]:
        fire(s, TradingState.SAFE, event, g)
        return TradingState.SAFE
    return None


def _handle_idle(
    fire: Callable,
    s: TradingState,
    event: TradingEvent,
    g: Dict[str, bool],
) -> Optional[TradingState]:
    """IDLE -> SAFE or ARMED."""
    if g["data_stale"] or g["greeks_bad"] or g["broker_down"]:
        fire(s, TradingState.SAFE, event, g)
        return TradingState.SAFE
    if g["have_option_position"] and g["strategy_enabled"]:
        fire(s, TradingState.ARMED, event, g)
        return TradingState.ARMED
    return None


def _handle_armed(
    fire: Callable,
    s: TradingState,
    event: TradingEvent,
    g: Dict[str, bool],
) -> Optional[TradingState]:
    """ARMED -> MONITOR."""
    if g["delta_band_ready"]:
        fire(s, TradingState.MONITOR, event, g)
        return TradingState.MONITOR
    return None


def _handle_monitor(
    fire: Callable,
    s: TradingState,
    event: TradingEvent,
    g: Dict[str, bool],
) -> Optional[TradingState]:
    """MONITOR -> NO_TRADE, NEED_HEDGE, PAUSE_COST, PAUSE_LIQ."""
    if g["in_no_trade_band"]:
        fire(s, TradingState.NO_TRADE, event, g)
        return TradingState.NO_TRADE
    if g["cost_ok"] and g["liquidity_ok"]:
        fire(s, TradingState.NEED_HEDGE, event, g)
        return TradingState.NEED_HEDGE
    if not g["cost_ok"]:
        fire(s, TradingState.PAUSE_COST, event, g)
        return TradingState.PAUSE_COST
    if not g["liquidity_ok"]:
        fire(s, TradingState.PAUSE_LIQ, event, g)
        return TradingState.PAUSE_LIQ
    return None


def _handle_no_trade(
    fire: Callable,
    s: TradingState,
    event: TradingEvent,
    g: Dict[str, bool],
) -> Optional[TradingState]:
    """NO_TRADE -> NEED_HEDGE, PAUSE_COST, PAUSE_LIQ (only when !in_no_trade_band)."""
    if g["in_no_trade_band"]:
        return None  # stay in NO_TRADE
    if g["cost_ok"] and g["liquidity_ok"]:
        fire(s, TradingState.NEED_HEDGE, event, g)
        return TradingState.NEED_HEDGE
    if not g["cost_ok"]:
        fire(s, TradingState.PAUSE_COST, event, g)
        return TradingState.PAUSE_COST
    if not g["liquidity_ok"]:
        fire(s, TradingState.PAUSE_LIQ, event, g)
        return TradingState.PAUSE_LIQ
    return None


def _handle_pause(
    fire: Callable,
    s: TradingState,
    event: TradingEvent,
    g: Dict[str, bool],
) -> Optional[TradingState]:
    """PAUSE_COST/PAUSE_LIQ -> NO_TRADE or NEED_HEDGE."""
    if g["in_no_trade_band"]:
        fire(s, TradingState.NO_TRADE, event, g)
        return TradingState.NO_TRADE
    if g["cost_ok"] and g["liquidity_ok"]:
        fire(s, TradingState.NEED_HEDGE, event, g)
        return TradingState.NEED_HEDGE
    return None


class TradingFSM:
    """
    Top-level Trading FSM. Transition table driven by events and guards.
    Any -> SAFE on broker_down || data_stale || greeks_bad || exec_fault.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        guard: Any = None,
        on_transition: Optional[
            Callable[[TradingState, TradingState, TradingEvent, Dict[str, bool]], None]
        ] = None,
    ):
        self._state = TradingState.BOOT
        self._config = config or {}
        self._guard = guard
        self._on_transition = on_transition

    @property
    def state(self) -> TradingState:
        return self._state

    def eval_guards(self, snapshot: StateSnapshot) -> Dict[str, bool]:
        """Return current guard evaluations for logging."""
        return _eval_guards(snapshot, self._config, self._guard)

    def transition(
        self,
        event: TradingEvent,
        snapshot: StateSnapshot,
    ) -> Optional[TradingState]:
        """
        Compute next state from current state, event, and guards.
        Returns new state if transition is valid, else None.
        Does not mutate state; caller should set state = return value.
        """
        s = self._state
        g = _eval_guards(snapshot, self._config, self._guard)
        fire = self._fire_transition

        # Any -> SAFE on broker_down || data_stale || greeks_bad || exec_fault
        if g["broker_down"] or g["data_stale"] or g["greeks_bad"] or g["exec_fault"]:
            if s != TradingState.SAFE:
                fire(s, TradingState.SAFE, event, g)
                return TradingState.SAFE
            return None

        if event == TradingEvent.SHUTDOWN:
            return None  # caller handles shutdown

        if event == TradingEvent.START and s == TradingState.BOOT:
            fire(s, TradingState.SYNC, event, g)
            return TradingState.SYNC

        if event in (
            TradingEvent.SYNCED,
            TradingEvent.QUOTE,
            TradingEvent.TICK,
            TradingEvent.GREEKS_UPDATE,
        ):
            if s == TradingState.SYNC:
                return _handle_sync(fire, s, event, g)
            if s == TradingState.IDLE:
                return _handle_idle(fire, s, event, g)
            if s == TradingState.ARMED:
                return _handle_armed(fire, s, event, g)
            if s == TradingState.MONITOR:
                return _handle_monitor(fire, s, event, g)
            if s == TradingState.NO_TRADE:
                return _handle_no_trade(fire, s, event, g)
            if s in (TradingState.PAUSE_COST, TradingState.PAUSE_LIQ):
                return _handle_pause(fire, s, event, g)

        if event == TradingEvent.TARGET_EMITTED and s == TradingState.NEED_HEDGE:
            fire(s, TradingState.HEDGING, event, g)
            return TradingState.HEDGING

        if event == TradingEvent.HEDGE_DONE and s == TradingState.HEDGING:
            fire(s, TradingState.MONITOR, event, g)
            return TradingState.MONITOR

        if event == TradingEvent.HEDGE_FAILED and s == TradingState.HEDGING:
            if g["retry_allowed"]:
                fire(s, TradingState.NEED_HEDGE, event, g)
                return TradingState.NEED_HEDGE
            fire(s, TradingState.SAFE, event, g)
            return TradingState.SAFE

        if event == TradingEvent.MANUAL_RESUME and s == TradingState.SAFE:
            if g["broker_up"] and g["data_ok"]:
                fire(s, TradingState.SYNC, event, g)
                return TradingState.SYNC

        if event == TradingEvent.BROKER_UP and s == TradingState.SAFE:
            if g["data_ok"]:
                fire(s, TradingState.SYNC, event, g)
                return TradingState.SYNC

        return None

    def apply_transition(
        self,
        event: TradingEvent,
        snapshot: StateSnapshot,
    ) -> bool:
        """
        Apply transition: compute next state and if valid, set _state and return True.
        """
        next_state = self.transition(event, snapshot)
        if next_state is not None:
            self._state = next_state
            return True
        return False

    def _fire_transition(
        self,
        from_state: TradingState,
        to_state: TradingState,
        event: TradingEvent,
        guards: Dict[str, bool],
    ) -> None:
        logger.debug(
            "TradingFSM %s -> %s on %s guards=%s",
            from_state.value,
            to_state.value,
            event.value,
            {k: v for k, v in guards.items() if v},
        )
        if self._on_transition:
            try:
                self._on_transition(from_state, to_state, event, guards)
            except Exception as e:
                logger.debug("on_transition error: %s", e)
