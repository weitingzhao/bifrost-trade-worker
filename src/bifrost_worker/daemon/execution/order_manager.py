"""Order manager: tracks active order state for execution state (E)."""

from typing import TYPE_CHECKING, Optional

from bifrost_worker.daemon.core.state.enums import ExecutionState

if TYPE_CHECKING:
    from bifrost_worker.daemon.fsm.hedge_fsm import HedgeFSM


class OrderManager:
    """Tracks execution state: IDLE, ORDER_WORKING, PARTIAL_FILL, DISCONNECTED, BROKER_ERROR."""

    def __init__(self):
        self._execution_state = ExecutionState.IDLE
        self._connected = True
        self._broker_error: Optional[str] = None
        self._hedge_fsm: Optional["HedgeFSM"] = None

    @property
    def execution_state(self) -> ExecutionState:
        return self._execution_state

    def set_execution_state(self, state: ExecutionState) -> None:
        self._execution_state = state

    def set_hedge_fsm(self, fsm: Optional["HedgeFSM"]) -> None:
        self._hedge_fsm = fsm

    @property
    def connected(self) -> bool:
        return self._connected

    def set_connected(self, connected: bool) -> None:
        self._connected = connected
        if self._hedge_fsm is not None:
            self._hedge_fsm.set_connected(connected)

    @property
    def broker_error(self) -> Optional[str]:
        return self._broker_error

    def set_broker_error(self, msg: Optional[str]) -> None:
        self._broker_error = msg

    def effective_e_state(self) -> ExecutionState:
        """E state: when HedgeFSM is set, use its mapping; else legacy internal state."""
        if self._hedge_fsm is not None:
            return self._hedge_fsm.effective_execution_state()
        if not self._connected:
            return ExecutionState.DISCONNECTED
        if self._broker_error:
            return ExecutionState.BROKER_ERROR
        return self._execution_state
