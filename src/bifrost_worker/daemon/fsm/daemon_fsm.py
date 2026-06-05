"""Daemon lifecycle FSM: IDLE -> CONNECTING -> CONNECTED -> RUNNING <-> RUNNING_SUSPENDED -> STOPPING -> STOPPED.

Engine uses Redis (IB Ingestor / IB Account Agent) and IB Operator RPC only; no in-process IB connection or WAITING_IB.

Transition implementation (daemon_handlers.py):
- IDLE -> CONNECTING: _handle_idle (always)
- IDLE -> STOPPED: request_stop() when IDLE
- CONNECTING -> CONNECTED: _handle_connecting (immediate; no IB socket)
- CONNECTING -> STOPPING: request_stop() during connect
- CONNECTED -> RUNNING: _handle_connected
- CONNECTED -> STOPPING: request_stop() or exception in _handle_connected
- RUNNING -> RUNNING_SUSPENDED: when daemon_run_status.suspended is true (poll in heartbeat)
- RUNNING -> STOPPING: _handle_running (loop exit) or request_stop()
- RUNNING_SUSPENDED -> RUNNING: when daemon_run_status.suspended is false (poll in heartbeat)
- RUNNING_SUSPENDED -> STOPPING: request_stop()
- STOPPING -> STOPPED: _handle_stopping
"""

import enum
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class DaemonState(str, enum.Enum):
    """Daemon lifecycle states. RUNNING_SUSPENDED = running but hedging paused (daemon_run_status.suspended)."""

    IDLE = "idle"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RUNNING = "running"
    RUNNING_SUSPENDED = "running_suspended"  # Same as RUNNING but hedge suspended by monitoring
    STOPPING = "stopping"
    STOPPED = "stopped"


# Valid transitions: from_state -> set of allowed to_states
_TRANSITIONS: dict[DaemonState, set[DaemonState]] = {
    DaemonState.IDLE: {DaemonState.CONNECTING, DaemonState.STOPPED},
    DaemonState.CONNECTING: {DaemonState.CONNECTED, DaemonState.STOPPING},
    DaemonState.CONNECTED: {
        DaemonState.RUNNING,
        DaemonState.STOPPING,
    },
    DaemonState.RUNNING: {DaemonState.STOPPING, DaemonState.RUNNING_SUSPENDED},
    DaemonState.RUNNING_SUSPENDED: {DaemonState.RUNNING, DaemonState.STOPPING},
    DaemonState.STOPPING: {DaemonState.STOPPED},
    DaemonState.STOPPED: set(),
}


class DaemonFSM:
    """Manages daemon lifecycle state and transitions."""

    def __init__(
        self,
        on_transition: Optional[Callable[[DaemonState, DaemonState], None]] = None,
    ):
        self._current = DaemonState.IDLE
        self._on_transition = on_transition

    @property
    def current(self) -> DaemonState:
        return self._current

    def can_transition_to(self, to_state: DaemonState) -> bool:
        """Check if transition from current state to to_state is valid."""
        allowed = _TRANSITIONS.get(self._current, set())
        return to_state in allowed

    def transition(self, to_state: DaemonState) -> bool:
        """
        Transition to new state if valid. Returns True on success, False otherwise.
        Calls on_transition(from, to) callback if provided.
        """
        if not self.can_transition_to(to_state):
            logger.warning(
                "Invalid transition: %s -> %s (allowed: %s)",
                self._current.value,
                to_state.value,
                [s.value for s in _TRANSITIONS.get(self._current, set())],
            )
            return False
        from_state = self._current
        self._current = to_state
        logger.debug("State: %s -> %s", from_state.value, to_state.value)
        if self._on_transition:
            try:
                self._on_transition(from_state, to_state)
            except Exception as e:
                logger.debug("on_transition callback error: %s", e)
        return True

    def is_running(self) -> bool:
        """True when daemon is in RUNNING or RUNNING_SUSPENDED (heartbeat, main loop active)."""
        return self._current in (DaemonState.RUNNING, DaemonState.RUNNING_SUSPENDED)

    def is_active(self) -> bool:
        """True when daemon can process hedges (CONNECTED or RUNNING)."""
        return self._current in (DaemonState.CONNECTED, DaemonState.RUNNING)

    def request_stop(self) -> bool:
        """Request stop: transition to STOPPING (for cleanup) or STOPPED (IDLE only)."""
        if self._current in (
            DaemonState.RUNNING,
            DaemonState.RUNNING_SUSPENDED,
            DaemonState.CONNECTING,
            DaemonState.CONNECTED,
        ):
            return self.transition(DaemonState.STOPPING)
        if self._current == DaemonState.IDLE:
            return self.transition(DaemonState.STOPPED)
        return False
