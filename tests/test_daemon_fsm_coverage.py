"""
Verify that every DaemonFSM state transition is implemented by GsTrading.

DaemonFSM defines valid transitions; GsTrading handlers + run() must cover them.
This test ensures no transition is "orphaned" (defined but never used).
"""

from bifrost_worker.daemon.fsm.daemon_fsm import DaemonFSM, DaemonState

import bifrost_worker.daemon.fsm.daemon_fsm as daemon_fsm_module

_TRANSITIONS = daemon_fsm_module._TRANSITIONS

_MINIMAL_CONFIG = {
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
    "structure": {},
    "risk": {"paper_trade": True},
    "greeks": {},
    "earnings": {},
}


def test_all_non_terminal_states_have_handlers_in_gs_trading():
    """Every state except STOPPED must have a handler in GsTrading._get_state_handlers()."""
    from bifrost_worker.daemon.app.gs_trading import GsTrading

    app = GsTrading(_MINIMAL_CONFIG)
    handlers = app._get_state_handlers()

    # STOPPED is terminal - no handler needed.
    # RUNNING_SUSPENDED is never dispatched: we stay inside _handle_running() and the FSM
    # flips to RUNNING_SUSPENDED in heartbeat (_apply_run_status_transition).
    states_needing_handlers = [
        s for s in DaemonState
        if s not in (DaemonState.STOPPED, DaemonState.RUNNING_SUSPENDED)
    ]
    for state in states_needing_handlers:
        assert state in handlers, f"Missing handler for state {state.value}"


def test_every_valid_transition_has_implementation_path():
    """
    For each (from_state, to_state) in _TRANSITIONS, document how it is reached.
    """
    implementation_paths = {
        (DaemonState.IDLE, DaemonState.CONNECTING): "_handle_idle always returns CONNECTING",
        (DaemonState.IDLE, DaemonState.STOPPED): "request_stop() when IDLE",
        (DaemonState.CONNECTING, DaemonState.CONNECTED): "_handle_connecting returns CONNECTED",
        (DaemonState.CONNECTING, DaemonState.STOPPING): "request_stop() during connect",
        (DaemonState.CONNECTED, DaemonState.RUNNING): "_handle_connected returns RUNNING",
        (DaemonState.CONNECTED, DaemonState.STOPPING): "request_stop() when CONNECTED",
        (DaemonState.RUNNING, DaemonState.STOPPING): "_handle_running returns STOPPING when loop exits, or request_stop()",
        (DaemonState.RUNNING, DaemonState.RUNNING_SUSPENDED): "heartbeat _apply_run_status_transition when daemon_run_status.suspended=true",
        (DaemonState.RUNNING_SUSPENDED, DaemonState.RUNNING): "heartbeat _apply_run_status_transition when suspended=false",
        (DaemonState.RUNNING_SUSPENDED, DaemonState.STOPPING): "request_stop() during RUNNING_SUSPENDED",
        (DaemonState.STOPPING, DaemonState.STOPPED): "_handle_stopping returns STOPPED",
    }

    for from_state, allowed in _TRANSITIONS.items():
        for to_state in allowed:
            key = (from_state, to_state)
            assert key in implementation_paths, (
                f"Transition {from_state.value} -> {to_state.value} has no documented implementation. "
                "Add handler return or request_stop() path in gs_trading.py"
            )


def test_transition_table_completeness():
    """All states appear in transition table; STOPPED has no outgoing transitions."""
    all_states = set(DaemonState)
    states_in_table = set(_TRANSITIONS.keys())
    assert states_in_table == all_states
    assert _TRANSITIONS[DaemonState.STOPPED] == set()


def test_request_stop_covers_expected_transitions():
    """request_stop() implements IDLE->STOPPED and *->STOPPING for active states."""
    fsm = DaemonFSM()
    assert fsm.current == DaemonState.IDLE
    assert fsm.request_stop()  # IDLE -> STOPPED
    assert fsm.current == DaemonState.STOPPED

    fsm._current = DaemonState.CONNECTING
    assert fsm.request_stop()  # CONNECTING -> STOPPING
    assert fsm.current == DaemonState.STOPPING

    fsm._current = DaemonState.CONNECTED
    assert fsm.request_stop()
    assert fsm.current == DaemonState.STOPPING

    fsm._current = DaemonState.RUNNING
    assert fsm.request_stop()
    assert fsm.current == DaemonState.STOPPING

    fsm._current = DaemonState.RUNNING_SUSPENDED
    assert fsm.request_stop()
    assert fsm.current == DaemonState.STOPPING
