"""Hedge FSM tests: full fill, partial fill, timeout->reprice, broker_down->cancel->recover."""

import pytest

from bifrost_worker.daemon.core.state.enums import ExecutionState, HedgeState
from bifrost_worker.daemon.fsm.events import TargetPositionEvent
from bifrost_worker.daemon.fsm.hedge_fsm import HedgeFSM


def _target(target_shares: int, side: str = "BUY", quantity: int = 0) -> TargetPositionEvent:
    q = quantity or abs(target_shares)
    return TargetPositionEvent(target_shares=target_shares, side=side, quantity=q, ts=1000.0)


class TestFullFill:
    def test_exec_idle_to_plan_on_target(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        assert fsm.state == HedgeState.EXEC_IDLE
        assert fsm.can_place_order() is True
        ok = fsm.on_target(_target(100, "BUY", 100), current_stock_pos=0)
        assert ok is True
        assert fsm.state == HedgeState.PLAN
        assert fsm.need_shares == 100

    def test_plan_to_send_then_working_then_filled(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        assert fsm.state == HedgeState.PLAN
        fsm.on_plan_decide(send_order=True)
        assert fsm.state == HedgeState.SEND
        fsm.on_order_placed()
        assert fsm.state == HedgeState.WAIT_ACK
        fsm.on_ack_ok()
        assert fsm.state == HedgeState.WORKING
        assert fsm.effective_execution_state() == ExecutionState.ORDER_WORKING
        fsm.on_full_fill()
        assert fsm.state == HedgeState.FILLED
        assert fsm.effective_execution_state() == ExecutionState.IDLE
        assert fsm.can_place_order() is True
        assert fsm.current_target is None
        assert fsm.need_shares == 0


class TestPartialFill:
    def test_working_to_partial_then_replan_send(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(100, "BUY", 100), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_ok()
        assert fsm.state == HedgeState.WORKING
        fsm.on_partial_fill()
        assert fsm.state == HedgeState.PARTIAL
        assert fsm.effective_execution_state() == ExecutionState.PARTIAL_FILL
        fsm.on_partial_replan(send_order=True)
        assert fsm.state == HedgeState.SEND
        fsm.on_order_placed()
        fsm.on_ack_ok()
        fsm.on_full_fill()
        assert fsm.state == HedgeState.FILLED

    def test_partial_replan_skip_to_idle(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(100, "BUY", 100), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_ok()
        fsm.on_partial_fill()
        fsm.on_partial_replan(send_order=False)
        assert fsm.state == HedgeState.EXEC_IDLE
        assert fsm.can_place_order() is True


class TestPlanSkip:
    def test_plan_to_idle_when_need_below_min_size(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(5, "BUY", 5), current_stock_pos=0)
        assert fsm.state == HedgeState.PLAN
        fsm.on_plan_decide(send_order=False)
        assert fsm.state == HedgeState.EXEC_IDLE
        assert fsm.current_target is None


class TestTimeoutReprice:
    def test_working_to_reprice_then_send(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "SELL", 50), current_stock_pos=50)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_ok()
        assert fsm.state == HedgeState.WORKING
        fsm.on_timeout_working()
        assert fsm.state == HedgeState.REPRICE
        fsm.on_order_placed()
        assert fsm.state == HedgeState.WAIT_ACK
        fsm.on_ack_ok()
        fsm.on_full_fill()
        assert fsm.state == HedgeState.FILLED


class TestBrokerDownCancelRecover:
    def test_working_to_cancel_on_broker_down(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_ok()
        assert fsm.state == HedgeState.WORKING
        fsm.on_broker_down()
        assert fsm.state == HedgeState.CANCEL

    def test_cancel_to_recover_on_cancel_sent(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_ok()
        fsm.on_broker_down()
        assert fsm.state == HedgeState.CANCEL
        fsm.on_cancel_sent()
        assert fsm.state == HedgeState.RECOVER

    def test_recover_to_idle_on_positions_resynced(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_ok()
        fsm.on_broker_down()
        fsm.on_cancel_sent()
        assert fsm.state == HedgeState.RECOVER
        fsm.on_positions_resynced()
        assert fsm.state == HedgeState.EXEC_IDLE
        assert fsm.can_place_order() is True

    def test_recover_to_fail_on_cannot_recover(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_ok()
        fsm.on_broker_down()
        fsm.on_cancel_sent()
        fsm.on_cannot_recover()
        assert fsm.state == HedgeState.FAIL
        assert fsm.effective_execution_state() == ExecutionState.BROKER_ERROR

    def test_fail_to_recover_on_try_resync(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_reject()
        assert fsm.state == HedgeState.FAIL
        fsm.on_try_resync()
        assert fsm.state == HedgeState.RECOVER
        fsm.on_positions_resynced()
        assert fsm.state == HedgeState.EXEC_IDLE


class TestWaitAckFail:
    def test_wait_ack_to_fail_on_reject(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_reject()
        assert fsm.state == HedgeState.FAIL

    def test_wait_ack_to_fail_on_timeout(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_timeout_ack()
        assert fsm.state == HedgeState.FAIL


class TestManualCancel:
    def test_working_to_cancel_on_manual_cancel(self):
        fsm = HedgeFSM(min_hedge_shares=10)
        fsm.on_target(_target(50, "BUY", 50), current_stock_pos=0)
        fsm.on_plan_decide(send_order=True)
        fsm.on_order_placed()
        fsm.on_ack_ok()
        fsm.on_manual_cancel()
        assert fsm.state == HedgeState.CANCEL
