"""Strategy tests: event sequence -> CompositeState -> should_output_target / apply_hedge_gates."""

import time

import pytest

from bifrost_worker.daemon.core.state.composite import CompositeState
from bifrost_worker.daemon.core.state.enums import (
    DeltaDeviationState,
    ExecutionState,
    LiquidityState,
    MarketRegimeState,
    OptionPositionState,
    SystemHealthState,
)
from bifrost_worker.daemon.strategy.gamma_scalper import HedgeIntent, compute_target_position
from bifrost_worker.daemon.strategy.hedge_gate import apply_hedge_gates, should_output_target
from bifrost_worker.daemon.guards.execution_guard import ExecutionGuard


def _cs(
    O=OptionPositionState.LONG_GAMMA,
    D=DeltaDeviationState.HEDGE_NEEDED,
    M=MarketRegimeState.NORMAL,
    L=LiquidityState.NORMAL,
    E=ExecutionState.IDLE,
    S=SystemHealthState.OK,
    net_delta=50.0,
    stock_pos=0,
    **kwargs,
) -> CompositeState:
    defaults = {
        "option_delta": net_delta - stock_pos,
        "last_hedge_price": None,
        "last_hedge_ts": None,
        "spread": 0.05,
        "data_lag_ms": None,
        "greeks_valid": True,
        "ts": time.time(),
    }
    defaults.update(kwargs)
    return CompositeState(O=O, D=D, M=M, L=L, E=E, S=S, net_delta=net_delta, stock_pos=stock_pos, **defaults)


class TestShouldOutputTarget:
    """A1: O1, D2, L0, E0, S0 -> True; A2: D1 -> False; A3: L2 -> False."""

    def test_a1_output_target(self):
        cs = _cs(O=OptionPositionState.LONG_GAMMA, D=DeltaDeviationState.HEDGE_NEEDED, L=LiquidityState.NORMAL, E=ExecutionState.IDLE, S=SystemHealthState.OK)
        assert should_output_target(cs) is True

    def test_a1_o2_d2(self):
        cs = _cs(O=OptionPositionState.SHORT_GAMMA, D=DeltaDeviationState.HEDGE_NEEDED)
        assert should_output_target(cs) is True

    def test_a2_d1_no_output(self):
        cs = _cs(D=DeltaDeviationState.MINOR)
        assert should_output_target(cs) is False

    def test_a2_d0_no_output(self):
        cs = _cs(D=DeltaDeviationState.IN_BAND)
        assert should_output_target(cs) is False

    def test_a3_safe_mode_l2(self):
        cs = _cs(L=LiquidityState.EXTREME_WIDE)
        assert should_output_target(cs) is False

    def test_a3_safe_mode_l3(self):
        cs = _cs(L=LiquidityState.NO_QUOTE)
        assert should_output_target(cs) is False

    def test_a3_safe_mode_s1(self):
        cs = _cs(S=SystemHealthState.GREEKS_BAD)
        assert should_output_target(cs) is False

    def test_a3_safe_mode_e3(self):
        cs = _cs(E=ExecutionState.DISCONNECTED)
        assert should_output_target(cs) is False

    def test_o0_no_output(self):
        cs = _cs(O=OptionPositionState.NONE)
        assert should_output_target(cs) is False


class TestApplyHedgeGates:
    """D3 bypasses cooldown; min_hedge_shares enforced."""

    def test_a4_d3_bypass_cooldown(self):
        guard = ExecutionGuard(cooldown_sec=60, trading_hours_only=False)
        guard.record_hedge_sent()
        cs = _cs(D=DeltaDeviationState.FORCE_HEDGE, net_delta=100.0, stock_pos=0)
        intent = HedgeIntent(target_shares=-100, side="SELL", quantity=100, force_hedge=False)
        approved = apply_hedge_gates(intent, cs, guard, now_ts=time.time(), spot=100.0, min_hedge_shares=10)
        assert approved is not None
        assert approved.quantity == 100

    def test_blocked_min_hedge_shares(self):
        guard = ExecutionGuard(cooldown_sec=1, trading_hours_only=False)
        guard.set_last_hedge_time(time.time() - 10)
        cs = _cs(net_delta=15.0, stock_pos=0)
        intent = HedgeIntent(target_shares=-15, side="SELL", quantity=5, force_hedge=False)
        approved = apply_hedge_gates(intent, cs, guard, min_hedge_shares=10)
        assert approved is None

    def test_allowed_after_cooldown(self):
        guard = ExecutionGuard(cooldown_sec=1, trading_hours_only=False)
        guard.set_last_hedge_time(time.time() - 10)
        cs = _cs()
        intent = HedgeIntent(target_shares=-50, side="SELL", quantity=50, force_hedge=False)
        approved = apply_hedge_gates(intent, cs, guard, now_ts=time.time(), spot=100.0, min_hedge_shares=10)
        assert approved is not None


class TestComputeTargetPosition:
    def test_target_position(self):
        assert compute_target_position(50.0, 0) == -50  # opt_delta=50 -> target=-50
        assert compute_target_position(-30.0, 10) == 40  # opt_delta=-40 -> target=40
