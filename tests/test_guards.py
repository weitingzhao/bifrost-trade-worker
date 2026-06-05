"""Unit tests for pure guard functions: data_stale, greeks_bad, in_no_trade_band, cost_ok, liquidity_ok."""

import math
from types import SimpleNamespace

import pytest

from bifrost_worker.daemon.core.state.enums import (
    DeltaDeviationState,
    ExecutionState,
    LiquidityState,
    OptionPositionState,
    SystemHealthState,
)
from bifrost_worker.daemon.guards.trading_guard import TradingGuard
from bifrost_worker.daemon.core.state.snapshot import StateSnapshot, GreeksSnapshot, default_snapshot


def _make_snap(
    event_lag_ms=None,
    spread_pct=0.05,
    spot=100.0,
    L=LiquidityState.NORMAL,
    E=ExecutionState.IDLE,
    greeks_valid=True,
    net_delta=0.0,
    last_hedge_price=None,
    O=OptionPositionState.LONG_GAMMA,
) -> StateSnapshot:
    from bifrost_worker.daemon.core.state.enums import MarketRegimeState

    g = (
        GreeksSnapshot(delta=net_delta, gamma=0.02, valid=greeks_valid)
        if greeks_valid
        else None
    )
    return StateSnapshot(
        O=O,
        D=(
            DeltaDeviationState.IN_BAND
            if abs(net_delta) <= 10
            else DeltaDeviationState.HEDGE_NEEDED
        ),
        M=MarketRegimeState.NORMAL,
        L=L,
        E=E,
        S=SystemHealthState.OK,
        net_delta=net_delta,
        option_delta=net_delta,
        stock_pos=0,
        spot=spot,
        spread_pct=spread_pct,
        event_lag_ms=event_lag_ms,
        greeks=g,
        last_hedge_price=last_hedge_price,
        last_hedge_ts=None,
        ts=1000.0,
    )


class TestDataStale:
    def test_data_ok_when_lag_under_threshold(self):
        snap = _make_snap(event_lag_ms=500, spot=100.0)
        cfg = {"system": {"data_lag_threshold_ms": 1000}}
        tg = TradingGuard(snap, cfg)
        assert tg.is_data_ok() is True
        assert tg.is_data_stale() is False

    def test_data_stale_when_lag_over_threshold(self):
        snap = _make_snap(event_lag_ms=2000, spot=100.0)
        cfg = {"system": {"data_lag_threshold_ms": 1000}}
        tg = TradingGuard(snap, cfg)
        assert tg.is_data_ok() is False
        assert tg.is_data_stale() is True

    def test_data_stale_when_no_quote(self):
        snap = _make_snap(L=LiquidityState.NO_QUOTE, spot=None)
        tg = TradingGuard(snap)
        assert tg.is_data_ok() is False
        assert tg.is_data_stale() is True

    def test_data_ok_when_no_config_uses_default_threshold(self):
        snap = _make_snap(event_lag_ms=500)
        assert TradingGuard(snap).is_data_ok() is True
        snap2 = _make_snap(event_lag_ms=2000)
        assert TradingGuard(snap2).is_data_ok() is False

    def test_backward_compat_state_space_still_works(self):
        """Legacy state_space.delta, state_space.system etc. still supported."""
        snap = _make_snap(event_lag_ms=500)
        cfg = {"state_space": {"system": {"data_lag_threshold_ms": 1000}}}
        assert TradingGuard(snap, cfg).is_data_ok() is True

    def test_gates_structure_resolved(self):
        """Option 2 gates structure: gates.state.system etc. resolved by get_config_for_guards."""
        snap = _make_snap(event_lag_ms=500)
        cfg = {
            "gates": {
                "state": {"system": {"data_lag_threshold_ms": 1000}},
            }
        }
        assert TradingGuard(snap, cfg).is_data_ok() is True


class TestGreeksBad:
    def test_greeks_bad_when_invalid(self):
        snap = _make_snap(greeks_valid=False)
        tg = TradingGuard(snap)
        assert tg.is_greeks_bad() is True
        assert tg.is_greeks_ok() is False

    def test_greeks_ok_when_valid_finite(self):
        snap = _make_snap(greeks_valid=True, net_delta=10.0)
        tg = TradingGuard(snap)
        assert tg.is_greeks_bad() is False
        assert tg.is_greeks_ok() is True

    def test_greeks_bad_when_nan(self):
        g = GreeksSnapshot(delta=float("nan"), gamma=0.02, valid=True)
        snap = _make_snap(greeks_valid=True)
        # Replace greeks with NaN delta (snapshot is frozen so rebuild)
        snap = StateSnapshot(
            O=snap.O,
            D=snap.D,
            M=snap.M,
            L=snap.L,
            E=snap.E,
            S=snap.S,
            net_delta=snap.net_delta,
            option_delta=snap.option_delta,
            stock_pos=snap.stock_pos,
            spot=snap.spot,
            spread_pct=snap.spread_pct,
            event_lag_ms=snap.event_lag_ms,
            greeks=g,
            option_legs_count=snap.option_legs_count,
            last_hedge_ts=snap.last_hedge_ts,
            last_hedge_price=snap.last_hedge_price,
            cost_params=snap.cost_params,
            risk_limits=snap.risk_limits,
            ts=snap.ts,
        )
        assert not g.is_finite()
        assert TradingGuard(snap).is_greeks_bad() is True


class TestInNoTradeBand:
    def test_in_band_when_delta_within_epsilon(self):
        snap = _make_snap(net_delta=5.0)
        cfg = {"delta": {"epsilon_band": 10.0}}
        tg = TradingGuard(snap, cfg)
        assert tg.is_in_no_trade_band() is True

    def test_not_in_no_trade_band_when_delta_above_epsilon(self):
        snap = _make_snap(net_delta=25.0)
        cfg = {"delta": {"epsilon_band": 10.0}}
        tg = TradingGuard(snap, cfg)
        assert tg.is_in_no_trade_band() is False

    def test_boundary_epsilon(self):
        snap = _make_snap(net_delta=10.0)
        cfg = {"delta": {"epsilon_band": 10.0}}
        assert TradingGuard(snap, cfg).is_in_no_trade_band() is True


class TestCostOk:
    def test_cost_ok_when_no_last_hedge(self):
        snap = _make_snap(spot=100.0, last_hedge_price=None)
        assert TradingGuard(snap).is_cost_ok() is True

    def test_cost_ok_when_spread_extreme_fails(self):
        snap = _make_snap(spread_pct=0.6)
        cfg = {"liquidity": {"extreme_spread_pct": 0.5}}
        assert TradingGuard(snap, cfg).is_cost_ok() is False

    def test_cost_ok_when_price_moved_enough(self):
        snap = _make_snap(spot=101.0, last_hedge_price=100.0)
        cfg = {"hedge": {"min_price_move_pct": 0.2}}
        assert TradingGuard(snap, cfg).is_cost_ok() is True

    def test_cost_ok_when_price_not_moved_enough(self):
        snap = _make_snap(spot=100.1, last_hedge_price=100.0)
        cfg = {"hedge": {"min_price_move_pct": 1.0}}
        assert TradingGuard(snap, cfg).is_cost_ok() is False


class TestLiquidityOk:
    def test_liquidity_ok_when_normal_spread(self):
        snap = _make_snap(L=LiquidityState.NORMAL, spread_pct=0.05)
        assert TradingGuard(snap).is_liquidity_ok() is True

    def test_liquidity_not_ok_when_no_quote(self):
        snap = _make_snap(L=LiquidityState.NO_QUOTE)
        assert TradingGuard(snap).is_liquidity_ok() is False

    def test_liquidity_not_ok_when_extreme_wide(self):
        snap = _make_snap(L=LiquidityState.EXTREME_WIDE)
        assert TradingGuard(snap).is_liquidity_ok() is False

    def test_liquidity_ok_respects_max_spread_pct(self):
        snap = _make_snap(spread_pct=0.10)
        assert (
            TradingGuard(snap, {"risk": {"max_spread_pct": 0.05}}).is_liquidity_ok()
            is False
        )
        assert (
            TradingGuard(snap, {"risk": {"max_spread_pct": 0.15}}).is_liquidity_ok()
            is True
        )


class TestBrokerAndExec:
    def test_broker_down_when_disconnected(self):
        snap = _make_snap(E=ExecutionState.DISCONNECTED)
        tg = TradingGuard(snap)
        assert tg.is_broker_down() is True
        assert tg.is_broker_up() is False

    def test_broker_down_when_broker_error(self):
        snap = _make_snap(E=ExecutionState.BROKER_ERROR)
        tg = TradingGuard(snap)
        assert tg.is_broker_down() is True
        assert tg.is_exec_fault() is True

    def test_broker_up_when_idle(self):
        snap = _make_snap(E=ExecutionState.IDLE)
        tg = TradingGuard(snap)
        assert tg.is_broker_up() is True
        assert tg.is_exec_fault() is False


class TestOptionPosition:
    def test_have_option_position_long_gamma(self):
        snap = _make_snap(O=OptionPositionState.LONG_GAMMA)
        tg = TradingGuard(snap)
        assert tg.is_option_position() is True
        assert tg.is_no_option_position() is False

    def test_no_option_position(self):
        snap = _make_snap(O=OptionPositionState.NONE)
        tg = TradingGuard(snap)
        assert tg.is_no_option_position() is True
        assert tg.is_option_position() is False


class TestDeltaBandReady:
    def test_delta_band_ready_when_greeks_valid(self):
        snap = _make_snap(greeks_valid=True)
        assert TradingGuard(snap).is_delta_band_ready() is True

    def test_delta_band_not_ready_when_greeks_invalid(self):
        snap = _make_snap(greeks_valid=False)
        assert TradingGuard(snap).is_delta_band_ready() is False


class TestRetryAllowed:
    def test_retry_allowed_when_under_limit(self):
        snap = _make_snap()
        exec_guard = SimpleNamespace(max_daily_hedge_count=50, _daily_hedge_count=10)
        assert TradingGuard(snap, execution_guard=exec_guard).is_retry_allowed() is True

    def test_retry_not_allowed_when_at_limit(self):
        snap = _make_snap()
        exec_guard = SimpleNamespace(max_daily_hedge_count=50, _daily_hedge_count=50)
        assert TradingGuard(snap, execution_guard=exec_guard).is_retry_allowed() is False
