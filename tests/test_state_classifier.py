"""Unit tests for StateClassifier: O, D, M, L, E, S mapping (2+ cases per state)."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from bifrost_worker.daemon.core.state.classifier import StateClassifier
from bifrost_worker.daemon.core.state.composite import CompositeState
from bifrost_worker.daemon.core.state.enums import (
    DeltaDeviationState,
    ExecutionState,
    LiquidityState,
    MarketRegimeState,
    OptionPositionState,
    SystemHealthState,
)
from bifrost_core.portfolio.positions.portfolio import OptionLeg


def _future_yyyymmdd(days: int) -> str:
    d = datetime.now(timezone.utc) + timedelta(days=days)
    return d.strftime("%Y%m%d")


# --- O: OptionPositionState ---
class TestClassifyO:
    def test_o0_no_options(self):
        g = SimpleNamespace(valid=True, gamma=0.0, delta=0.0, _legs=[])
        assert StateClassifier._classify_o(g) == OptionPositionState.NONE

    def test_o0_gamma_zero_with_legs(self):
        g = SimpleNamespace(valid=True, gamma=0.0, delta=50.0, _legs=[1])
        assert StateClassifier._classify_o(g) == OptionPositionState.NONE

    def test_o1_long_gamma(self):
        g = SimpleNamespace(valid=True, gamma=0.01, delta=20.0, _legs=[1])
        assert StateClassifier._classify_o(g) == OptionPositionState.LONG_GAMMA

    def test_o1_long_gamma_small(self):
        g = SimpleNamespace(valid=True, gamma=1e-6, delta=0.0, _legs=[1])
        assert StateClassifier._classify_o(g) == OptionPositionState.LONG_GAMMA

    def test_o2_short_gamma(self):
        g = SimpleNamespace(valid=True, gamma=-0.01, delta=-20.0, _legs=[1])
        assert StateClassifier._classify_o(g) == OptionPositionState.SHORT_GAMMA

    def test_o2_short_gamma_small(self):
        g = SimpleNamespace(valid=True, gamma=-1e-6, delta=0.0, _legs=[1])
        assert StateClassifier._classify_o(g) == OptionPositionState.SHORT_GAMMA

    def test_o_invalid_greeks(self):
        g = SimpleNamespace(valid=False, gamma=0.5, _legs=[1])
        assert StateClassifier._classify_o(g) == OptionPositionState.NONE


# --- D: DeltaDeviationState ---
class TestClassifyD:
    def test_d0_in_band(self):
        assert StateClassifier._classify_d(5.0, True, {}) == DeltaDeviationState.IN_BAND
        assert StateClassifier._classify_d(-8.0, True, {"delta": {"epsilon_band": 10}}) == DeltaDeviationState.IN_BAND

    def test_d0_boundary_epsilon(self):
        cfg = {"delta": {"epsilon_band": 10.0}}
        assert StateClassifier._classify_d(10.0, True, cfg) == DeltaDeviationState.IN_BAND
        assert StateClassifier._classify_d(-10.0, True, cfg) == DeltaDeviationState.IN_BAND

    def test_d1_minor(self):
        cfg = {"delta": {"epsilon_band": 10, "threshold_hedge_shares": 25}}
        assert StateClassifier._classify_d(15.0, True, cfg) == DeltaDeviationState.MINOR
        assert StateClassifier._classify_d(-20.0, True, cfg) == DeltaDeviationState.MINOR

    def test_d2_hedge_needed(self):
        cfg = {"delta": {"epsilon_band": 10, "threshold_hedge_shares": 25, "max_delta_limit": 500}}
        assert StateClassifier._classify_d(30.0, True, cfg) == DeltaDeviationState.HEDGE_NEEDED
        assert StateClassifier._classify_d(-25.0, True, cfg) == DeltaDeviationState.HEDGE_NEEDED

    def test_d2_boundary_threshold_hedge_shares(self):
        cfg = {"delta": {"epsilon_band": 10, "threshold_hedge_shares": 25, "max_delta_limit": 500}}
        assert StateClassifier._classify_d(25.0, True, cfg) == DeltaDeviationState.HEDGE_NEEDED

    def test_d3_force_hedge(self):
        cfg = {"delta": {"epsilon_band": 10, "threshold_hedge_shares": 25, "max_delta_limit": 500}}
        assert StateClassifier._classify_d(500.0, True, cfg) == DeltaDeviationState.FORCE_HEDGE
        assert StateClassifier._classify_d(-600.0, True, cfg) == DeltaDeviationState.FORCE_HEDGE

    def test_d4_invalid(self):
        assert StateClassifier._classify_d(100.0, False, {}) == DeltaDeviationState.INVALID


# --- M: MarketRegimeState ---
class TestClassifyM:
    def test_m5_stale(self):
        md = SimpleNamespace(last_ts=None)
        # No last_ts -> no stale
        cfg = {"market": {"stale_ts_threshold_ms": 1000}}
        # Use a mock that has last_ts very old
        import time
        md_old = SimpleNamespace(last_ts=time.time() - 100)
        assert StateClassifier._classify_m(md_old, cfg) == MarketRegimeState.STALE

    def test_m_normal_no_history(self):
        md = SimpleNamespace(last_ts=None)
        assert StateClassifier._classify_m(md, {}, price_history=None) == MarketRegimeState.NORMAL

    def test_m_quiet_low_vol(self):
        md = SimpleNamespace(last_ts=None)
        hist = [100.0] * 10
        assert StateClassifier._classify_m(md, {}, price_history=hist) == MarketRegimeState.QUIET

    def test_m_trend(self):
        md = SimpleNamespace(last_ts=None)
        hist = [100 + i * 0.1 for i in range(20)]
        assert StateClassifier._classify_m(md, {}, price_history=hist) == MarketRegimeState.TREND


# --- L: LiquidityState ---
class TestClassifyL:
    def test_l0_normal(self):
        md = SimpleNamespace(spread_pct=0.05)
        assert StateClassifier._classify_l(md, {}) == LiquidityState.NORMAL

    def test_l1_wide(self):
        md = SimpleNamespace(spread_pct=0.15)
        cfg = {"liquidity": {"wide_spread_pct": 0.1, "extreme_spread_pct": 0.5}}
        assert StateClassifier._classify_l(md, cfg) == LiquidityState.WIDE

    def test_l2_extreme_wide(self):
        md = SimpleNamespace(spread_pct=0.6)
        cfg = {"liquidity": {"wide_spread_pct": 0.1, "extreme_spread_pct": 0.5}}
        assert StateClassifier._classify_l(md, cfg) == LiquidityState.EXTREME_WIDE

    def test_l3_no_quote(self):
        md = SimpleNamespace(spread_pct=None)
        assert StateClassifier._classify_l(md, {}) == LiquidityState.NO_QUOTE

    def test_l_boundary_wide(self):
        md = SimpleNamespace(spread_pct=0.1)
        cfg = {"liquidity": {"wide_spread_pct": 0.1, "extreme_spread_pct": 0.5}}
        assert StateClassifier._classify_l(md, cfg) == LiquidityState.WIDE


# --- E: ExecutionState ---
class TestClassifyE:
    def test_e0_idle(self):
        om = SimpleNamespace(effective_e_state=lambda: ExecutionState.IDLE)
        assert StateClassifier._classify_e(om) == ExecutionState.IDLE

    def test_e1_working(self):
        om = SimpleNamespace(effective_e_state=lambda: ExecutionState.ORDER_WORKING)
        assert StateClassifier._classify_e(om) == ExecutionState.ORDER_WORKING

    def test_e2_partial_fill(self):
        om = SimpleNamespace(effective_e_state=lambda: ExecutionState.PARTIAL_FILL)
        assert StateClassifier._classify_e(om) == ExecutionState.PARTIAL_FILL

    def test_e3_disconnected(self):
        om = SimpleNamespace(effective_e_state=lambda: ExecutionState.DISCONNECTED)
        assert StateClassifier._classify_e(om) == ExecutionState.DISCONNECTED

    def test_e4_broker_error(self):
        om = SimpleNamespace(effective_e_state=lambda: ExecutionState.BROKER_ERROR)
        assert StateClassifier._classify_e(om) == ExecutionState.BROKER_ERROR


# --- S: SystemHealthState ---
class TestClassifyS:
    def test_s0_ok(self):
        assert StateClassifier._classify_s(True, None, False, {}) == SystemHealthState.OK
        assert StateClassifier._classify_s(True, 500.0, False, {"system": {"data_lag_threshold_ms": 1000}}) == SystemHealthState.OK

    def test_s1_greeks_bad(self):
        assert StateClassifier._classify_s(False, None, False, {}) == SystemHealthState.GREEKS_BAD

    def test_s2_data_lag(self):
        cfg = {"system": {"data_lag_threshold_ms": 1000}}
        assert StateClassifier._classify_s(True, 1500.0, False, cfg) == SystemHealthState.DATA_LAG

    def test_s2_boundary(self):
        cfg = {"system": {"data_lag_threshold_ms": 1000}}
        assert StateClassifier._classify_s(True, 1000.0, False, cfg) == SystemHealthState.OK

    def test_s3_risk_halt(self):
        assert StateClassifier._classify_s(True, None, True, {}) == SystemHealthState.RISK_HALT
        assert StateClassifier._classify_s(False, 2000.0, True, {}) == SystemHealthState.RISK_HALT


# --- Full classify() ---
class TestClassifyFull:
    def test_classify_returns_composite_state(self):
        pb = SimpleNamespace(stock_shares=0)
        md = SimpleNamespace(spread_pct=0.05, last_ts=None)
        g = SimpleNamespace(valid=True, delta=0.0, gamma=0.01, _legs=[1])
        om = SimpleNamespace(effective_e_state=lambda: ExecutionState.IDLE)
        cs = StateClassifier.classify(pb, md, g, om, config={})
        assert isinstance(cs, CompositeState)
        assert cs.O == OptionPositionState.LONG_GAMMA
        assert cs.D == DeltaDeviationState.IN_BAND
        assert cs.E == ExecutionState.IDLE
        assert cs.S == SystemHealthState.OK
        assert cs.stock_pos == 0
        assert cs.greeks_valid is True
