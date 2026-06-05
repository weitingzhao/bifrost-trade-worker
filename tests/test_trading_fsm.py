"""Trading FSM transition tests: 10+ key paths."""

import pytest

from bifrost_worker.daemon.core.state.enums import (
    ExecutionState,
    LiquidityState,
    OptionPositionState,
    SystemHealthState,
)
from bifrost_worker.daemon.core.state.snapshot import StateSnapshot, GreeksSnapshot, default_snapshot
from bifrost_worker.daemon.core.state.enums import TradingState
from bifrost_worker.daemon.fsm.events import TradingEvent
from bifrost_worker.daemon.fsm.trading_fsm import TradingFSM


def _snap(
    net_delta=0.0,
    spot=100.0,
    spread_pct=0.05,
    event_lag_ms=100.0,
    L=LiquidityState.NORMAL,
    E=ExecutionState.IDLE,
    S=SystemHealthState.OK,
    O=OptionPositionState.LONG_GAMMA,
    greeks_valid=True,
    last_hedge_price=None,
) -> StateSnapshot:
    from bifrost_worker.daemon.core.state.enums import DeltaDeviationState, MarketRegimeState
    eps = 10.0
    D = DeltaDeviationState.IN_BAND if abs(net_delta) <= eps else DeltaDeviationState.HEDGE_NEEDED
    g = GreeksSnapshot(delta=net_delta, gamma=0.02, valid=greeks_valid) if greeks_valid else None
    return StateSnapshot(
        O=O,
        D=D,
        M=MarketRegimeState.NORMAL,
        L=L,
        E=E,
        S=S,
        net_delta=net_delta,
        option_delta=net_delta,
        stock_pos=0,
        spot=spot,
        spread_pct=spread_pct,
        event_lag_ms=event_lag_ms,
        greeks=g,
        last_hedge_price=last_hedge_price,
        ts=1000.0,
    )


class TestBootToSync:
    def test_boot_to_sync_on_start(self):
        fsm = TradingFSM()
        assert fsm.state == TradingState.BOOT
        ok = fsm.apply_transition(TradingEvent.START, _snap())
        assert ok is True
        assert fsm.state == TradingState.SYNC


class TestSyncToIdleOrSafe:
    def test_sync_to_idle_when_positions_ok_and_data_ok(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        assert fsm.state == TradingState.SYNC
        ok = fsm.apply_transition(TradingEvent.SYNCED, _snap(spot=100.0, event_lag_ms=500))
        assert ok is True
        assert fsm.state == TradingState.IDLE

    def test_sync_to_safe_when_data_stale(self):
        fsm = TradingFSM(config={"system": {"data_lag_threshold_ms": 1000}})
        fsm.apply_transition(TradingEvent.START, _snap())
        snap = _snap(event_lag_ms=2000, spot=100.0)
        ok = fsm.apply_transition(TradingEvent.SYNCED, snap)
        assert ok is True
        assert fsm.state == TradingState.SAFE

    def test_sync_to_safe_when_broker_down(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        snap = _snap(E=ExecutionState.DISCONNECTED)
        ok = fsm.apply_transition(TradingEvent.SYNCED, snap)
        assert ok is True
        assert fsm.state == TradingState.SAFE


class TestIdleToArmedOrSafe:
    def test_idle_to_armed_when_option_position_and_strategy_enabled(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        assert fsm.state == TradingState.IDLE
        ok = fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        assert ok is True
        assert fsm.state == TradingState.ARMED

    def test_idle_to_safe_on_greeks_bad(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap())
        snap = _snap(greeks_valid=False)
        ok = fsm.apply_transition(TradingEvent.TICK, snap)
        assert ok is True
        assert fsm.state == TradingState.SAFE


class TestArmedToMonitor:
    def test_armed_to_monitor_when_delta_band_ready(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        assert fsm.state == TradingState.ARMED
        ok = fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=5.0))
        assert ok is True
        assert fsm.state == TradingState.MONITOR


class TestMonitorToNoTradeOrNeedHedgeOrPause:
    def test_monitor_to_no_trade_when_in_band(self):
        fsm = TradingFSM(config={"delta": {"epsilon_band": 10.0}})
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=5.0))
        assert fsm.state == TradingState.MONITOR
        ok = fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=5.0))
        assert ok is True
        assert fsm.state == TradingState.NO_TRADE

    def test_monitor_to_need_hedge_when_not_in_no_trade_band_and_cost_and_liquidity_ok(self):
        fsm = TradingFSM(config={"delta": {"epsilon_band": 10.0}})
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        assert fsm.state == TradingState.MONITOR
        ok = fsm.apply_transition(
            TradingEvent.TICK,
            _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0, spread_pct=0.05),
        )
        assert ok is True
        assert fsm.state == TradingState.NEED_HEDGE

    def test_monitor_to_pause_cost_when_not_in_no_trade_band_and_not_cost_ok(self):
        fsm = TradingFSM(config={
            "delta": {"epsilon_band": 10.0},
            "hedge": {"min_price_move_pct": 10.0},
        })
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        snap = _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0, last_hedge_price=100.0, spot=100.05)
        ok = fsm.apply_transition(TradingEvent.TICK, snap)
        assert ok is True
        assert fsm.state == TradingState.PAUSE_COST

    def test_monitor_to_pause_liq_when_not_in_no_trade_band_and_not_liquidity_ok(self):
        fsm = TradingFSM(config={"delta": {"epsilon_band": 10.0}, "risk": {"max_spread_pct": 0.02}})
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        snap = _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0, L=LiquidityState.WIDE, spread_pct=0.15)
        ok = fsm.apply_transition(TradingEvent.TICK, snap)
        assert ok is True
        assert fsm.state == TradingState.PAUSE_LIQ


class TestNeedHedgeToHedgingAndBack:
    def test_need_hedge_to_hedging_on_target_emitted(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        assert fsm.state == TradingState.NEED_HEDGE
        ok = fsm.apply_transition(TradingEvent.TARGET_EMITTED, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        assert ok is True
        assert fsm.state == TradingState.HEDGING

    def test_hedging_to_monitor_on_hedge_done(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        fsm.apply_transition(TradingEvent.TARGET_EMITTED, _snap(O=OptionPositionState.LONG_GAMMA))
        assert fsm.state == TradingState.HEDGING
        ok = fsm.apply_transition(TradingEvent.HEDGE_DONE, _snap(O=OptionPositionState.LONG_GAMMA))
        assert ok is True
        assert fsm.state == TradingState.MONITOR

    def test_hedging_to_need_hedge_on_hedge_failed_when_retry_allowed(self):
        fsm = TradingFSM(guard=type("G", (), {"max_daily_hedge_count": 50, "_daily_hedge_count": 5})())  # execution guard for retry_allowed
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=30.0))
        fsm.apply_transition(TradingEvent.TARGET_EMITTED, _snap(O=OptionPositionState.LONG_GAMMA))
        ok = fsm.apply_transition(TradingEvent.HEDGE_FAILED, _snap(O=OptionPositionState.LONG_GAMMA))
        assert ok is True
        assert fsm.state == TradingState.NEED_HEDGE


class TestAnyToSafe:
    def test_monitor_to_safe_on_broker_down(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.TICK, _snap(O=OptionPositionState.LONG_GAMMA))
        fsm.apply_transition(TradingEvent.GREEKS_UPDATE, _snap(O=OptionPositionState.LONG_GAMMA, net_delta=5.0))
        snap = _snap(O=OptionPositionState.LONG_GAMMA, E=ExecutionState.DISCONNECTED)
        ok = fsm.apply_transition(TradingEvent.TICK, snap)
        assert ok is True
        assert fsm.state == TradingState.SAFE


class TestSafeToSync:
    def test_safe_to_sync_on_manual_resume_when_broker_up_and_data_ok(self):
        fsm = TradingFSM()
        fsm.apply_transition(TradingEvent.START, _snap())
        fsm.apply_transition(TradingEvent.SYNCED, _snap(event_lag_ms=2000))
        assert fsm.state == TradingState.SAFE
        ok = fsm.apply_transition(TradingEvent.MANUAL_RESUME, _snap(event_lag_ms=500))
        assert ok is True
        assert fsm.state == TradingState.SYNC
