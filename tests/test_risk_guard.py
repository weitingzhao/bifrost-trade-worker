"""Unit tests for ExecutionGuard (Hedge Execution FSM order-send gate)."""

import time
from datetime import date, timedelta

import pytest

from bifrost_worker.daemon.guards.execution_guard import ExecutionGuard


class TestExecutionGuard:
    def test_allow_hedge_ok(self):
        guard = ExecutionGuard(cooldown_sec=60, trading_hours_only=False)
        allowed, reason = guard.allow_hedge(time.time(), 0, "BUY", 100)
        assert allowed is True
        assert reason == "ok"

    def test_cooldown_blocks(self):
        guard = ExecutionGuard(cooldown_sec=60, trading_hours_only=False)
        guard.record_hedge_sent()
        allowed, reason = guard.allow_hedge(time.time(), 0, "BUY", 100)
        assert allowed is False
        assert reason == "cooldown"

    def test_max_daily_hedge_count(self):
        guard = ExecutionGuard(max_daily_hedge_count=2, trading_hours_only=False)
        guard.set_daily_hedge_count(2)
        allowed, reason = guard.allow_hedge(time.time() + 100, 0, "BUY", 100)
        assert allowed is False
        assert reason == "max_daily_hedge_count"

    def test_max_position_blocks(self):
        guard = ExecutionGuard(max_position_shares=500, trading_hours_only=False)
        allowed, reason = guard.allow_hedge(time.time(), 400, "BUY", 200)
        assert allowed is False
        assert reason == "max_position"

    def test_circuit_breaker_blocks(self):
        guard = ExecutionGuard(trading_hours_only=False)
        guard.set_circuit_breaker(True)
        allowed, reason = guard.allow_hedge(time.time(), 0, "BUY", 100)
        assert allowed is False
        assert reason == "circuit_breaker"

    def test_earnings_blackout(self):
        tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        guard = ExecutionGuard(
            earnings_dates=[tomorrow],
            blackout_days_before=3,
            blackout_days_after=1,
            trading_hours_only=False,
        )
        allowed, reason = guard.allow_hedge(time.time(), 0, "BUY", 100)
        assert allowed is False
        assert reason == "earnings_blackout"

    def test_spread_too_wide_blocks(self):
        guard = ExecutionGuard(max_spread_pct=0.5, trading_hours_only=False)
        allowed, reason = guard.allow_hedge(
            time.time() + 100, 0, "BUY", 100, spread_pct=1.0
        )
        assert allowed is False
        assert reason == "spread_too_wide"

    def test_min_price_move_blocks(self):
        guard = ExecutionGuard(
            min_price_move_pct=0.5,
            trading_hours_only=False,
        )
        guard.set_last_hedge_time(time.time() - 100)
        allowed, reason = guard.allow_hedge(
            time.time(), 0, "BUY", 100,
            spot=100.0,
            last_hedge_price=100.2,
        )
        assert allowed is False
        assert reason == "min_price_move"
