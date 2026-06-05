"""Unit tests for gamma scalper hedge logic (target-position based)."""

import pytest

from bifrost_worker.daemon.strategy.gamma_scalper import HedgeOrder, compute_target_and_need, gamma_scalper_hedge


class TestComputeTargetAndNeed:
    def test_need_equals_neg_port_delta(self):
        target, need = compute_target_and_need(50.0, 0)
        assert need == -50.0
        target, need = compute_target_and_need(-30.0, 10)
        assert need == 30.0


class TestGammaScalperHedge:
    def test_no_hedge_within_threshold(self):
        assert gamma_scalper_hedge(20.0, 0, threshold_hedge_shares=25) is None
        assert gamma_scalper_hedge(-20.0, 0, threshold_hedge_shares=25) is None
        assert gamma_scalper_hedge(0.0, 0) is None

    def test_sell_when_need_negative(self):
        h = gamma_scalper_hedge(50.0, 0, threshold_hedge_shares=25)
        assert h is not None
        assert isinstance(h, HedgeOrder)
        assert h.side == "SELL"
        assert h.quantity == 50

    def test_buy_when_need_positive(self):
        h = gamma_scalper_hedge(-50.0, 0, threshold_hedge_shares=25)
        assert h is not None
        assert h.side == "BUY"
        assert h.quantity == 50

    def test_max_hedge_capped(self):
        h = gamma_scalper_hedge(1000.0, 0, threshold_hedge_shares=25, max_hedge_shares_per_order=500)
        assert h is not None
        assert h.side == "SELL"
        assert h.quantity == 500

    def test_custom_threshold(self):
        assert gamma_scalper_hedge(30.0, 0, threshold_hedge_shares=50) is None
        h = gamma_scalper_hedge(60.0, 0, threshold_hedge_shares=50)
        assert h is not None
        assert h.quantity == 60
