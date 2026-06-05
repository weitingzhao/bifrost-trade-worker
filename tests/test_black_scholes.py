"""Unit tests for Black-Scholes pricing."""

import pytest

from bifrost_worker.daemon.pricing.black_scholes import PY_VOLLIB_AVAILABLE, delta, gamma


@pytest.mark.skipif(not PY_VOLLIB_AVAILABLE, reason="py_vollib not installed")
class TestBlackScholes:
    def test_call_delta_atm(self):
        d = delta(100.0, 100.0, 0.25, 0.05, 0.25, "call")
        assert 0.4 < d < 0.6

    def test_put_delta_atm(self):
        d = delta(100.0, 100.0, 0.25, 0.05, 0.25, "put")
        assert -0.6 < d < -0.4

    def test_call_delta_itm(self):
        d = delta(110.0, 100.0, 0.25, 0.05, 0.25, "call")
        assert d > 0.7

    def test_put_delta_otm(self):
        # OTM put: spot > strike (110 > 100), delta near 0
        d = delta(110.0, 100.0, 0.25, 0.05, 0.25, "put")
        assert -0.3 < d < 0

    def test_gamma_positive(self):
        g = gamma(100.0, 100.0, 0.25, 0.05, 0.25, "call")
        assert g > 0

    def test_zero_expiry_returns_zero(self):
        assert delta(100.0, 100.0, 0.0, 0.05, 0.25, "call") == 0.0
        assert gamma(100.0, 100.0, 0.0, 0.05, 0.25, "call") == 0.0
