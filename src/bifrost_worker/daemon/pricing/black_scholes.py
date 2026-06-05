"""Black-Scholes delta and gamma via py_vollib."""

import logging
from typing import Optional

try:
    from py_vollib.black_scholes.greeks.analytical import delta as _delta, gamma as _gamma
    PY_VOLLIB_AVAILABLE = True
except ImportError:
    PY_VOLLIB_AVAILABLE = False

logger = logging.getLogger(__name__)


def delta(
    underlying_price: float,
    strike: float,
    time_to_expiration: float,
    risk_free_rate: float,
    volatility: float,
    option_type: str,
) -> float:
    """Option delta (per unit). option_type: 'call' or 'put'."""
    if time_to_expiration <= 0:
        return 0.0
    if not PY_VOLLIB_AVAILABLE:
        logger.error("py_vollib not available")
        return 0.0
    try:
        flag = "c" if option_type.upper() in ("C", "CALL") else "p"
        return float(_delta(flag, underlying_price, strike, time_to_expiration, risk_free_rate, volatility))
    except Exception as e:
        logger.error("BS delta error: %s", e)
        return 0.0


def gamma(
    underlying_price: float,
    strike: float,
    time_to_expiration: float,
    risk_free_rate: float,
    volatility: float,
    option_type: str,
) -> float:
    """Option gamma (per unit). option_type: 'call' or 'put'."""
    if time_to_expiration <= 0:
        return 0.0
    if not PY_VOLLIB_AVAILABLE:
        logger.error("py_vollib not available")
        return 0.0
    try:
        flag = "c" if option_type.upper() in ("C", "CALL") else "p"
        return float(_gamma(flag, underlying_price, strike, time_to_expiration, risk_free_rate, volatility))
    except Exception as e:
        logger.error("BS gamma error: %s", e)
        return 0.0


def calculate_greeks(
    underlying_price: float,
    strike: float,
    time_to_expiration: float,
    risk_free_rate: float,
    volatility: float,
    option_type: str,
) -> dict:
    """Return dict with delta, gamma (and 0 theta/vega for compatibility)."""
    return {
        "delta": delta(underlying_price, strike, time_to_expiration, risk_free_rate, volatility, option_type),
        "gamma": gamma(underlying_price, strike, time_to_expiration, risk_free_rate, volatility, option_type),
        "theta": 0.0,
        "vega": 0.0,
    }
