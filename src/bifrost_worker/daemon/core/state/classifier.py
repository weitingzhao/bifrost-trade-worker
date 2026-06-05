"""State classifier: map raw data to six discrete states O,D,M,L,E,S."""

import math
import time
from typing import Any, Dict, List, Optional

from bifrost_worker.daemon.core.state.composite import CompositeState
from bifrost_worker.daemon.core.state.enums import (
    DeltaDeviationState,
    ExecutionState,
    LiquidityState,
    MarketRegimeState,
    OptionPositionState,
    SystemHealthState,
)

# Default thresholds (overridable via config)
_DEFAULT = {
    "delta": {
        "epsilon_band": 10.0,
        "threshold_hedge_shares": 25.0,
        "max_delta_limit": 500.0,
    },
    "market": {
        "vol_window_min": 5,
        "stale_ts_threshold_ms": 5000.0,
    },
    "liquidity": {
        "wide_spread_pct": 0.1,
        "extreme_spread_pct": 0.5,
    },
    "system": {
        "data_lag_threshold_ms": 1000.0,
    },
}


def _get_cfg(config: Dict[str, Any], section: str, key: str, default: Any) -> Any:
    """Read section from top-level or state_space (backward compat)."""
    sec = config.get(section) or (config.get("state_space") or {}).get(section, {})
    if isinstance(sec, dict):
        return sec.get(key, _DEFAULT.get(section, {}).get(key, default))
    return default


class StateClassifier:
    """Maps position_book, market_data, greeks, execution -> CompositeState."""

    @staticmethod
    def _classify_o(greeks: Any) -> OptionPositionState:
        # No options -> O0; portfolio gamma > 0 -> O1; < 0 -> O2
        if not getattr(greeks, "valid", False):
            return OptionPositionState.NONE
        legs = getattr(greeks, "_legs", [])
        if not legs:
            return OptionPositionState.NONE
        gamma = getattr(greeks, "gamma", 0.0)
        if gamma > 0:
            return OptionPositionState.LONG_GAMMA
        if gamma < 0:
            return OptionPositionState.SHORT_GAMMA
        return OptionPositionState.NONE

    @staticmethod
    def _classify_d(
        net_delta: float,
        greeks_valid: bool,
        config: Dict[str, Any],
    ) -> DeltaDeviationState:
        if not greeks_valid:
            return DeltaDeviationState.INVALID
        epsilon = _get_cfg(config, "delta", "epsilon_band", 10.0)
        # threshold_hedge_shares (backward compat: hedge_threshold)
        sec = config.get("delta") or (config.get("state_space") or {}).get("delta", {})
        threshold = sec.get("threshold_hedge_shares", sec.get("hedge_threshold", 25.0))
        max_limit = _get_cfg(config, "delta", "max_delta_limit", 500.0)
        abs_d = abs(net_delta)
        if abs_d <= epsilon:
            return DeltaDeviationState.IN_BAND
        if abs_d >= max_limit:
            return DeltaDeviationState.FORCE_HEDGE
        if abs_d >= threshold:
            return DeltaDeviationState.HEDGE_NEEDED
        return DeltaDeviationState.MINOR

    @staticmethod
    def _classify_m(
        market_data: Any,
        config: Dict[str, Any],
        price_history: Optional[List[float]] = None,
    ) -> MarketRegimeState:
        # Stale: data timestamp too old
        last_ts = getattr(market_data, "last_ts", None)
        stale_ms = _get_cfg(config, "market", "stale_ts_threshold_ms", 5000.0)
        if last_ts is not None:
            lag_ms = (time.time() - last_ts) * 1000.0
            if lag_ms > stale_ms:
                return MarketRegimeState.STALE
        # Simplified: no price history -> NORMAL; with history could do vol/trend/gap
        if price_history and len(price_history) >= 2:
            # Very simple: variance and slope
            n = len(price_history)
            mean = sum(price_history) / n
            var = sum((x - mean) ** 2 for x in price_history) / max(n - 1, 1)
            vol = math.sqrt(var) / mean if mean else 0
            slope = (price_history[-1] - price_history[0]) / n if n else 0
            if vol > 0.02 and abs(slope) < 0.001:
                return MarketRegimeState.CHOPPY_HIGHVOL
            if abs(slope) > 0.005:
                return MarketRegimeState.TREND
            if vol < 0.005:
                return MarketRegimeState.QUIET
        return MarketRegimeState.NORMAL

    @staticmethod
    def _classify_l(market_data: Any, config: Dict[str, Any]) -> LiquidityState:
        spread_pct = getattr(market_data, "spread_pct", None)
        if spread_pct is None:
            return LiquidityState.NO_QUOTE
        wide = _get_cfg(config, "liquidity", "wide_spread_pct", 0.1)
        extreme = _get_cfg(config, "liquidity", "extreme_spread_pct", 0.5)
        if spread_pct >= extreme:
            return LiquidityState.EXTREME_WIDE
        if spread_pct >= wide:
            return LiquidityState.WIDE
        return LiquidityState.NORMAL

    @staticmethod
    def _classify_e(execution: Any) -> ExecutionState:
        e_state = getattr(execution, "effective_e_state", None)
        if callable(e_state):
            return e_state()
        return getattr(execution, "execution_state", ExecutionState.IDLE)

    @staticmethod
    def _classify_s(
        greeks_valid: bool,
        data_lag_ms: Optional[float],
        risk_halt: bool,
        config: Dict[str, Any],
    ) -> SystemHealthState:
        if risk_halt:
            return SystemHealthState.RISK_HALT
        if not greeks_valid:
            return SystemHealthState.GREEKS_BAD
        lag_threshold = _get_cfg(config, "system", "data_lag_threshold_ms", 1000.0)
        if data_lag_ms is not None and data_lag_ms > lag_threshold:
            return SystemHealthState.DATA_LAG
        return SystemHealthState.OK

    @classmethod
    def classify(
        cls,
        position_book: Any,
        market_data: Any,
        greeks: Any,
        execution: Any,
        last_hedge_price: Optional[float] = None,
        last_hedge_ts: Optional[float] = None,
        data_lag_ms: Optional[float] = None,
        risk_halt: bool = False,
        config: Optional[Dict[str, Any]] = None,
        price_history: Optional[List[float]] = None,
    ) -> CompositeState:
        """Produce CompositeState from runtime objects."""
        config = config or {}
        O = cls._classify_o(greeks)
        greeks_valid = getattr(greeks, "valid", False)
        port_delta = getattr(greeks, "delta", 0.0)
        stock_pos = getattr(position_book, "stock_shares", 0)
        option_delta = port_delta - float(stock_pos)  # option contribution in shares
        net_delta = port_delta  # net_delta = option_delta + stock_pos
        D = cls._classify_d(net_delta, greeks_valid, config)
        M = cls._classify_m(market_data, config, price_history)
        L = cls._classify_l(market_data, config)
        E = cls._classify_e(execution)
        S = cls._classify_s(greeks_valid, data_lag_ms, risk_halt, config)
        spread = getattr(market_data, "spread_pct", None)
        if data_lag_ms is None and getattr(market_data, "last_ts", None) is not None:
            data_lag_ms = (time.time() - market_data.last_ts) * 1000.0
        return CompositeState(
            O=O,
            D=D,
            M=M,
            L=L,
            E=E,
            S=S,
            net_delta=net_delta,
            option_delta=option_delta,
            stock_pos=stock_pos,
            last_hedge_price=last_hedge_price,
            last_hedge_ts=last_hedge_ts,
            spread=spread,
            data_lag_ms=data_lag_ms,
            greeks_valid=greeks_valid,
            ts=time.time(),
        )
