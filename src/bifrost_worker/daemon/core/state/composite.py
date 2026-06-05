"""Composite state: O,D,M,L,E,S plus numeric snapshots."""

import time
from dataclasses import dataclass, replace
from typing import Any, Dict, Optional

from bifrost_worker.daemon.core.state.enums import (
    DeltaDeviationState,
    ExecutionState,
    LiquidityState,
    MarketRegimeState,
    OptionPositionState,
    SystemHealthState,
)


@dataclass(frozen=True)
class CompositeState:
    """Immutable composite state for gamma scalping FSM."""

    O: OptionPositionState
    D: DeltaDeviationState
    M: MarketRegimeState
    L: LiquidityState
    E: ExecutionState
    S: SystemHealthState
    # Numeric snapshots
    net_delta: float
    option_delta: float
    stock_pos: int
    last_hedge_price: Optional[float]
    last_hedge_ts: Optional[float]
    spread: Optional[float]  # spread_pct
    data_lag_ms: Optional[float]
    greeks_valid: bool
    ts: float

    @classmethod
    def from_runtime(
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
    ) -> "CompositeState":
        """Build CompositeState from runtime objects. Classification done by StateClassifier."""
        from bifrost_worker.daemon.core.state.classifier import StateClassifier  # avoid circular import

        return StateClassifier.classify(
            position_book=position_book,
            market_data=market_data,
            greeks=greeks,
            execution=execution,
            last_hedge_price=last_hedge_price,
            last_hedge_ts=last_hedge_ts,
            data_lag_ms=data_lag_ms,
            risk_halt=risk_halt,
            config=config or {},
        )

    def update(self, event: Dict[str, Any]) -> "CompositeState":
        """Pure function: return new CompositeState from event (tick, position, order_status, greeks)."""
        # Minimal update: only override fields present in event; rest copy from self.
        kwargs: Dict[str, Any] = {
            "O": self.O,
            "D": self.D,
            "M": self.M,
            "L": self.L,
            "E": self.E,
            "S": self.S,
            "net_delta": self.net_delta,
            "option_delta": self.option_delta,
            "stock_pos": self.stock_pos,
            "last_hedge_price": self.last_hedge_price,
            "last_hedge_ts": self.last_hedge_ts,
            "spread": self.spread,
            "data_lag_ms": self.data_lag_ms,
            "greeks_valid": self.greeks_valid,
            "ts": self.ts,
        }
        if "net_delta" in event:
            kwargs["net_delta"] = float(event["net_delta"])
        if "option_delta" in event:
            kwargs["option_delta"] = float(event["option_delta"])
        if "stock_pos" in event:
            kwargs["stock_pos"] = int(event["stock_pos"])
        if "last_hedge_price" in event:
            kwargs["last_hedge_price"] = event["last_hedge_price"]
        if "last_hedge_ts" in event:
            kwargs["last_hedge_ts"] = event["last_hedge_ts"]
        if "spread" in event:
            kwargs["spread"] = event["spread"]
        if "data_lag_ms" in event:
            kwargs["data_lag_ms"] = event["data_lag_ms"]
        if "greeks_valid" in event:
            kwargs["greeks_valid"] = bool(event["greeks_valid"])
        if "ts" in event:
            kwargs["ts"] = float(event["ts"])
        if "O" in event:
            kwargs["O"] = event["O"]
        if "D" in event:
            kwargs["D"] = event["D"]
        if "M" in event:
            kwargs["M"] = event["M"]
        if "L" in event:
            kwargs["L"] = event["L"]
        if "E" in event:
            kwargs["E"] = event["E"]
        if "S" in event:
            kwargs["S"] = event["S"]
        kwargs["ts"] = kwargs.get("ts", time.time())
        return replace(self, **kwargs)
