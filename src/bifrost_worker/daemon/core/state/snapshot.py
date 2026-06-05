"""StateSnapshot / WorldState: immutable O/D/M/L/E/S plus raw data for guards."""

import math
import time
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional

from bifrost_worker.daemon.core.state.enums import (
    DeltaDeviationState,
    ExecutionState,
    LiquidityState,
    MarketRegimeState,
    OptionPositionState,
    SystemHealthState,
)


@dataclass(frozen=True)
class GreeksSnapshot:
    """Immutable snapshot of greeks (delta, gamma, optional theta/vega)."""

    delta: float
    gamma: float
    theta: float = 0.0
    vega: float = 0.0
    valid: bool = False

    def is_finite(self) -> bool:
        return (
            math.isfinite(self.delta)
            and math.isfinite(self.gamma)
            and math.isfinite(self.theta)
            and math.isfinite(self.vega)
        )


@dataclass(frozen=True)
class StateSnapshot:
    """
    Immutable world state for guard evaluation and FSM transitions.
    O/D/M/L/E/S from state space; E can be from hedge_fsm (HedgeFSM) or legacy ExecutionState.
    """

    # State space dimensions
    O: OptionPositionState
    D: DeltaDeviationState
    M: MarketRegimeState
    L: LiquidityState
    E: ExecutionState  # from execution layer (hedge_fsm maps to E0..E4)
    S: SystemHealthState
    # Numeric
    net_delta: float
    option_delta: float
    stock_pos: int
    # Raw market / data
    spot: Optional[float]
    spread_pct: Optional[float]
    event_lag_ms: Optional[float]
    greeks: Optional[GreeksSnapshot]
    # Positions summary (e.g. option_legs count or serializable summary)
    option_legs_count: int = 0
    last_hedge_ts: Optional[float] = None
    last_hedge_price: Optional[float] = None
    # Cost / risk params (for guards; optional dict or typed)
    cost_params: Optional[Dict[str, Any]] = None
    risk_limits: Optional[Dict[str, Any]] = None
    ts: float = 0.0

    # Legacy compatibility: spread and data_lag_ms alias
    @property
    def spread(self) -> Optional[float]:
        return self.spread_pct

    @property
    def data_lag_ms(self) -> Optional[float]:
        return self.event_lag_ms

    @property
    def greeks_valid(self) -> bool:
        return self.greeks is not None and self.greeks.valid and self.greeks.is_finite()

    @classmethod
    def from_composite_state(
        cls,
        cs: Any,
        spot: Optional[float] = None,
        greeks_snapshot: Optional[GreeksSnapshot] = None,
        cost_params: Optional[Dict[str, Any]] = None,
        risk_limits: Optional[Dict[str, Any]] = None,
        option_legs_count: int = 0,
    ) -> "StateSnapshot":
        """Build StateSnapshot from CompositeState (e.g. from StateClassifier)."""
        return cls(
            O=cs.O,
            D=cs.D,
            M=cs.M,
            L=cs.L,
            E=cs.E,
            S=cs.S,
            net_delta=cs.net_delta,
            option_delta=cs.option_delta,
            stock_pos=cs.stock_pos,
            spot=spot or getattr(cs, "spot", None),
            spread_pct=cs.spread,
            event_lag_ms=cs.data_lag_ms,
            greeks=greeks_snapshot,
            option_legs_count=option_legs_count,
            last_hedge_ts=cs.last_hedge_ts,
            last_hedge_price=cs.last_hedge_price,
            cost_params=cost_params,
            risk_limits=risk_limits,
            ts=cs.ts,
        )

    def update(self, event: Dict[str, Any]) -> "StateSnapshot":
        """Return new StateSnapshot with event fields applied (pure)."""
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
            "spot": self.spot,
            "spread_pct": self.spread_pct,
            "event_lag_ms": self.event_lag_ms,
            "greeks": self.greeks,
            "option_legs_count": self.option_legs_count,
            "last_hedge_ts": self.last_hedge_ts,
            "last_hedge_price": self.last_hedge_price,
            "cost_params": self.cost_params,
            "risk_limits": self.risk_limits,
            "ts": self.ts,
        }
        for key in (
            "net_delta", "option_delta", "stock_pos", "spot", "spread_pct",
            "event_lag_ms", "greeks", "option_legs_count", "last_hedge_ts",
            "last_hedge_price", "cost_params", "risk_limits", "ts",
            "O", "D", "M", "L", "E", "S",
        ):
            if key in event:
                v = event[key]
                if key == "ts":
                    kwargs["ts"] = float(v)
                elif key in ("O", "D", "M", "L", "E", "S"):
                    kwargs[key] = v
                elif key == "greeks" and isinstance(v, dict):
                    kwargs["greeks"] = GreeksSnapshot(**v) if v else None
                elif key in ("net_delta", "option_delta", "spot", "spread_pct", "event_lag_ms", "last_hedge_ts", "last_hedge_price"):
                    kwargs[key] = v if v is None else float(v)
                elif key == "stock_pos" or key == "option_legs_count":
                    kwargs[key] = int(v)
                else:
                    kwargs[key] = v
        kwargs.setdefault("ts", time.time())
        return replace(self, **kwargs)


def default_snapshot() -> StateSnapshot:
    """Safe default snapshot (no position, no quote, safe state)."""
    return StateSnapshot(
        O=OptionPositionState.NONE,
        D=DeltaDeviationState.IN_BAND,
        M=MarketRegimeState.NORMAL,
        L=LiquidityState.NO_QUOTE,
        E=ExecutionState.IDLE,
        S=SystemHealthState.OK,
        net_delta=0.0,
        option_delta=0.0,
        stock_pos=0,
        spot=None,
        spread_pct=None,
        event_lag_ms=None,
        greeks=None,
        option_legs_count=0,
        last_hedge_ts=None,
        last_hedge_price=None,
        cost_params=None,
        risk_limits=None,
        ts=time.time(),
    )
