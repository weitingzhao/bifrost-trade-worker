"""Event enums and payload types for Trading FSM and Hedge Execution FSM."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class TradingEvent(str, Enum):
    """Top-level Trading FSM events."""

    START = "start"
    SYNCED = "synced"
    TICK = "tick"
    QUOTE = "quote"
    GREEKS_UPDATE = "greeks_update"
    TARGET_EMITTED = "target_emitted"
    HEDGE_DONE = "hedge_done"
    HEDGE_FAILED = "hedge_failed"
    DATA_STALE = "data_stale"
    GREEKS_BAD = "greeks_bad"
    BROKER_DOWN = "broker_down"
    BROKER_UP = "broker_up"
    MANUAL_RESUME = "manual_resume"
    SHUTDOWN = "shutdown"


class HedgeEvent(str, Enum):
    """Hedge FSM events."""

    RECV_TARGET = "recv_target"
    PLAN_SKIP = "plan_skip"
    PLAN_SEND = "plan_send"
    PLACE_ORDER = "place_order"
    ACK_OK = "ack_ok"
    ACK_REJECT = "ack_reject"
    TIMEOUT_ACK = "timeout_ack"
    PARTIAL_FILL = "partial_fill"
    FULL_FILL = "full_fill"
    TIMEOUT_WORKING = "timeout_working"
    RISK_TRIP = "risk_trip"
    MANUAL_CANCEL = "manual_cancel"
    BROKER_DOWN = "broker_down"
    CANCEL_SENT = "cancel_sent"
    POSITIONS_RESYNCED = "positions_resynced"
    CANNOT_RECOVER = "cannot_recover"
    TRY_RESYNC = "try_resync"


@dataclass
class TargetPositionEvent:
    """Emitted by strategy; consumed by HedgeFSM."""

    target_shares: int
    reason: str = ""
    ts: float = 0.0
    trace_id: Optional[str] = None
    side: str = ""  # BUY / SELL derived
    quantity: int = 0  # abs(need)


@dataclass
class TickEvent:
    """Normalized tick from market data."""

    ts: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    last: Optional[float] = None
    symbol: str = ""


@dataclass
class QuoteEvent:
    """Bid/ask quote update."""

    ts: float
    bid: float
    ask: float
    symbol: str = ""


@dataclass
class PositionEvent:
    """Position update."""

    ts: float
    stock_shares: int
    option_delta: Optional[float] = None
    positions: Any = None


@dataclass
class FillEvent:
    """Fill / partial fill from broker."""

    ts: float
    side: str
    quantity: int
    price: Optional[float] = None
    cumulative: int = 0
    order_id: Optional[str] = None


@dataclass
class AckEvent:
    """Order ack from broker."""

    ts: float
    order_id: Optional[str] = None
    ok: bool = True
    reject_reason: Optional[str] = None
