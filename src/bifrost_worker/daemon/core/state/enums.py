"""Six state-space enums for Gamma Scalping FSM: O, D, M, L, E, S."""

import enum


class OptionPositionState(str, enum.Enum):
    """O: Option position / gamma sign."""

    NONE = "O0"  # No option position
    LONG_GAMMA = "O1"  # Portfolio gamma > 0
    SHORT_GAMMA = "O2"  # Portfolio gamma < 0


class DeltaDeviationState(str, enum.Enum):
    """D: Net delta deviation from target."""

    IN_BAND = "D0"  # Within epsilon band
    MINOR = "D1"  # Beyond band but below hedge threshold
    HEDGE_NEEDED = "D2"  # At or above hedge threshold
    FORCE_HEDGE = "D3"  # At or above max limit, must hedge
    INVALID = "D4"  # Greeks NaN or missing


class MarketRegimeState(str, enum.Enum):
    """M: Market regime from vol/trend/gap/stale."""

    QUIET = "M0"
    NORMAL = "M1"
    TREND = "M2"
    CHOPPY_HIGHVOL = "M3"
    GAP = "M4"
    STALE = "M5"  # Data timestamp too old


class LiquidityState(str, enum.Enum):
    """L: Bid-ask spread / quote quality."""

    NORMAL = "L0"
    WIDE = "L1"
    EXTREME_WIDE = "L2"
    NO_QUOTE = "L3"


class ExecutionState(str, enum.Enum):
    """E: Order/execution layer state."""

    IDLE = "E0"
    ORDER_WORKING = "E1"
    PARTIAL_FILL = "E2"
    DISCONNECTED = "E3"
    BROKER_ERROR = "E4"


class SystemHealthState(str, enum.Enum):
    """S: System health (greeks, data lag, risk halt)."""

    OK = "S0"
    GREEKS_BAD = "S1"
    DATA_LAG = "S2"
    RISK_HALT = "S3"


class TradingState(str, enum.Enum):
    """Top-level Trading FSM macro states."""

    BOOT = "BOOT"
    SYNC = "SYNC"
    IDLE = "IDLE"
    ARMED = "ARMED"
    MONITOR = "MONITOR"
    NO_TRADE = "NO_TRADE"
    PAUSE_COST = "PAUSE_COST"
    PAUSE_LIQ = "PAUSE_LIQ"
    NEED_HEDGE = "NEED_HEDGE"
    HEDGING = "HEDGING"
    SAFE = "SAFE"


class HedgeState(str, enum.Enum):
    """Hedge FSM states (E mapping for detailed execution)."""

    EXEC_IDLE = "EXEC_IDLE"
    PLAN = "PLAN"
    SEND = "SEND"
    WAIT_ACK = "WAIT_ACK"
    WORKING = "WORKING"
    PARTIAL = "PARTIAL"
    REPRICE = "REPRICE"
    CANCEL = "CANCEL"
    RECOVER = "RECOVER"
    FILLED = "FILLED"
    FAIL = "FAIL"
