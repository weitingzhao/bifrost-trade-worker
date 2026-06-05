"""Simple in-memory metrics for hedge count, slippage, data lag, spread, delta."""

import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)


class Metrics:
    """In-memory counters and running averages; log on update or periodically."""

    def __init__(self):
        self._lock = threading.Lock()
        self._hedge_count = 0
        self._slippage_sum = 0.0
        self._slippage_n = 0
        self._last_data_lag_ms: Optional[float] = None
        self._last_spread_bucket: Optional[str] = None
        self._last_delta_abs: Optional[float] = None
        self._current_state: Optional[str] = None
        self._last_gamma: Optional[float] = None
        self._reprice_count = 0
        self._safe_mode_count = 0

    def inc_hedge_count(self) -> int:
        with self._lock:
            self._hedge_count += 1
            return self._hedge_count

    @property
    def hedge_count(self) -> int:
        with self._lock:
            return self._hedge_count

    def record_slippage(self, slippage: float) -> None:
        with self._lock:
            self._slippage_n += 1
            self._slippage_sum += slippage

    @property
    def avg_slippage(self) -> Optional[float]:
        with self._lock:
            if self._slippage_n == 0:
                return None
            return self._slippage_sum / self._slippage_n

    def set_data_lag_ms(self, ms: Optional[float]) -> None:
        with self._lock:
            self._last_data_lag_ms = ms

    @property
    def data_lag_ms(self) -> Optional[float]:
        with self._lock:
            return self._last_data_lag_ms

    def set_spread_bucket(self, bucket: Optional[str]) -> None:
        with self._lock:
            self._last_spread_bucket = bucket

    def set_delta_abs(self, delta_abs: Optional[float]) -> None:
        with self._lock:
            self._last_delta_abs = delta_abs

    def set_current_state(self, state: Optional[str]) -> None:
        with self._lock:
            self._current_state = state

    @property
    def current_state(self) -> Optional[str]:
        with self._lock:
            return self._current_state

    def set_gamma(self, gamma: Optional[float]) -> None:
        with self._lock:
            self._last_gamma = gamma

    @property
    def gamma(self) -> Optional[float]:
        with self._lock:
            return self._last_gamma

    def inc_reprice_count(self) -> int:
        with self._lock:
            self._reprice_count += 1
            return self._reprice_count

    @property
    def reprice_count(self) -> int:
        with self._lock:
            return self._reprice_count

    def inc_safe_mode_count(self) -> int:
        with self._lock:
            self._safe_mode_count += 1
            return self._safe_mode_count

    @property
    def safe_mode_count(self) -> int:
        with self._lock:
            return self._safe_mode_count

    def log_snapshot(self) -> None:
        """Log current metrics snapshot."""
        with self._lock:
            parts = [f"hedge_count={self._hedge_count}"]
            if self._slippage_n:
                parts.append(f"avg_slippage={self._slippage_sum / self._slippage_n:.4f}")
            if self._last_data_lag_ms is not None:
                parts.append(f"data_lag_ms={self._last_data_lag_ms:.0f}")
            if self._last_spread_bucket:
                parts.append(f"spread_bucket={self._last_spread_bucket}")
            if self._last_delta_abs is not None:
                parts.append(f"delta_abs={self._last_delta_abs:.1f}")
            if self._current_state:
                parts.append(f"current_state={self._current_state}")
            if self._last_gamma is not None:
                parts.append(f"gamma={self._last_gamma:.4f}")
            parts.append(f"reprice_count={self._reprice_count}")
            parts.append(f"safe_mode_count={self._safe_mode_count}")
        logger.info("metrics " + " ".join(parts))


_global_metrics: Optional[Metrics] = None


def get_metrics() -> Metrics:
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = Metrics()
    return _global_metrics
