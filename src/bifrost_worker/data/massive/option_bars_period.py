"""Map option_min UI period labels to Massive /v2/aggs params (aligned with Option Discovery)."""

from __future__ import annotations

from typing import Tuple

from bifrost_core.persistence.postgres.stock_ohlc_massive import timespan_to_stock_period

# Intraday periods stored in option_min.period (not daily — use option_day for 1 D).
OPTION_MIN_INTRADAY_PERIODS: Tuple[str, ...] = ("1 min", "5 mins", "1 hour")


def period_label_to_aggs_timespan_multiplier(period_label: str) -> Tuple[str, int]:
    """Return (timespan, multiplier) for MassiveClient.fetch_option_aggs."""
    p = (period_label or "").strip()
    if p == "1 min":
        return "minute", 1
    if p == "5 mins":
        return "minute", 5
    if p == "1 hour":
        return "hour", 1
    raise ValueError(
        f"unsupported option_min period {period_label!r}; "
        f"expected one of {OPTION_MIN_INTRADAY_PERIODS}"
    )


def period_label_to_db_period(period_label: str) -> str:
    """Label used in option_min.period column (matches timespan_to_stock_period)."""
    ts, mult = period_label_to_aggs_timespan_multiplier(period_label)
    return timespan_to_stock_period(ts, mult)


def lookback_ms_for_option_min(lookback_days: int) -> int:
    """Convert lookback days to ms (intraday backfill window)."""
    d = max(1, min(int(lookback_days), 366))
    return d * 24 * 60 * 60 * 1000
