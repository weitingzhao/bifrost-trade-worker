"""Option bar period label helpers (kept for API mapping; no ingest)."""

from __future__ import annotations

from typing import Tuple

OPTION_MIN_INTRADAY_PERIODS: Tuple[str, ...] = ("1 min", "5 mins", "1 hour")


def timespan_to_stock_period(timespan: str, multiplier: int = 1) -> str:
    """Map Massive timespan + multiplier to a bar period label."""
    ts = (timespan or "minute").strip().lower()
    m = max(1, int(multiplier or 1))
    if ts == "minute":
        if m == 5:
            return "5 mins"
        return f"{m} min" if m > 1 else "1 min"
    if ts == "hour":
        return f"{m} hour" if m > 1 else "1 hour"
    if ts == "second":
        return f"{m} sec" if m > 1 else "1 sec"
    if ts == "day":
        return f"{m} D" if m > 1 else "1 D"
    if ts == "week":
        return f"{m} W" if m > 1 else "1 W"
    if ts == "month":
        return f"{m} M" if m > 1 else "1 M"
    return f"{m} {ts}"


def period_label_to_aggs_timespan_multiplier(period_label: str) -> Tuple[str, int]:
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
    ts, mult = period_label_to_aggs_timespan_multiplier(period_label)
    return timespan_to_stock_period(ts, mult)


def lookback_ms_for_option_min(lookback_days: int) -> int:
    d = max(1, min(int(lookback_days), 366))
    return d * 24 * 60 * 60 * 1000
