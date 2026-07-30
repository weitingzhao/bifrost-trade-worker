"""Polygon ticker aliases — retired stub (P9 S3)."""

from __future__ import annotations

from typing import Any


def polygon_ticker_for_massive_aggs(symbol: str, *_args: Any, **_kwargs: Any) -> str:
    return (symbol or "").strip().upper()
