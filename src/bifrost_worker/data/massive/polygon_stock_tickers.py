"""Map display/reference symbols to Polygon/Massive v2 aggs tickers (stocks + indices)."""

from __future__ import annotations

from typing import Any, List, Optional

# Reference indices often use Yahoo-style ^GSPC in config/DB; Polygon aggs use I: prefix for indices.
# See Massive docs: indices tickers differ from IB/Yahoo.
# config.yaml `reference_indices[].polygon_ticker` overrides this table for that symbol.
_POLYGON_STOCK_AGG_ALIASES: dict[str, str] = {
    # S&P 500
    "^GSPC": "I:SPX",
    "GSPC": "I:SPX",
    "^SPX": "I:SPX",
    # Dow Jones Industrial Average
    "^DJI": "I:DJI",
    "DJI": "I:DJI",
    # Nasdaq Composite (Polygon uses I:COMP; Yahoo uses ^IXIC)
    "^IXIC": "I:COMP",
    "IXIC": "I:COMP",
    "^COMP": "I:COMP",
    # Nasdaq-100 (optional)
    "^NDX": "I:NDX",
    "NDX": "I:NDX",
    # CBOE Volatility Index (Yahoo ^VIX)
    "^VIX": "I:VIX",
    "VIX": "I:VIX",
}


def _polygon_from_reference_indices(
    symbol: str, reference_indices: Optional[List[Any]]
) -> Optional[str]:
    if not reference_indices:
        return None
    s = (symbol or "").strip().upper()
    for item in reference_indices:
        if not isinstance(item, dict):
            continue
        sym = (item.get("symbol") or "").strip().upper()
        if sym != s:
            continue
        pt = (item.get("polygon_ticker") or item.get("massive_polygon_ticker") or "").strip()
        if pt:
            return pt.upper()
    return None


def polygon_ticker_for_massive_aggs(
    symbol: str,
    reference_indices: Optional[List[Any]] = None,
) -> str:
    """Return Polygon ticker for GET /v2/aggs/ticker/...; DB storage still uses `symbol`."""
    from_cfg = _polygon_from_reference_indices(symbol, reference_indices)
    if from_cfg:
        return from_cfg
    s = (symbol or "").strip().upper()
    return _POLYGON_STOCK_AGG_ALIASES.get(s, s)
