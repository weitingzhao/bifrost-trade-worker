"""Bridge IB ``contract_key`` (SYM|OPT|…) ↔ Polygon ``option_ticker`` (O:…).

Trade Discovery/Screener pass IB keys; market.* stores Polygon tickers.
"""

from __future__ import annotations

from datetime import date
from typing import List, NamedTuple, Optional, Sequence, Tuple

from bifrost_worker.data.massive.vendor.client import contract_key_from_parts


class IbContractParts(NamedTuple):
    underlying: str
    expiry: date
    strike: float
    option_right: str
    original_key: str


def is_polygon_option_ticker(ck: str) -> bool:
    return (ck or "").strip().upper().startswith("O:")


def parse_ib_contract_key(ck: str) -> Optional[IbContractParts]:
    """Parse ``SYM|OPT|expiry|strike|C`` into parts. Returns None if not IB-shaped."""
    raw = (ck or "").strip()
    if not raw or is_polygon_option_ticker(raw):
        return None
    parts = raw.split("|")
    if len(parts) < 5:
        return None
    if (parts[1] or "").strip().upper() != "OPT":
        return None
    underlying = (parts[0] or "").strip().upper()
    if not underlying:
        return None
    exp_raw = (parts[2] or "").strip()
    expiry = _expiry_to_date(exp_raw)
    if expiry is None:
        return None
    try:
        strike = round(float(parts[3]), 8)
    except (TypeError, ValueError):
        return None
    right = (parts[4] or "").strip().upper()
    if right in ("CALL",):
        right = "C"
    if right in ("PUT",):
        right = "P"
    if right not in ("C", "P"):
        return None
    return IbContractParts(underlying, expiry, strike, right, raw)


def ib_contract_key_from_parts(
    underlying: str,
    expiry: date | str,
    strike: float,
    option_right: str,
) -> str:
    """Rebuild IB ``contract_key`` from market.option_contract columns."""
    if isinstance(expiry, date):
        exp_s = expiry.strftime("%Y%m%d")
    else:
        exp_s = str(expiry)
    return contract_key_from_parts(underlying, exp_s, float(strike), option_right)


def identity_key(
    underlying: str,
    expiry: date,
    strike: float,
    option_right: str,
) -> Tuple[str, date, float, str]:
    return (
        (underlying or "").strip().upper(),
        expiry,
        round(float(strike), 8),
        (option_right or "").strip().upper()[:1],
    )


def split_contract_keys(
    keys: Sequence[str],
) -> Tuple[List[str], List[IbContractParts]]:
    """Split request keys into Polygon tickers and parsed IB parts."""
    polygon: List[str] = []
    ib_parts: List[IbContractParts] = []
    seen_poly: set[str] = set()
    seen_ib: set[str] = set()
    for raw in keys:
        k = (raw or "").strip()
        if not k:
            continue
        if is_polygon_option_ticker(k):
            # Preserve canonical O: uppercase ticker for DB match
            canon = "O:" + k.split(":", 1)[1].upper() if ":" in k else k.upper()
            if canon not in seen_poly:
                seen_poly.add(canon)
                polygon.append(canon)
            continue
        parts = parse_ib_contract_key(k)
        if parts is None:
            continue
        if parts.original_key not in seen_ib:
            seen_ib.add(parts.original_key)
            ib_parts.append(parts)
    return polygon, ib_parts


def _expiry_to_date(expiry: str) -> Optional[date]:
    e = (expiry or "").strip()
    if not e:
        return None
    if len(e) >= 10 and e[4] == "-":
        try:
            return date.fromisoformat(e[:10])
        except ValueError:
            return None
    digits = "".join(c for c in e if c.isdigit())
    if len(digits) >= 8:
        try:
            return date(int(digits[0:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None
