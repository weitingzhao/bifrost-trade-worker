"""Massive / Polygon client compatibility shim (P9 S3).

Live HTTP ingest moved to bifrost-platform-plugin-market-data.
Keep contract-key helpers and a stub ``MassiveClient`` so API imports resolve.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

DEFAULT_REST_BASE = "https://api.polygon.io"

_RETIRED_MSG = "Massive client retired; use market-data plugin"


def _as_error_str(err: Any) -> str:
    """Polygon/Massive sometimes returns error as a string, object, or list."""
    if isinstance(err, str):
        return err
    if err is None:
        return "Unknown error"
    try:
        return json.dumps(err, default=str)
    except (TypeError, ValueError):
        return str(err)


def _norm_expiry(s: str) -> str:
    """Normalize expiration to YYYYMMDD or YYYYMM as stored elsewhere."""
    s = (s or "").strip()
    if len(s) >= 10 and s[4] == "-":
        return s[:4] + s[5:7] + s[8:10]
    return s


def _right_from_contract_type(ct: str) -> str:
    u = (ct or "").upper()
    if u in ("CALL", "C"):
        return "C"
    if u in ("PUT", "P"):
        return "P"
    return "C"


def contract_key_from_parts(
    symbol: str, expiry: str, strike: float, option_right: str
) -> str:
    """Match account_positions / DATABASE.md: symbol|OPT|expiry|strike|right."""
    sym = (symbol or "").strip().upper()
    exp = _norm_expiry(expiry)
    r = (option_right or "").strip().upper()
    if r in ("CALL",):
        r = "C"
    if r in ("PUT",):
        r = "P"
    sk = round(float(strike), 8)
    return f"{sym}|OPT|{exp}|{sk}|{r}"


def contract_key_from_reference_result(
    underlying: str, row: Dict[str, Any]
) -> Optional[str]:
    """Build ``option_contracts.contract_key`` from a Polygon reference result row."""
    u = (underlying or "").strip().upper()
    if not u or not isinstance(row, dict):
        return None
    exp = row.get("expiration_date") or row.get("expiration") or ""
    if not exp:
        return None
    ed = _norm_expiry(str(exp)[:10])
    if len(ed) != 8 or not ed.isdigit():
        return None
    sp = row.get("strike_price")
    if sp is None:
        return None
    try:
        strike = float(sp)
    except (TypeError, ValueError):
        return None
    ort = _right_from_contract_type(str(row.get("contract_type") or "call"))
    return contract_key_from_parts(u, ed, strike, ort)


class MassiveClient:
    """Retired HTTP client stub — methods refuse live Polygon calls."""

    def __init__(self, api_key: str, rest_base: str = DEFAULT_REST_BASE) -> None:
        self._api_key = (api_key or "").strip()
        self._base = (rest_base or DEFAULT_REST_BASE).rstrip("/")

    @property
    def configured(self) -> bool:
        return bool(self._api_key)

    def _retired(self, *_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        return {"error": _RETIRED_MSG, "ok": False, "reason": "massive_retired"}

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        def _method(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
            return self._retired()

        return _method
