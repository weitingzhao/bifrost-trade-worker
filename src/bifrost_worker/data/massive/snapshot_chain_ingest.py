"""Shared ingest: one chain-style snapshot item -> option_contracts + option_snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bifrost_worker.data.massive.vendor.client import contract_key_from_parts


def _f_or_none(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _norm_expiry(s: str) -> str:
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


def _ns_to_datetime(ns: Any) -> Optional[datetime]:
    if ns is None:
        return None
    try:
        n = int(ns)
        if n > 1_000_000_000_000_000_000:
            return datetime.fromtimestamp(n / 1e9, tz=timezone.utc)
        if n > 1_000_000_000_000:
            return datetime.fromtimestamp(n / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(float(n), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _parse_snapshot_ts(item: Dict[str, Any]) -> datetime:
    lt = item.get("last_trade") or {}
    lq = item.get("last_quote") or {}
    day = item.get("day") if isinstance(item.get("day"), dict) else {}
    for ns in (
        lt.get("sip_timestamp"),
        lt.get("participant_timestamp"),
        lq.get("last_updated"),
        day.get("last_updated"),
    ):
        dt = _ns_to_datetime(ns)
        if dt is not None:
            return dt
    return datetime.now(timezone.utc)


def contract_snapshot_api_response_to_chain_item(api: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize GET /v3/snapshot/options/{u}/{contract} response to the same shape as one chain ``results[]`` item."""
    if not isinstance(api, dict):
        return None
    err = api.get("error")
    if err:
        return None
    r = api.get("results")
    if isinstance(r, dict) and (r.get("details") is not None or r.get("ticker")):
        return r
    if api.get("details") is not None or api.get("ticker"):
        return api
    return None


def apply_chain_snapshot_item(cur: Any, underlying: str, item: Dict[str, Any]) -> bool:
    """Upsert one snapshot result item (same schema as chain ``results[]``). Returns True if a row was written."""
    if not isinstance(item, dict):
        return False
    underlying_u = (underlying or "").strip().upper()
    if not underlying_u:
        return False

    det = item.get("details") or {}
    if not isinstance(det, dict):
        det = {}
    ticker = (det.get("ticker") or item.get("ticker") or "").strip()
    if not ticker:
        return False
    exp_raw = det.get("expiration_date") or det.get("expiration")
    if not exp_raw:
        return False
    exp = _norm_expiry(str(exp_raw)[:10])
    try:
        strike = float(det.get("strike_price"))
    except (TypeError, ValueError):
        return False
    ort = _right_from_contract_type(str(det.get("contract_type", "call")))
    ck = contract_key_from_parts(underlying_u, exp, strike, ort)
    g = item.get("greeks") if isinstance(item.get("greeks"), dict) else {}
    iv = g.get("iv")
    if iv is None:
        iv = item.get("implied_volatility")

    day = item.get("day") if isinstance(item.get("day"), dict) else {}

    oi = item.get("open_interest")
    if oi is not None:
        try:
            oi = int(oi)
        except (TypeError, ValueError):
            oi = None
    ua = item.get("underlying_asset") if isinstance(item.get("underlying_asset"), dict) else {}
    underlying_ticker = (ua.get("ticker") or "").strip() or None
    ts = _parse_snapshot_ts(item)

    ex_style = (det.get("exercise_style") or "").strip() or None
    spc = det.get("shares_per_contract")
    shares_per_contract: Optional[int] = None
    if spc is not None:
        try:
            shares_per_contract = int(spc)
        except (TypeError, ValueError):
            shares_per_contract = None

    day_ou = _f_or_none(day.get("open"))
    day_hi = _f_or_none(day.get("high"))
    day_lo = _f_or_none(day.get("low"))
    day_close = _f_or_none(day.get("close"))
    day_pc = _f_or_none(day.get("previous_close"))
    day_ch = _f_or_none(day.get("change"))
    day_chp = _f_or_none(day.get("change_percent"))
    day_vol: Optional[int] = None
    if day.get("volume") is not None:
        try:
            day_vol = int(day.get("volume"))
        except (TypeError, ValueError):
            day_vol = None
    day_vw = _f_or_none(day.get("vwap"))
    day_lu = _ns_to_datetime(day.get("last_updated"))

    cur.execute(
        """
        INSERT INTO option_contracts (
          contract_key, symbol, expiry, strike, option_right, massive_option_ticker,
          exercise_style, shares_per_contract, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (contract_key) DO UPDATE SET
          massive_option_ticker = COALESCE(EXCLUDED.massive_option_ticker, option_contracts.massive_option_ticker),
          exercise_style = COALESCE(EXCLUDED.exercise_style, option_contracts.exercise_style),
          shares_per_contract = COALESCE(EXCLUDED.shares_per_contract, option_contracts.shares_per_contract)
        """,
        (ck, underlying_u, exp, strike, ort, ticker, ex_style, shares_per_contract),
    )
    cur.execute(
        """
        INSERT INTO option_snapshots (
          contract_key, snapshot_ts,
          iv, delta, gamma, theta, vega, open_interest,
          underlying_ticker,
          day_open, day_high, day_low, day_close,
          day_previous_close, day_change, day_change_percent,
          day_volume, day_vwap, day_last_updated,
          source, created_at
        )
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          'massive', now()
        )
        ON CONFLICT (contract_key, snapshot_ts) DO UPDATE SET
          iv = EXCLUDED.iv,
          delta = EXCLUDED.delta,
          gamma = EXCLUDED.gamma,
          theta = EXCLUDED.theta,
          vega = EXCLUDED.vega,
          open_interest = EXCLUDED.open_interest,
          underlying_ticker = EXCLUDED.underlying_ticker,
          day_open = EXCLUDED.day_open,
          day_high = EXCLUDED.day_high,
          day_low = EXCLUDED.day_low,
          day_close = EXCLUDED.day_close,
          day_previous_close = EXCLUDED.day_previous_close,
          day_change = EXCLUDED.day_change,
          day_change_percent = EXCLUDED.day_change_percent,
          day_volume = EXCLUDED.day_volume,
          day_vwap = EXCLUDED.day_vwap,
          day_last_updated = EXCLUDED.day_last_updated,
          source = EXCLUDED.source,
          created_at = EXCLUDED.created_at
        """,
        (
            ck,
            ts,
            _f_or_none(iv),
            _f_or_none(g.get("delta")),
            _f_or_none(g.get("gamma")),
            _f_or_none(g.get("theta")),
            _f_or_none(g.get("vega")),
            oi,
            underlying_ticker,
            day_ou,
            day_hi,
            day_lo,
            day_close,
            day_pc,
            day_ch,
            day_chp,
            day_vol,
            day_vw,
            day_lu,
        ),
    )
    return True
