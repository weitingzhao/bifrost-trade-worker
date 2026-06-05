"""Compare PostgreSQL option_contracts rows to Massive reference API fields (L2 column parity)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bifrost_worker.data.massive.vendor.client import MassiveClient
from bifrost_worker.data.massive.vendor.reader import _norm_expiry_db, _right_from_ref_contract_type


def _strike_close(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-5 * max(1.0, abs(a), abs(b))


def _expected_from_reference_row(
    sym: str, api_row: Dict[str, Any]
) -> Optional[Tuple[str, float, str, Optional[str]]]:
    """Return (expiry_norm, strike, option_right, api_ticker) expected in DB from reference row."""
    exp = api_row.get("expiration_date") or api_row.get("expiration") or ""
    if not exp:
        return None
    ed = _norm_expiry_db(str(exp)[:10])
    if len(ed) != 8 or not ed.isdigit():
        return None
    sp = api_row.get("strike_price")
    if sp is None:
        return None
    try:
        strike = float(sp)
    except (TypeError, ValueError):
        return None
    ort = _right_from_ref_contract_type(str(api_row.get("contract_type") or "call"))
    ticker = (api_row.get("ticker") or "").strip() or None
    return ed, strike, ort, ticker


def _ticker_ok(api_ticker: Optional[str], pg_ticker: Optional[str]) -> bool:
    a = (api_ticker or "").strip()
    if not a:
        return True
    p = (pg_ticker or "").strip()
    return a == p


def compute_option_contracts_reference_column_parity(
    cur: Any,
    client: MassiveClient,
    symbol: str,
    *,
    max_expiries: int = 60,
    max_pages_per_expiry: int = 20,
    sample_limit: int = 25,
) -> Dict[str, Any]:
    """For each expiry (newest N in PG), walk reference API rows and compare ref-owned columns to PG.

    Comparable columns match :func:`upsert_option_contracts_from_reference_rows`: symbol, expiry, strike,
    option_right, massive_option_ticker. Does not validate exercise_style / shares_per_contract (snapshot path).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    compared_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    cur.execute(
        """
        SELECT COUNT(*)::bigint FROM option_contracts WHERE UPPER(TRIM(symbol)) = %s
        """,
        (sym,),
    )
    total_row = cur.fetchone()
    db_row_count = int(total_row[0] or 0) if total_row else 0

    cur.execute(
        """
        SELECT COUNT(DISTINCT expiry)::bigint FROM option_contracts WHERE UPPER(TRIM(symbol)) = %s
        """,
        (sym,),
    )
    distinct_exp_row = cur.fetchone()
    distinct_exp_ct = int(distinct_exp_row[0] or 0) if distinct_exp_row else 0
    expiries_truncated = distinct_exp_ct > max_expiries

    if db_row_count == 0:
        return {
            "ok": True,
            "symbol": sym,
            "has_rows": False,
            "db_row_count": 0,
            "compared_at": compared_at,
            "api_rows_compared": 0,
            "pg_rows_missing": 0,
            "value_mismatch_rows": 0,
            "field_mismatches": {},
            "truncated": False,
            "expiries_truncated": False,
            "sample_mismatches": [],
            "message": "No option_contracts rows for this symbol.",
        }

    cur.execute(
        """
        SELECT expiry, COUNT(*)::bigint AS n
        FROM option_contracts
        WHERE UPPER(TRIM(symbol)) = %s
        GROUP BY expiry
        ORDER BY expiry DESC
        LIMIT %s
        """,
        (sym, max_expiries),
    )
    rows = cur.fetchall() or []
    expiries_raw = [(str(r[0]).strip(), int(r[1] or 0)) for r in rows if r and r[0] is not None]

    field_mismatches: Dict[str, int] = {
        "symbol": 0,
        "expiry": 0,
        "strike": 0,
        "option_right": 0,
        "massive_option_ticker": 0,
    }
    pg_rows_missing = 0
    value_mismatch_rows = 0
    api_rows_compared = 0
    any_truncated = False
    sample_mismatches: List[Dict[str, Any]] = []

    def _bump_sample(kind: str, contract_key: str, detail: str, fields: List[str]) -> None:
        if len(sample_mismatches) >= sample_limit:
            return
        sample_mismatches.append(
            {"kind": kind, "contract_key": contract_key, "detail": detail, "fields": fields}
        )

    for exp_key, _pg_all in expiries_raw:
        ck_result = client.collect_option_contract_reference_rows_paginated(
            sym,
            expiration_date=exp_key,
            max_pages=max_pages_per_expiry,
            limit=250,
        )
        if ck_result.get("error"):
            return {
                "ok": False,
                "symbol": sym,
                "error": str(ck_result.get("error")),
                "compared_at": compared_at,
            }
        if ck_result.get("truncated"):
            any_truncated = True
        ref_rows = ck_result.get("rows") or []
        for item in ref_rows:
            if not isinstance(item, dict):
                continue
            contract_key = item.get("contract_key")
            api_row = item.get("result")
            if not contract_key or not isinstance(api_row, dict):
                continue
            exp = _expected_from_reference_row(sym, api_row)
            if not exp:
                continue
            ed, strike, ort, api_ticker = exp
            api_rows_compared += 1

            cur.execute(
                """
                SELECT symbol, expiry, strike, option_right, massive_option_ticker
                FROM option_contracts
                WHERE contract_key = %s
                """,
                (contract_key,),
            )
            pg = cur.fetchone()
            if not pg:
                pg_rows_missing += 1
                _bump_sample("missing_pg_row", contract_key, "No PG row for API contract_key", [])
                continue

            pg_symbol = str(pg[0] or "").strip().upper()
            pg_exp = str(pg[1] or "").strip()
            pg_strike = float(pg[2])
            pg_right = str(pg[3] or "").strip().upper()
            if pg_right in ("CALL",):
                pg_right = "C"
            if pg_right in ("PUT",):
                pg_right = "P"
            pg_ticker = pg[4]

            row_bad = False
            bad_fields: List[str] = []
            if pg_symbol != sym:
                field_mismatches["symbol"] += 1
                bad_fields.append("symbol")
                row_bad = True
            if pg_exp != ed:
                field_mismatches["expiry"] += 1
                bad_fields.append("expiry")
                row_bad = True
            if not _strike_close(pg_strike, strike):
                field_mismatches["strike"] += 1
                bad_fields.append("strike")
                row_bad = True
            if pg_right != ort:
                field_mismatches["option_right"] += 1
                bad_fields.append("option_right")
                row_bad = True
            if not _ticker_ok(api_ticker, pg_ticker if pg_ticker is None else str(pg_ticker)):
                field_mismatches["massive_option_ticker"] += 1
                bad_fields.append("massive_option_ticker")
                row_bad = True
            if row_bad:
                value_mismatch_rows += 1
                _bump_sample(
                    "value_mismatch",
                    contract_key,
                    "PG row differs from reference API fields",
                    bad_fields,
                )

    return {
        "ok": True,
        "symbol": sym,
        "has_rows": True,
        "db_row_count": db_row_count,
        "compared_at": compared_at,
        "api_rows_compared": api_rows_compared,
        "pg_rows_missing": pg_rows_missing,
        "value_mismatch_rows": value_mismatch_rows,
        "field_mismatches": field_mismatches,
        "truncated": any_truncated,
        "expiries_truncated": expiries_truncated,
        "sample_mismatches": sample_mismatches,
    }
