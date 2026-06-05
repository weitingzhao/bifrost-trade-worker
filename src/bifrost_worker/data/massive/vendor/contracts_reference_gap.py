"""Compare local option_contracts rows to Massive reference list API (contract_key–aligned)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bifrost_worker.data.massive.vendor.client import MassiveClient

# Tunable via API; hard caps protect Massive REST from accidental huge scans.
_DEFAULT_MAX_EXPIRIES = 60
_MAX_EXPIRIES_CAP = 120
_DEFAULT_MAX_PAGES_PER_EXPIRY = 20
_MAX_PAGES_PER_EXPIRY_CAP = 30


def _clamp_int(n: Any, lo: int, hi: int, default: int) -> int:
    try:
        v = int(n)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


def compute_option_contracts_reference_gap(
    cur: Any,
    client: MassiveClient,
    symbol: str,
    *,
    max_expiries: int = _DEFAULT_MAX_EXPIRIES,
    max_pages_per_expiry: int = _DEFAULT_MAX_PAGES_PER_EXPIRY,
) -> Dict[str, Any]:
    """For each distinct expiry in option_contracts, paginate Massive GET /v3/reference/options/contracts.

    **Comparable scope** is defined by **API result rows** for that expiry. PostgreSQL counts only rows whose
    ``contract_key`` appears in that API-derived set. Rows that exist only in the database (not returned by the
    reference list for that expiry) are **excluded** from the PG side of the comparison.

    *gap* = *massive_total* − *pg_total* where *pg_total* is the sum of matched PG rows (contract_key in API universe).
    *coverage_pct* = 100 × pg_total / massive_total when massive_total > 0 (never above 100% for this definition).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    max_expiries = _clamp_int(max_expiries, 1, _MAX_EXPIRIES_CAP, _DEFAULT_MAX_EXPIRIES)
    max_pages_per_expiry = _clamp_int(max_pages_per_expiry, 1, _MAX_PAGES_PER_EXPIRY_CAP, _DEFAULT_MAX_PAGES_PER_EXPIRY)

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
            "distinct_expiry_total": distinct_exp_ct,
            "expiries_scanned": 0,
            "max_expiries_used": max_expiries,
            "max_pages_per_expiry_used": max_pages_per_expiry,
            "pg_total": 0,
            "massive_total": None,
            "gap": None,
            "coverage_pct": None,
            "compared_at": compared_at,
            "expiries": [],
            "truncated": False,
            "expiries_truncated": False,
            "message": "No option_contracts rows for this symbol; run a chain snapshot first.",
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

    expiries_out: List[Dict[str, Any]] = []
    massive_total = 0
    pg_total_sum = 0
    any_truncated = False

    for exp_key, pg_all_for_expiry in expiries_raw:
        ck_result = client.collect_option_contract_keys_paginated(
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
        mcount = int(ck_result.get("count") or 0)
        raw_keys: List[str] = ck_result.get("keys") or []
        if ck_result.get("truncated"):
            any_truncated = True
        # Dedupe keys for SQL ANY (same contract_key may appear if API duplicates; COUNT in PG is still per row)
        uniq_keys = list(dict.fromkeys(raw_keys))

        if uniq_keys:
            cur.execute(
                """
                SELECT COUNT(*)::bigint
                FROM option_contracts
                WHERE UPPER(TRIM(symbol)) = %s
                  AND expiry = %s
                  AND contract_key = ANY(%s)
                """,
                (sym, exp_key, uniq_keys),
            )
            row_ct = cur.fetchone()
            pg_matched = int(row_ct[0] or 0) if row_ct else 0
        else:
            pg_matched = 0

        massive_total += mcount
        pg_total_sum += pg_matched
        pg_outside = max(0, pg_all_for_expiry - pg_matched)

        expiries_out.append(
            {
                "expiry": exp_key,
                "pg_count": pg_matched,
                "pg_count_all": pg_all_for_expiry,
                "pg_rows_outside_reference": pg_outside,
                "massive_count": mcount,
                "gap": mcount - pg_matched,
                "truncated": bool(ck_result.get("truncated")),
            }
        )

    gap = massive_total - pg_total_sum
    coverage_pct: Optional[float]
    if massive_total > 0:
        coverage_pct = round(100.0 * pg_total_sum / massive_total, 1)
    elif pg_total_sum == 0:
        coverage_pct = 100.0
    else:
        coverage_pct = None

    return {
        "ok": True,
        "symbol": sym,
        "has_rows": True,
        "db_row_count": db_row_count,
        "distinct_expiry_total": distinct_exp_ct,
        "expiries_scanned": len(expiries_raw),
        "max_expiries_used": max_expiries,
        "max_pages_per_expiry_used": max_pages_per_expiry,
        "pg_total": pg_total_sum,
        "massive_total": massive_total,
        "gap": gap,
        "coverage_pct": coverage_pct,
        "compared_at": compared_at,
        "expiries": expiries_out,
        "truncated": any_truncated or expiries_truncated,
        "expiries_truncated": expiries_truncated,
    }
