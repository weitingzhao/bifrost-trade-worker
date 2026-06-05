"""Compare PostgreSQL option_snapshots coverage to Massive GET /v3/snapshot/options/{underlying} (per expiry)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from bifrost_worker.data.massive.vendor.client import (
    MassiveClient,
    _norm_expiry,
    _right_from_contract_type,
    contract_key_from_parts,
)


def _contract_key_from_snapshot_item(underlying: str, item: Dict[str, Any]) -> Optional[str]:
    """Build ``contract_key`` from a ``/v3/snapshot/options/{underlying}`` result item (same as ingest)."""
    u = (underlying or "").strip().upper()
    if not u or not isinstance(item, dict):
        return None
    det = item.get("details") if isinstance(item.get("details"), dict) else {}
    exp_raw = det.get("expiration_date") or det.get("expiration")
    if not exp_raw:
        return None
    exp = _norm_expiry(str(exp_raw)[:10])
    if len(exp) != 8 or not exp.isdigit():
        return None
    try:
        strike = float(det.get("strike_price"))
    except (TypeError, ValueError):
        return None
    ort = _right_from_contract_type(str(det.get("contract_type") or "call"))
    return contract_key_from_parts(u, exp, strike, ort)


def compute_option_snapshots_contracts_gap(
    cur: Any,
    client: MassiveClient,
    symbol: str,
    *,
    max_expiries: int = 60,
    max_pages_per_expiry: int = 20,
) -> Dict[str, Any]:
    """For each distinct expiry in option_contracts, paginate Massive GET /v3/snapshot/options/{underlying}.

    **Ref (massive_count)** per expiry: distinct ``contract_key`` values derived from API items that also exist in
    ``option_contracts`` for that expiry (intersection).

    **pg_count** per expiry: distinct ``contract_key`` in that intersection that have at least one
    ``option_snapshots`` row with ``source = 'massive'``.

    *gap* = *massive_total* − *pg_total* (sums over compared expiries).
    *coverage_pct* = 100 × pg_total / massive_total when massive_total > 0.
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
    oc_count = int(total_row[0] or 0) if total_row else 0

    cur.execute(
        """
        SELECT COUNT(DISTINCT contract_key)::bigint
        FROM option_snapshots
        WHERE source = 'massive'
          AND position('|' IN contract_key) > 0
          AND UPPER(TRIM(split_part(contract_key, '|', 1))) = %s
        """,
        (sym,),
    )
    snap_distinct = cur.fetchone()
    db_snapshot_distinct = int(snap_distinct[0] or 0) if snap_distinct else 0

    cur.execute(
        """
        SELECT COUNT(DISTINCT expiry)::bigint FROM option_contracts WHERE UPPER(TRIM(symbol)) = %s
        """,
        (sym,),
    )
    distinct_exp_row = cur.fetchone()
    distinct_exp_ct = int(distinct_exp_row[0] or 0) if distinct_exp_row else 0
    expiries_truncated = distinct_exp_ct > max_expiries

    if oc_count == 0:
        return {
            "ok": True,
            "symbol": sym,
            "has_rows": False,
            "db_row_count": db_snapshot_distinct,
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

    for exp_key, _pg_all_for_expiry in expiries_raw:
        cur.execute(
            """
            SELECT contract_key FROM option_contracts
            WHERE UPPER(TRIM(symbol)) = %s AND expiry = %s
            """,
            (sym, exp_key),
        )
        oc_rows = cur.fetchall() or []
        oc_keys: Set[str] = {str(r[0]).strip() for r in oc_rows if r and r[0]}
        if not oc_keys:
            expiries_out.append(
                {
                    "expiry": exp_key,
                    "pg_count": 0,
                    "pg_count_all": 0,
                    "massive_count": 0,
                    "gap": 0,
                    "truncated": False,
                }
            )
            continue

        snap_out = client.fetch_options_snapshot_all_pages(
            sym,
            expiration_date=exp_key,
            max_pages=max_pages_per_expiry,
            page_delay_sec=0.2,
        )
        if snap_out.get("error"):
            return {
                "ok": False,
                "symbol": sym,
                "error": str(snap_out.get("error")),
                "compared_at": compared_at,
            }
        if snap_out.get("truncated"):
            any_truncated = True

        results = snap_out.get("results") or []
        api_in_oc: Set[str] = set()
        if isinstance(results, list):
            for item in results:
                if not isinstance(item, dict):
                    continue
                ck = _contract_key_from_snapshot_item(sym, item)
                if ck and ck in oc_keys:
                    api_in_oc.add(ck)

        massive_count = len(api_in_oc)
        uniq_list = list(api_in_oc)
        if uniq_list:
            cur.execute(
                """
                SELECT COUNT(DISTINCT contract_key)::bigint
                FROM option_snapshots
                WHERE source = 'massive'
                  AND contract_key = ANY(%s)
                """,
                (uniq_list,),
            )
            row_ct = cur.fetchone()
            pg_matched = int(row_ct[0] or 0) if row_ct else 0
        else:
            pg_matched = 0

        massive_total += massive_count
        pg_total_sum += pg_matched

        expiries_out.append(
            {
                "expiry": exp_key,
                "pg_count": pg_matched,
                "pg_count_all": _pg_all_for_expiry,
                "massive_count": massive_count,
                "gap": massive_count - pg_matched,
                "truncated": bool(snap_out.get("truncated")),
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
        "db_row_count": db_snapshot_distinct,
        "pg_total": pg_total_sum,
        "massive_total": massive_total,
        "gap": gap,
        "coverage_pct": coverage_pct,
        "compared_at": compared_at,
        "expiries": expiries_out,
        "truncated": any_truncated or expiries_truncated,
        "expiries_truncated": expiries_truncated,
    }
