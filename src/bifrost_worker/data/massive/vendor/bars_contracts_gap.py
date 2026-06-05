"""Compare option_day / option_min coverage to option_contracts (purely local — no external API)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def compute_option_bars_contracts_gap(
    cur: Any,
    symbol: str,
    table: str = "option_day",
    period: Optional[str] = None,
    max_expiries: int = 60,
) -> Dict[str, Any]:
    """Compare option_day / option_min bar coverage against option_contracts.

    No external API call — purely local comparison.

    Gap logic:
      ref_keys  = option_contracts distinct (expiry, strike, option_right) for this symbol
      cov_keys  = option_day/option_min rows with ≥1 bar (source='massive')
      gap       = |ref_keys| − |cov_keys ∩ ref_keys|
      coverage% = 100 × |covered| / |ref_keys|

    Returns same shape as OptionSnapshotsContractsGapResult (frontend-compatible).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    if table not in ("option_day", "option_min"):
        return {"ok": False, "error": f"table must be 'option_day' or 'option_min', got {table!r}"}

    compared_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Total rows in option_contracts for this symbol
    cur.execute(
        "SELECT COUNT(*)::bigint FROM option_contracts WHERE UPPER(TRIM(symbol)) = %s",
        (sym,),
    )
    row = cur.fetchone()
    oc_count = int(row[0] or 0) if row else 0

    # Total distinct bar contracts in target table
    if table == "option_min" and period:
        cur.execute(
            f"""
            SELECT COUNT(DISTINCT CONCAT(expiry,'|',strike::text,'|',option_right))::bigint
            FROM {table}
            WHERE source = 'massive'
              AND UPPER(TRIM(symbol)) = %s
              AND period = %s
            """,
            (sym, period),
        )
    else:
        cur.execute(
            f"""
            SELECT COUNT(DISTINCT CONCAT(expiry,'|',strike::text,'|',option_right))::bigint
            FROM {table}
            WHERE source = 'massive'
              AND UPPER(TRIM(symbol)) = %s
            """,
            (sym,),
        )
    row = cur.fetchone()
    db_bar_distinct = int(row[0] or 0) if row else 0

    if oc_count == 0:
        return {
            "ok": True,
            "symbol": sym,
            "has_rows": False,
            "db_row_count": db_bar_distinct,
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

    # Get distinct expiries from option_contracts (newest first, limit max_expiries)
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
    expiry_rows = cur.fetchall() or []

    # Check if we're truncating
    cur.execute(
        "SELECT COUNT(DISTINCT expiry)::bigint FROM option_contracts WHERE UPPER(TRIM(symbol)) = %s",
        (sym,),
    )
    row = cur.fetchone()
    total_distinct_expiries = int(row[0] or 0) if row else 0
    expiries_truncated = total_distinct_expiries > max_expiries

    expiries_out: List[Dict[str, Any]] = []
    ref_total = 0
    covered_total = 0

    for exp_key, oc_n in expiry_rows:
        exp_key = str(exp_key).strip()
        oc_n = int(oc_n or 0)

        # Reference keys from option_contracts for this expiry
        cur.execute(
            """
            SELECT CONCAT(expiry,'|',strike::text,'|',option_right)
            FROM option_contracts
            WHERE UPPER(TRIM(symbol)) = %s AND expiry = %s
            """,
            (sym, exp_key),
        )
        ref_keys = {str(r[0]).strip() for r in (cur.fetchall() or []) if r and r[0]}
        ref_count = len(ref_keys)

        if not ref_keys:
            expiries_out.append(
                {"expiry": exp_key, "pg_count": 0, "pg_count_all": oc_n, "massive_count": 0, "gap": 0}
            )
            continue

        # Covered keys: bar rows with source='massive' for this expiry
        if table == "option_min" and period:
            cur.execute(
                f"""
                SELECT DISTINCT CONCAT(expiry,'|',strike::text,'|',option_right)
                FROM {table}
                WHERE source = 'massive'
                  AND UPPER(TRIM(symbol)) = %s
                  AND expiry = %s
                  AND period = %s
                """,
                (sym, exp_key, period),
            )
        else:
            cur.execute(
                f"""
                SELECT DISTINCT CONCAT(expiry,'|',strike::text,'|',option_right)
                FROM {table}
                WHERE source = 'massive'
                  AND UPPER(TRIM(symbol)) = %s
                  AND expiry = %s
                """,
                (sym, exp_key),
            )
        cov_keys = {str(r[0]).strip() for r in (cur.fetchall() or []) if r and r[0]}

        # Coverage = intersection of covered keys with reference keys
        covered = len(cov_keys & ref_keys)
        gap = ref_count - covered

        ref_total += ref_count
        covered_total += covered

        expiries_out.append(
            {
                "expiry": exp_key,
                "pg_count": covered,
                "pg_count_all": oc_n,
                "massive_count": ref_count,
                "gap": gap,
                # real_gap / illiquid filled in bulk query below
                "real_gap": 0,
                "illiquid": 0,
            }
        )

    # ── OI classification: join missing contracts with option_snapshots_latest ─
    # real_gap  = missing contracts whose latest snapshot shows open_interest > 0
    #             (system should have bar data but doesn't → actionable gap)
    # illiquid  = missing contracts with OI = 0 or no snapshot at all
    #             (never traded / no market activity → expected absence)
    expiry_list = [e["expiry"] for e in expiries_out if e["gap"] > 0]
    if expiry_list:
        period_clause = "AND period = %(period)s" if (table == "option_min" and period) else ""
        cur.execute(
            f"""
            SELECT
                oc.expiry,
                COUNT(CASE WHEN COALESCE(sl.open_interest, 0) > 0 THEN 1 END)::int AS real_gap,
                COUNT(CASE WHEN COALESCE(sl.open_interest, 0) = 0  THEN 1 END)::int AS illiquid
            FROM option_contracts oc
            LEFT JOIN option_snapshots_latest sl USING (contract_key)
            LEFT JOIN (
                SELECT DISTINCT expiry, strike, option_right
                FROM {table}
                WHERE source = 'massive'
                  AND UPPER(TRIM(symbol)) = %(sym)s
                  {period_clause}
            ) cov
              ON  cov.expiry       = oc.expiry
              AND cov.strike       = oc.strike
              AND cov.option_right = oc.option_right
            WHERE UPPER(TRIM(oc.symbol)) = %(sym)s
              AND oc.expiry = ANY(%(expiries)s)
              AND cov.expiry IS NULL
            GROUP BY oc.expiry
            """,
            {"sym": sym, "expiries": expiry_list, "period": period},
        )
        oi_by_expiry: Dict[str, tuple] = {
            str(r[0]).strip(): (int(r[1] or 0), int(r[2] or 0))
            for r in (cur.fetchall() or [])
        }
        for entry in expiries_out:
            if entry["gap"] > 0 and entry["expiry"] in oi_by_expiry:
                real_g, illiquid_g = oi_by_expiry[entry["expiry"]]
                entry["real_gap"] = real_g
                entry["illiquid"] = illiquid_g

    # Global coverage
    global_gap = ref_total - covered_total
    coverage_pct: Optional[float]
    if ref_total > 0:
        coverage_pct = round(100.0 * covered_total / ref_total, 1)
    elif covered_total == 0:
        coverage_pct = 100.0
    else:
        coverage_pct = None

    return {
        "ok": True,
        "symbol": sym,
        "has_rows": True,
        "db_row_count": db_bar_distinct,
        "pg_total": covered_total,
        "massive_total": ref_total,
        "gap": global_gap,
        "coverage_pct": coverage_pct,
        "compared_at": compared_at,
        "expiries": expiries_out,
        "truncated": expiries_truncated,
        "expiries_truncated": expiries_truncated,
    }
