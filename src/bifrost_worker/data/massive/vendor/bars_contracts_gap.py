"""Compare market.option_daily / option_minute coverage to market.option_contract (local)."""

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
    """Compare option daily/minute bar coverage against ``market.option_contract``.

    API still accepts legacy ``table`` values ``option_day`` / ``option_min``.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    if table not in ("option_day", "option_min"):
        return {"ok": False, "error": f"table must be 'option_day' or 'option_min', got {table!r}"}

    bars_table = "market.option_daily" if table == "option_day" else "market.option_minute"
    compared_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    cur.execute(
        """
        SELECT COUNT(*)::bigint FROM market.option_contract
        WHERE UPPER(TRIM(underlying)) = %s
        """,
        (sym,),
    )
    row = cur.fetchone()
    oc_count = int(row[0] or 0) if row else 0

    if table == "option_min" and period:
        period_db = {
            "1 min": "1 minute",
            "1 minute": "1 minute",
            "5 mins": "5 minute",
            "5 min": "5 minute",
            "5 minute": "5 minute",
            "1 hour": "1 hour",
        }.get((period or "").strip(), (period or "").strip())
        cur.execute(
            f"""
            SELECT COUNT(DISTINCT oc.option_ticker)::bigint
            FROM {bars_table} b
            JOIN market.option_contract oc ON oc.option_ticker = b.option_ticker
            WHERE UPPER(TRIM(oc.underlying)) = %s
              AND b.period = %s
            """,
            (sym, period_db),
        )
    else:
        cur.execute(
            f"""
            SELECT COUNT(DISTINCT oc.option_ticker)::bigint
            FROM {bars_table} b
            JOIN market.option_contract oc ON oc.option_ticker = b.option_ticker
            WHERE UPPER(TRIM(oc.underlying)) = %s
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
            "message": "No market.option_contract rows for this symbol; run option-refresh first.",
        }

    cur.execute(
        """
        SELECT expiry, COUNT(*)::bigint AS n
        FROM market.option_contract
        WHERE UPPER(TRIM(underlying)) = %s
        GROUP BY expiry
        ORDER BY expiry DESC
        LIMIT %s
        """,
        (sym, max_expiries),
    )
    expiry_rows = cur.fetchall() or []

    cur.execute(
        """
        SELECT COUNT(DISTINCT expiry)::bigint FROM market.option_contract
        WHERE UPPER(TRIM(underlying)) = %s
        """,
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

        cur.execute(
            """
            SELECT CONCAT(expiry::text,'|',strike::text,'|',option_right)
            FROM market.option_contract
            WHERE UPPER(TRIM(underlying)) = %s AND expiry = %s::date
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

        if table == "option_min" and period:
            period_db = {
                "1 min": "1 minute",
                "1 minute": "1 minute",
                "5 mins": "5 minute",
                "5 min": "5 minute",
                "5 minute": "5 minute",
                "1 hour": "1 hour",
            }.get((period or "").strip(), (period or "").strip())
            cur.execute(
                f"""
                SELECT DISTINCT CONCAT(oc.expiry::text,'|',oc.strike::text,'|',oc.option_right)
                FROM {bars_table} b
                JOIN market.option_contract oc ON oc.option_ticker = b.option_ticker
                WHERE UPPER(TRIM(oc.underlying)) = %s
                  AND oc.expiry = %s::date
                  AND b.period = %s
                """,
                (sym, exp_key, period_db),
            )
        else:
            cur.execute(
                f"""
                SELECT DISTINCT CONCAT(oc.expiry::text,'|',oc.strike::text,'|',oc.option_right)
                FROM {bars_table} b
                JOIN market.option_contract oc ON oc.option_ticker = b.option_ticker
                WHERE UPPER(TRIM(oc.underlying)) = %s
                  AND oc.expiry = %s::date
                """,
                (sym, exp_key),
            )
        cov_keys = {str(r[0]).strip() for r in (cur.fetchall() or []) if r and r[0]}

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
                "real_gap": 0,
                "illiquid": 0,
            }
        )

    expiry_list = [e["expiry"] for e in expiries_out if e["gap"] > 0]
    if expiry_list:
        period_join = ""
        period_params: Dict[str, Any] = {"sym": sym, "expiries": expiry_list}
        if table == "option_min" and period:
            period_db = {
                "1 min": "1 minute",
                "1 minute": "1 minute",
                "5 mins": "5 minute",
                "5 min": "5 minute",
                "5 minute": "5 minute",
                "1 hour": "1 hour",
            }.get((period or "").strip(), (period or "").strip())
            period_join = "AND b.period = %(period)s"
            period_params["period"] = period_db
        cur.execute(
            f"""
            SELECT
                oc.expiry,
                COUNT(CASE WHEN COALESCE(sl.open_interest, 0) > 0 THEN 1 END)::int AS real_gap,
                COUNT(CASE WHEN COALESCE(sl.open_interest, 0) = 0  THEN 1 END)::int AS illiquid
            FROM market.option_contract oc
            LEFT JOIN market.v_option_chain_latest sl ON sl.option_ticker = oc.option_ticker
            LEFT JOIN (
                SELECT DISTINCT b.option_ticker
                FROM {bars_table} b
                JOIN market.option_contract oc2 ON oc2.option_ticker = b.option_ticker
                WHERE UPPER(TRIM(oc2.underlying)) = %(sym)s
                  {period_join}
            ) cov ON cov.option_ticker = oc.option_ticker
            WHERE UPPER(TRIM(oc.underlying)) = %(sym)s
              AND oc.expiry = ANY(%(expiries)s::date[])
              AND cov.option_ticker IS NULL
            GROUP BY oc.expiry
            """,
            period_params,
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
