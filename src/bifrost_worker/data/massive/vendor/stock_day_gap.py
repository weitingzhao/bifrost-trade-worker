"""Compare market.stock_daily coverage against a NYSE-oriented trading-day calendar.

``ref`` is ``generate_series`` from the symbol's effective start through the cap date,
excluding weekends and full-closure rows in ``public.reference_us_holidays``
(``exchange='NYSE'`` AND ``status IS NULL OR status='closed'``). Early-close days
(``status='early-close'``) are still expected to have a daily bar.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional


def _gap_ctes_sql(ref_end_sql: str, cap_filter_sql: str) -> str:
    """Shared WITH block: sym_first, effective_start, ref (calendar), covered."""
    return f"""
        WITH sym_first AS (
          SELECT MIN(bar_date) AS first_bar
          FROM market.stock_daily
          WHERE UPPER(TRIM(symbol)) = %(symbol)s
        ),
        effective_start AS (
          SELECT GREATEST(
            CURRENT_DATE - (%(years)s || ' years')::interval,
            COALESCE((SELECT first_bar FROM sym_first),
                     CURRENT_DATE - (%(years)s || ' years')::interval)
          ) AS ts
        ),
        ref AS (
          SELECT s::date AS bar_date
          FROM generate_series(
            (SELECT (ts::date) FROM effective_start),
            {ref_end_sql},
            INTERVAL '1 day'
          ) AS s
          WHERE EXTRACT(DOW FROM s::date) NOT IN (0, 6)
            AND s::date NOT IN (
              SELECT holiday_date FROM reference_us_holidays
              WHERE exchange = 'NYSE'
                AND (status IS NULL OR status = 'closed')
            )
        ),
        covered AS (
          SELECT DISTINCT bar_date
          FROM market.stock_daily
          WHERE UPPER(TRIM(symbol)) = %(symbol)s
            AND bar_date >= (SELECT ts FROM effective_start)
            {cap_filter_sql}
        )"""


def compute_stock_day_gap(
    cur: Any,
    symbol: str,
    lookback_years: int = 10,
    cap_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Compare ``market.stock_daily`` bar coverage for *symbol* against the reference calendar.

    Returns a dict compatible with StockDayGapResult (frontend).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    compared_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    cap_filter_sql = "AND bar_date <= %(cap_date)s" if cap_date else ""
    sql_params: Dict[str, Any] = {"years": lookback_years, "symbol": sym}
    if cap_date:
        sql_params["cap_date"] = cap_date.isoformat()
        ref_end_sql = "%(cap_date)s::date"
    else:
        ref_end_sql = "CURRENT_DATE"

    ctes = _gap_ctes_sql(ref_end_sql, cap_filter_sql)

    cur.execute(
        f"""
        {ctes}
        SELECT
          (SELECT COUNT(*) FROM ref)::bigint     AS ref_total,
          (SELECT COUNT(*) FROM covered)::bigint AS covered_total
        """,
        sql_params,
    )
    row = cur.fetchone()
    ref_total = int(row[0] or 0) if row else 0
    covered_total = int(row[1] or 0) if row else 0

    has_rows = covered_total > 0

    if ref_total == 0:
        cur.execute("SELECT EXISTS(SELECT 1 FROM market.stock_daily LIMIT 1)")
        ex_row = cur.fetchone()
        db_has = bool(ex_row and ex_row[0])
        message = (
            "No market.stock_daily rows in the database yet."
            if not db_has
            else "No trading days fall in the computed window (effective start after ref end)."
        )
        return {
            "ok": True,
            "symbol": sym,
            "has_rows": has_rows,
            "ref_total": 0,
            "covered_total": covered_total,
            "gap": 0,
            "coverage_pct": 100.0 if covered_total == 0 else None,
            "missing_by_year": [],
            "compared_at": compared_at,
            "cap_date": cap_date.isoformat() if cap_date else None,
            "message": message,
        }

    gap = ref_total - covered_total
    coverage_pct: Optional[float]
    if ref_total > 0:
        coverage_pct = round(100.0 * covered_total / ref_total, 1)
    else:
        coverage_pct = 100.0

    cur.execute(
        f"""
        {ctes}
        SELECT
          EXTRACT(YEAR FROM r.bar_date)::int AS year,
          COUNT(*)::bigint                   AS count,
          MIN(r.bar_date)::text              AS first_missing,
          MAX(r.bar_date)::text              AS last_missing
        FROM ref r
        LEFT JOIN covered c USING (bar_date)
        WHERE c.bar_date IS NULL
        GROUP BY year
        ORDER BY year DESC
        """,
        sql_params,
    )
    missing_by_year: List[Dict[str, Any]] = []
    for yr_row in (cur.fetchall() or []):
        missing_by_year.append(
            {
                "year": int(yr_row[0]),
                "count": int(yr_row[1]),
                "first_missing": str(yr_row[2])[:10] if yr_row[2] else None,
                "last_missing": str(yr_row[3])[:10] if yr_row[3] else None,
            }
        )

    return {
        "ok": True,
        "symbol": sym,
        "has_rows": has_rows,
        "ref_total": ref_total,
        "covered_total": covered_total,
        "gap": gap,
        "coverage_pct": coverage_pct,
        "missing_by_year": missing_by_year,
        "compared_at": compared_at,
        "cap_date": cap_date.isoformat() if cap_date else None,
    }


def compute_stock_day_quality_detail(
    cur: Any,
    symbol: str,
    days: int = 90,
) -> Dict[str, Any]:
    """Return per-day OHLC / volume / VWAP completeness for a symbol from ``market.stock_daily``."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "symbol": "", "latest_date": None, "daily": [], "error": "symbol is required"}

    cur.execute(
        """
        SELECT
          bar_date::text                                                        AS bar_date,
          CASE WHEN open IS NOT NULL AND high IS NOT NULL
                    AND low  IS NOT NULL AND close IS NOT NULL
               THEN 100.0 ELSE 0.0 END                                         AS ohlc_pct,
          CASE WHEN volume IS NOT NULL THEN 100.0 ELSE 0.0 END                 AS volume_pct,
          CASE WHEN vwap   IS NOT NULL THEN 100.0 ELSE 0.0 END                 AS vwap_pct
        FROM market.stock_daily
        WHERE UPPER(TRIM(symbol)) = %(symbol)s
          AND bar_date >= CURRENT_DATE - (%(days)s || ' days')::interval
        ORDER BY bar_date DESC
        LIMIT %(days)s
        """,
        {"symbol": sym, "days": days},
    )
    rows = cur.fetchall() or []

    daily = []
    for r in rows:
        daily.append(
            {
                "date": str(r[0])[:10] if r[0] else None,
                "ohlc_pct": float(r[1] or 0),
                "volume_pct": float(r[2] or 0),
                "vwap_pct": float(r[3] or 0),
            }
        )
    latest = daily[0]["date"] if daily else None
    return {"ok": True, "symbol": sym, "latest_date": latest, "daily": daily}
