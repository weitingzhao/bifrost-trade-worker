"""Compare stock_day coverage against a NYSE-oriented trading-day calendar in PostgreSQL.

``ref`` is ``generate_series`` from the symbol's effective start through the cap date,
excluding weekends and full-closure rows in ``public.reference_us_holidays``
(``exchange='NYSE'`` AND ``status IS NULL OR status='closed'``). Early-close days
(``status='early-close'``) are still expected to have a daily bar.

Holiday rows are populated by the SEPA Data Ready page Step 1 (Massive sync) and
the legacy seed ``scripts/db/reference_us_holidays_nyse_2020_2024.sql``.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional


def _gap_ctes_sql(ref_end_sql: str, cap_filter_sql: str) -> str:
    """Shared WITH block: sym_first, effective_start, ref (calendar), covered."""
    return f"""
        WITH sym_first AS (
          SELECT MIN(bar_time) AS first_bar
          FROM stock_day
          WHERE source = 'massive'
            AND UPPER(TRIM(symbol)) = %(symbol)s
        ),
        effective_start AS (
          SELECT GREATEST(
            CURRENT_DATE - (%(years)s || ' years')::interval,
            COALESCE((SELECT first_bar FROM sym_first),
                     CURRENT_DATE - (%(years)s || ' years')::interval)
          ) AS ts
        ),
        ref AS (
          SELECT s::date AS bar_time
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
          SELECT DISTINCT bar_time
          FROM stock_day
          WHERE source = 'massive'
            AND UPPER(TRIM(symbol)) = %(symbol)s
            AND bar_time >= (SELECT ts FROM effective_start)
            {cap_filter_sql}
        )"""


def compute_stock_day_gap(
    cur: Any,
    symbol: str,
    lookback_years: int = 10,
    cap_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Compare stock_day bar coverage for *symbol* against the reference trading calendar.

    Gap logic:
      ref     = each weekday from effective_start through ref_end, excluding
                ``reference_us_holidays`` (exchange='NYSE') and weekends.
      covered = DISTINCT bar_time for this symbol in the same window
      gap     = ref_total - covered_total

    cap_date should be passed as the last safely-closed trading date
    (i.e. yesterday when the NYSE session is still open, today once it has closed).
    This prevents today's bar from appearing as a phantom gap when the session
    is still in progress and the bar cannot yet be reliably fetched.

    Returns a dict compatible with StockDayGapResult (frontend).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    compared_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    cap_filter_sql = "AND bar_time::date <= %(cap_date)s" if cap_date else ""
    sql_params: Dict[str, Any] = {"years": lookback_years, "symbol": sym}
    if cap_date:
        sql_params["cap_date"] = cap_date.isoformat()
        ref_end_sql = "%(cap_date)s::date"
    else:
        ref_end_sql = "CURRENT_DATE"

    ctes = _gap_ctes_sql(ref_end_sql, cap_filter_sql)

    # ── Query A: ref total vs covered total ───────────────────────────────────
    # effective_start clips pre-IPO dates (first known bar for this symbol).
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
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM stock_day WHERE source = %s LIMIT 1)",
            ("massive",),
        )
        ex_row = cur.fetchone()
        db_has_massive = bool(ex_row and ex_row[0])
        message = (
            "No stock_day rows with source='massive' in the database yet."
            if not db_has_massive
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

    # ── Query B: missing by year (same effective_start + ref calendar) ──────
    cur.execute(
        f"""
        {ctes}
        SELECT
          EXTRACT(YEAR FROM r.bar_time)::int AS year,
          COUNT(*)::bigint                   AS count,
          MIN(r.bar_time)::text              AS first_missing,
          MAX(r.bar_time)::text              AS last_missing
        FROM ref r
        LEFT JOIN covered c USING (bar_time)
        WHERE c.bar_time IS NULL
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
    """Return per-day OHLC / volume / VWAP completeness for a symbol.

    Returns a dict compatible with StockDayQualityDetailResponse (frontend).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "symbol": "", "latest_date": None, "daily": [], "error": "symbol is required"}

    cur.execute(
        """
        SELECT
          bar_time::text                                                        AS bar_date,
          CASE WHEN open IS NOT NULL AND high IS NOT NULL
                    AND low  IS NOT NULL AND close IS NOT NULL
               THEN 100.0 ELSE 0.0 END                                         AS ohlc_pct,
          CASE WHEN volume IS NOT NULL THEN 100.0 ELSE 0.0 END                 AS volume_pct,
          CASE WHEN vwap   IS NOT NULL THEN 100.0 ELSE 0.0 END                 AS vwap_pct
        FROM stock_day
        WHERE source = 'massive'
          AND UPPER(TRIM(symbol)) = %(symbol)s
          AND bar_time >= CURRENT_DATE - (%(days)s || ' days')::interval
        ORDER BY bar_time DESC
        LIMIT %(days)s
        """,
        {"symbol": sym, "days": days},
    )
    rows = cur.fetchall() or []

    daily = [
        {
            "bar_date": str(r[0])[:10],
            "ohlc_pct": float(r[1]) if r[1] is not None else None,
            "volume_pct": float(r[2]) if r[2] is not None else None,
            "vwap_pct": float(r[3]) if r[3] is not None else None,
        }
        for r in rows
    ]
    latest_date = daily[0]["bar_date"] if daily else None

    return {
        "ok": True,
        "symbol": sym,
        "latest_date": latest_date,
        "daily": daily,
    }
