"""Celery-side orchestration: option_min pool row/column fill via Massive /v2/aggs."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from bifrost_worker.data.massive.option_bars_period import (
    lookback_ms_for_option_min,
    period_label_to_aggs_timespan_multiplier,
    period_label_to_db_period,
)

logger = logging.getLogger(__name__)


def _fetch_row_gap_targets(
    cur: Any,
    sym: str,
    period_db: str,
    max_contracts: int,
    *,
    expiration_date: Optional[str] = None,
) -> List[Tuple[str, str, str, float, str]]:
    """Contracts in option_contracts with ticker but no option_min row for this period (source=massive)."""
    exp = (expiration_date or "").strip()[:32] or None
    if exp:
        cur.execute(
            """
            SELECT oc.massive_option_ticker, oc.symbol, oc.expiry, oc.strike, oc.option_right
            FROM option_contracts oc
            WHERE UPPER(TRIM(oc.symbol)) = %s
              AND oc.expiry = %s
              AND oc.massive_option_ticker IS NOT NULL
              AND TRIM(oc.massive_option_ticker) <> ''
              AND NOT EXISTS (
                SELECT 1 FROM option_min om
                WHERE UPPER(TRIM(om.symbol)) = UPPER(TRIM(oc.symbol))
                  AND om.expiry = oc.expiry
                  AND om.strike = oc.strike
                  AND om.option_right = oc.option_right
                  AND om.period = %s
                  AND om.source = 'massive'
              )
            ORDER BY oc.strike, oc.option_right
            LIMIT %s
            """,
            (sym, exp, period_db, max(1, int(max_contracts))),
        )
    else:
        cur.execute(
            """
            SELECT oc.massive_option_ticker, oc.symbol, oc.expiry, oc.strike, oc.option_right
            FROM option_contracts oc
            WHERE UPPER(TRIM(oc.symbol)) = %s
              AND oc.massive_option_ticker IS NOT NULL
              AND TRIM(oc.massive_option_ticker) <> ''
              AND NOT EXISTS (
                SELECT 1 FROM option_min om
                WHERE UPPER(TRIM(om.symbol)) = UPPER(TRIM(oc.symbol))
                  AND om.expiry = oc.expiry
                  AND om.strike = oc.strike
                  AND om.option_right = oc.option_right
                  AND om.period = %s
                  AND om.source = 'massive'
              )
            ORDER BY oc.expiry DESC, oc.strike, oc.option_right
            LIMIT %s
            """,
            (sym, period_db, max(1, int(max_contracts))),
        )
    rows = cur.fetchall() or []
    out: List[Tuple[str, str, str, float, str]] = []
    for r in rows:
        if not r or len(r) < 5:
            continue
        out.append(
            (
                str(r[0]).strip(),
                str(r[1]).strip(),
                str(r[2]).strip(),
                float(r[3]),
                str(r[4]).strip(),
            )
        )
    return out


def _fetch_column_fill_targets(
    cur: Any,
    sym: str,
    period_db: str,
    lookback_days: int,
    max_contracts: int,
) -> List[Tuple[str, str, str, float, str]]:
    """Distinct contracts that have at least one incomplete bar row in the lookback window."""
    cur.execute(
        """
        SELECT DISTINCT ON (oc.massive_option_ticker)
            oc.massive_option_ticker, oc.symbol, oc.expiry, oc.strike, oc.option_right
        FROM option_min om
        INNER JOIN option_contracts oc
          ON UPPER(TRIM(om.symbol)) = UPPER(TRIM(oc.symbol))
         AND om.expiry = oc.expiry
         AND om.strike = oc.strike
         AND om.option_right = oc.option_right
        WHERE UPPER(TRIM(om.symbol)) = %s
          AND om.period = %s
          AND om.source = 'massive'
          AND om.bar_time >= NOW() - (interval '1 day' * %s)
          AND (
              om.open IS NULL OR om.high IS NULL OR om.low IS NULL OR om.close IS NULL
              OR om.volume IS NULL OR om.vwap IS NULL
          )
          AND oc.massive_option_ticker IS NOT NULL
          AND TRIM(oc.massive_option_ticker) <> ''
        ORDER BY oc.massive_option_ticker, oc.expiry DESC
        LIMIT %s
        """,
        (sym, period_db, max(1, min(int(lookback_days), 366)), max(1, int(max_contracts))),
    )
    rows = cur.fetchall() or []
    out: List[Tuple[str, str, str, float, str]] = []
    for r in rows:
        if not r or len(r) < 5:
            continue
        out.append(
            (
                str(r[0]).strip(),
                str(r[1]).strip(),
                str(r[2]).strip(),
                float(r[3]),
                str(r[4]).strip(),
            )
        )
    return out


def run_option_min_pool_aggregates(
    conn: Any,
    client: Any,
    payload: Dict[str, Any],
    *,
    mode: str,
) -> Dict[str, Any]:
    """Run option_min pool fill: ``mode`` is option_min_pool_row_gap or option_min_pool_column_fill."""
    from bifrost_worker.data.massive.tasks import _apply_aggs, _rest_throttle

    sym = (payload.get("underlying") or payload.get("symbol") or "").strip().upper()
    if not sym:
        raise ValueError("payload.underlying (or symbol) required")

    period_label = (payload.get("period") or "").strip()
    if not period_label:
        raise ValueError("payload.period required (e.g. 5 mins)")

    if period_label.strip() == "1 D":
        raise ValueError("option_min pool fill does not support daily period; use option_day tooling")

    lookback_days = int(payload.get("lookback_days") or 7)
    lookback_days = max(1, min(lookback_days, 366))
    max_contracts = int(payload.get("max_contracts") or 300)
    max_contracts = max(1, min(max_contracts, 2000))

    ts, mult = period_label_to_aggs_timespan_multiplier(period_label)
    period_db = period_label_to_db_period(period_label)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_ms_for_option_min(lookback_days)

    errors: List[str] = []
    contracts_processed = 0
    bars_upserted = 0

    exp_filter = (payload.get("expiration_date") or "").strip()[:32] or None

    with conn.cursor() as cur:
        if mode == "option_min_pool_row_gap":
            targets = _fetch_row_gap_targets(
                cur, sym, period_db, max_contracts, expiration_date=exp_filter
            )
        elif mode == "option_min_pool_column_fill":
            targets = _fetch_column_fill_targets(cur, sym, period_db, lookback_days, max_contracts)
        else:
            raise ValueError(f"unknown option_min pool mode: {mode!r}")

    for ot, u_sym, exp, strike, opt_right in targets:
        try:
            aggs = client.fetch_option_aggs(ot, mult, ts, start_ms, end_ms)
            if aggs.get("error"):
                err_s = str(aggs.get("error"))
                errors.append(f"{ot}: {err_s}")
                logger.warning("option_min pool %s: %s", mode, err_s)
                _rest_throttle()
                continue
            n = _apply_aggs(conn, u_sym, exp, strike, opt_right, period_db, aggs)
            conn.commit()
            bars_upserted += n
            contracts_processed += 1
        except Exception as ex:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:
                pass
            errors.append(f"{ot}: {ex}")
            logger.exception("option_min pool %s failed for %s", mode, ot)
        _rest_throttle()

    ok = len(errors) == 0
    summary = {
        "underlying": sym,
        "expiration_date": exp_filter,
        "period": period_label,
        "period_db": period_db,
        "timespan": ts,
        "multiplier": mult,
        "lookback_days": lookback_days,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "contracts_processed": contracts_processed,
        "bars_upserted": bars_upserted,
        "targets_found": len(targets),
        "errors": errors[:20],
        "errors_truncated": len(errors) > 20,
    }
    return {
        "ok": ok,
        "kind": "feed_options_aggregate",
        "mode": mode,
        "bars_upserted": bars_upserted,
        "summary": summary,
    }


def option_min_has_incomplete_rows(
    cur: Any,
    sym: str,
    period_db: str,
    lookback_days: int,
) -> bool:
    """True if any massive option_min row in the window has NULL OHLC/volume/vwap."""
    cur.execute(
        """
        SELECT 1 FROM option_min om
        WHERE UPPER(TRIM(om.symbol)) = %s
          AND om.period = %s
          AND om.source = 'massive'
          AND om.bar_time >= NOW() - (interval '1 day' * %s)
          AND (
              om.open IS NULL OR om.high IS NULL OR om.low IS NULL OR om.close IS NULL
              OR om.volume IS NULL OR om.vwap IS NULL
          )
        LIMIT 1
        """,
        (sym, period_db, max(1, min(int(lookback_days), 366))),
    )
    return cur.fetchone() is not None
