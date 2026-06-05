"""Celery-side orchestration: option_day pool row/column fill via Massive v2 aggs and v1 open-close."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_NY = ZoneInfo("America/New_York")

# Max explicit contracts per job (API fan-out chunks must stay at or below this).
ROW_GAP_EXPLICIT_TARGETS_MAX = 200


def option_day_row_lookback_days() -> int:
    raw = (os.environ.get("BIFROST_OPTION_DAY_ROW_LOOKBACK_DAYS") or "").strip()
    if not raw:
        return 730
    try:
        return max(30, min(3650, int(raw)))
    except ValueError:
        return 730


def _bar_time_ny_date_str(bt: Any) -> str:
    if bt is None:
        return ""
    if isinstance(bt, datetime):
        dt = bt
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        return ""
    return dt.astimezone(_NY).date().isoformat()


def list_option_day_row_gap_targets(
    cur: Any,
    sym: str,
    max_expiries: int,
    max_contracts: int,
    *,
    expiration_date: Optional[str] = None,
) -> List[Tuple[str, str, str, float, str]]:
    """Contracts in option_contracts (newest expiries) with ticker but no option_day row (source=massive).

    Returns tuples ``(massive_option_ticker, symbol, expiry, strike, option_right)``.
    """
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
                SELECT 1 FROM option_day od
                WHERE UPPER(TRIM(od.symbol)) = UPPER(TRIM(oc.symbol))
                  AND od.expiry = oc.expiry
                  AND od.strike = oc.strike
                  AND od.option_right = oc.option_right
                  AND od.source = 'massive'
              )
            ORDER BY oc.strike, oc.option_right
            LIMIT %s
            """,
            (sym, exp, max(1, int(max_contracts))),
        )
    else:
        cur.execute(
            """
            WITH expiries AS (
                SELECT DISTINCT expiry
                FROM option_contracts
                WHERE UPPER(TRIM(symbol)) = %s
                ORDER BY expiry DESC
                LIMIT %s
            )
            SELECT oc.massive_option_ticker, oc.symbol, oc.expiry, oc.strike, oc.option_right
            FROM option_contracts oc
            INNER JOIN expiries e ON oc.expiry = e.expiry
            WHERE UPPER(TRIM(oc.symbol)) = %s
              AND oc.massive_option_ticker IS NOT NULL
              AND TRIM(oc.massive_option_ticker) <> ''
              AND NOT EXISTS (
                SELECT 1 FROM option_day od
                WHERE UPPER(TRIM(od.symbol)) = UPPER(TRIM(oc.symbol))
                  AND od.expiry = oc.expiry
                  AND od.strike = oc.strike
                  AND od.option_right = oc.option_right
                  AND od.source = 'massive'
              )
            ORDER BY oc.expiry DESC, oc.strike, oc.option_right
            LIMIT %s
            """,
            (sym, max(1, int(max_expiries)), sym, max(1, int(max_contracts))),
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


# Backward compat for tests that monkeypatch the private name.
_fetch_option_day_row_gap_targets = list_option_day_row_gap_targets


def parse_row_gap_targets_from_payload(raw: Any, sym_upper: str) -> List[Tuple[str, str, str, float, str]]:
    """Parse ``payload.row_gap_targets`` into tuples; capped at :data:`ROW_GAP_EXPLICIT_TARGETS_MAX`."""
    if not isinstance(raw, list) or not raw:
        return []
    out: List[Tuple[str, str, str, float, str]] = []
    for item in raw:
        if len(out) >= ROW_GAP_EXPLICIT_TARGETS_MAX:
            break
        if isinstance(item, (list, tuple)) and len(item) >= 5:
            out.append(
                (
                    str(item[0]).strip(),
                    (str(item[1]).strip() or sym_upper).upper(),
                    str(item[2]).strip(),
                    float(item[3]),
                    str(item[4]).strip(),
                )
            )
        elif isinstance(item, dict):
            ot = str(item.get("options_ticker") or item.get("massive_option_ticker") or "").strip()
            u = str(item.get("symbol") or item.get("underlying") or sym_upper).strip().upper()
            exp = str(item.get("expiry") or "").strip()
            try:
                strike = float(item.get("strike") or 0)
            except (TypeError, ValueError):
                continue
            right = str(item.get("option_right") or item.get("right") or "C").strip()
            if ot and exp:
                out.append((ot, u, exp, strike, right))
    return out


def chunk_option_day_row_gap_targets(
    targets: List[Tuple[str, str, str, float, str]],
    chunk_size: int,
) -> List[List[Tuple[str, str, str, float, str]]]:
    """Split target list into slices of ``chunk_size`` (last slice may be smaller)."""
    if chunk_size < 1:
        return [targets] if targets else []
    return [targets[i : i + chunk_size] for i in range(0, len(targets), chunk_size)]


def row_gap_targets_to_payload_dicts(
    chunk: List[Tuple[str, str, str, float, str]],
) -> List[Dict[str, Any]]:
    """Serialize chunk for JSON payload (stable keys for dedup hash)."""
    return [
        {
            "options_ticker": ot,
            "symbol": u,
            "expiry": exp,
            "strike": strike,
            "option_right": opt_right,
        }
        for ot, u, exp, strike, opt_right in chunk
    ]


def _is_transient_aggs_error(msg: str) -> bool:
    m = (msg or "").lower()
    needles = (
        "remote end closed connection",
        "connection reset",
        "connection aborted",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
    )
    return any(n in m for n in needles)


def _is_transient_aggs_exception(ex: BaseException) -> bool:
    if isinstance(ex, (ConnectionError, TimeoutError, OSError)):
        return True
    return _is_transient_aggs_error(str(ex))


def fetch_option_aggs_with_retry(
    client: Any,
    options_ticker: str,
    multiplier: int,
    timespan: str,
    start_ms: int,
    end_ms: int,
    rest_throttle: Any,
    *,
    max_attempts: int = 3,
) -> Dict[str, Any]:
    """Call ``client.fetch_option_aggs`` with retries on transient network / HTTP-style errors."""
    last: Dict[str, Any] = {"results": [], "error": "no attempts"}
    for attempt in range(max(1, int(max_attempts))):
        try:
            aggs = client.fetch_option_aggs(options_ticker, multiplier, timespan, start_ms, end_ms)
            last = aggs if isinstance(aggs, dict) else {"results": [], "error": "invalid response"}
            err = last.get("error")
            if not err:
                return last
            err_s = str(err)
            if attempt < max_attempts - 1 and _is_transient_aggs_error(err_s):
                time.sleep(min(4.0, 0.4 * (2**attempt)))
                rest_throttle()
                continue
            return last
        except Exception as ex:  # noqa: BLE001
            last = {"results": [], "error": str(ex)}
            if attempt < max_attempts - 1 and _is_transient_aggs_exception(ex):
                time.sleep(min(4.0, 0.4 * (2**attempt)))
                rest_throttle()
                continue
            return last
    return last


def _fetch_option_day_column_targets(
    cur: Any,
    sym: str,
    lookback_days: int,
    max_rows: int,
    priority_dates: Optional[Sequence[str]],
) -> List[Tuple[Any, str, str, str, float, str, Any]]:
    """Incomplete option_day rows in lookback: option_day_id, ticker, symbol, expiry, strike, right, bar_time."""
    lookback_days = max(1, min(int(lookback_days), 366))
    max_rows = max(1, min(int(max_rows), 5000))
    pd: Optional[List[str]] = None
    if priority_dates:
        pd = [str(d).strip()[:10] for d in priority_dates if d and str(d).strip()]
        if not pd:
            pd = None

    if pd:
        cur.execute(
            """
            SELECT od.option_day_id, oc.massive_option_ticker, od.symbol, od.expiry, od.strike,
                   od.option_right, od.bar_time
            FROM option_day od
            INNER JOIN option_contracts oc
              ON UPPER(TRIM(od.symbol)) = UPPER(TRIM(oc.symbol))
             AND od.expiry = oc.expiry
             AND od.strike = oc.strike
             AND od.option_right = oc.option_right
            WHERE UPPER(TRIM(od.symbol)) = %s
              AND od.source = 'massive'
              AND od.bar_time >= NOW() - (interval '1 day' * %s)
              AND oc.massive_option_ticker IS NOT NULL
              AND TRIM(oc.massive_option_ticker) <> ''
              AND (
                  od.open IS NULL OR od.high IS NULL OR od.low IS NULL OR od.close IS NULL
                  OR od.volume IS NULL OR od.vwap IS NULL
              )
            ORDER BY
              CASE
                WHEN DATE(timezone('America/New_York', od.bar_time))::text = ANY(%s::text[])
                  THEN 0
                ELSE 1
              END,
              od.bar_time DESC
            LIMIT %s
            """,
            (sym, lookback_days, pd, max_rows),
        )
    else:
        cur.execute(
            """
            SELECT od.option_day_id, oc.massive_option_ticker, od.symbol, od.expiry, od.strike,
                   od.option_right, od.bar_time
            FROM option_day od
            INNER JOIN option_contracts oc
              ON UPPER(TRIM(od.symbol)) = UPPER(TRIM(oc.symbol))
             AND od.expiry = oc.expiry
             AND od.strike = oc.strike
             AND od.option_right = oc.option_right
            WHERE UPPER(TRIM(od.symbol)) = %s
              AND od.source = 'massive'
              AND od.bar_time >= NOW() - (interval '1 day' * %s)
              AND oc.massive_option_ticker IS NOT NULL
              AND TRIM(oc.massive_option_ticker) <> ''
              AND (
                  od.open IS NULL OR od.high IS NULL OR od.low IS NULL OR od.close IS NULL
                  OR od.volume IS NULL OR od.vwap IS NULL
              )
            ORDER BY od.bar_time DESC
            LIMIT %s
            """,
            (sym, lookback_days, max_rows),
        )
    rows = cur.fetchall() or []
    out: List[Tuple[Any, str, str, str, float, str, Any]] = []
    for r in rows:
        if not r or len(r) < 7:
            continue
        out.append(
            (
                r[0],
                str(r[1]).strip(),
                str(r[2]).strip(),
                str(r[3]).strip(),
                float(r[4]),
                str(r[5]).strip(),
                r[6],
            )
        )
    return out


def option_day_has_incomplete_rows(
    cur: Any,
    sym: str,
    lookback_days: int,
) -> bool:
    """True if any massive option_day row in the window has NULL OHLC/volume/vwap."""
    lookback_days = max(1, min(int(lookback_days), 366))
    cur.execute(
        """
        SELECT 1 FROM option_day od
        WHERE UPPER(TRIM(od.symbol)) = %s
          AND od.source = 'massive'
          AND od.bar_time >= NOW() - (interval '1 day' * %s)
          AND (
              od.open IS NULL OR od.high IS NULL OR od.low IS NULL OR od.close IS NULL
              OR od.volume IS NULL OR od.vwap IS NULL
          )
        LIMIT 1
        """,
        (sym, lookback_days),
    )
    return cur.fetchone() is not None


def run_option_day_pool_aggregates(
    conn: Any,
    client: Any,
    payload: Dict[str, Any],
    *,
    mode: str,
    apply_open_close_update: Any,
    apply_option_day_aggs: Any,
    patch_vwap: Any,
    rest_throttle: Any,
) -> Dict[str, Any]:
    """Run option_day pool fill: mode is option_day_pool_row_gap or option_day_pool_column_fill."""
    sym = (payload.get("underlying") or payload.get("symbol") or "").strip().upper()
    if not sym:
        raise ValueError("payload.underlying (or symbol) required")

    max_contracts = int(payload.get("max_contracts") or 300)
    max_contracts = max(1, min(max_contracts, 2000))
    max_expiries = int(payload.get("max_expiries") or 60)
    max_expiries = max(1, min(max_expiries, 120))

    row_lookback_days = int(payload.get("row_lookback_days") or option_day_row_lookback_days())
    row_lookback_days = max(30, min(row_lookback_days, 3650))

    column_lookback_days = int(payload.get("column_lookback_days") or 30)
    column_lookback_days = max(1, min(column_lookback_days, 366))

    max_rows = int(payload.get("max_rows") or 300)
    max_rows = max(1, min(max_rows, 5000))

    raw_pd = payload.get("priority_dates")
    priority_dates: Optional[List[str]] = None
    if isinstance(raw_pd, list) and raw_pd:
        priority_dates = [str(x).strip()[:10] for x in raw_pd if x]

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - row_lookback_days * 86400 * 1000

    errors: List[str] = []
    rows_touched = 0

    if mode == "option_day_pool_row_gap":
        exp_filter = (payload.get("expiration_date") or "").strip()[:32] or None
        contracts_processed = 0
        bars_upserted = 0
        explicit = parse_row_gap_targets_from_payload(payload.get("row_gap_targets"), sym)
        if explicit:
            targets = explicit
            targets_source = "explicit"
        else:
            with conn.cursor() as cur:
                targets = list_option_day_row_gap_targets(
                    cur, sym, max_expiries, max_contracts, expiration_date=exp_filter
                )
            targets_source = "query"

        for ot, u_sym, exp, strike, opt_right in targets:
            try:
                aggs = fetch_option_aggs_with_retry(
                    client, ot, 1, "day", start_ms, end_ms, rest_throttle, max_attempts=3
                )
                if aggs.get("error"):
                    err_s = str(aggs.get("error"))
                    errors.append(f"{ot}: {err_s}")
                    logger.warning("option_day_pool_row_gap: %s", err_s)
                    rest_throttle()
                    continue
                n = apply_option_day_aggs(conn, u_sym, exp, strike, opt_right, aggs)
                conn.commit()
                bars_upserted += n
                contracts_processed += 1
            except Exception as ex:  # noqa: BLE001
                try:
                    conn.rollback()
                except Exception:
                    pass
                errors.append(f"{ot}: {ex}")
                logger.exception("option_day_pool_row_gap failed for %s", ot)
            rest_throttle()

        contracts_failed = len(errors)
        contracts_ok = contracts_processed
        ok = len(errors) == 0
        summary = {
            "underlying": sym,
            "expiration_date": exp_filter,
            "row_lookback_days": row_lookback_days,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "max_expiries": max_expiries,
            "contracts_processed": contracts_processed,
            "contracts_ok": contracts_ok,
            "contracts_failed": contracts_failed,
            "targets_source": targets_source,
            "bars_upserted": bars_upserted,
            "targets_found": len(targets),
            "errors": errors[:20],
            "errors_truncated": len(errors) > 20,
        }
        if payload.get("fan_out_chunk_index") is not None:
            summary["fan_out_chunk_index"] = int(payload["fan_out_chunk_index"])
        if payload.get("fan_out_chunks_total") is not None:
            summary["fan_out_chunks_total"] = int(payload["fan_out_chunks_total"])
        return {
            "ok": ok,
            "kind": "feed_options_aggregate",
            "mode": mode,
            "bars_upserted": bars_upserted,
            "summary": summary,
        }

    if mode == "option_day_pool_column_fill":
        with conn.cursor() as cur:
            targets = _fetch_option_day_column_targets(
                cur, sym, column_lookback_days, max_rows, priority_dates
            )

        for row in targets:
            oid, ot, u_sym, exp, strike, opt_right, bar_time = row
            date_str = _bar_time_ny_date_str(bar_time)
            if not date_str:
                errors.append(f"option_day_id={oid}: bad bar_time")
                rest_throttle()
                continue
            try:
                data = client.fetch_option_open_close(ot, date_str)
                if data.get("error"):
                    err_s = str(data.get("error"))
                    errors.append(f"{ot} {date_str}: {err_s}")
                    rest_throttle()
                    continue
                n = apply_open_close_update(conn, u_sym, exp, strike, opt_right, bar_time, data)
                if n > 0 and patch_vwap:
                    patch_vwap(
                        conn,
                        client,
                        ot,
                        u_sym,
                        exp,
                        strike,
                        opt_right,
                        bar_time,
                        date_str,
                    )
                conn.commit()
                rows_touched += n
            except Exception as ex:  # noqa: BLE001
                try:
                    conn.rollback()
                except Exception:
                    pass
                errors.append(f"{ot} {date_str}: {ex}")
                logger.exception("option_day_pool_column_fill failed for %s", ot)
            rest_throttle()

        ok = len(errors) == 0
        summary = {
            "underlying": sym,
            "column_lookback_days": column_lookback_days,
            "max_rows": max_rows,
            "rows_updated": rows_touched,
            "targets_found": len(targets),
            "priority_dates_used": bool(priority_dates),
            "errors": errors[:20],
            "errors_truncated": len(errors) > 20,
        }
        return {
            "ok": ok,
            "kind": "feed_options_aggregate",
            "mode": mode,
            "rows_updated": rows_touched,
            "summary": summary,
        }

    raise ValueError(f"unknown option_day pool mode: {mode!r}")
