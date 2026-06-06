"""Celery tasks for Massive / Polygon sync (queues: see src.massive.celery_queues)."""

from __future__ import annotations

import json
import logging
import os
import time as time_module
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from bifrost_core.persistence.postgres.stock_ohlc_massive import timespan_to_stock_period
from bifrost_worker.celery.celery_app import app  # noqa: E402

logger = logging.getLogger(__name__)

# How often to persist running progress for long per-symbol ticker-reference jobs (DB + SSE poll).
_TICKER_REF_PROGRESS_EVERY = 50


def _emit_massive_job_running_progress(
    status_cfg: dict,
    job_id: int,
    *,
    kind: str,
    work_mode: str,
    total: int,
    processed: int,
    current_symbol: Optional[str],
    symbols_ok: int,
    symbols_failed: int,
    errors_sample: List[str],
) -> None:
    """Write ``result`` while status stays ``running`` so SSE clients can poll progress."""
    from bifrost_worker.data.massive.vendor.reader import update_job_massive_backfill_result

    remaining = max(0, total - processed)
    pct = round(100.0 * processed / total, 2) if total > 0 else None
    summary: Dict[str, Any] = {
        "mode": work_mode,
        "total_symbols": total,
        "processed": processed,
        "remaining": remaining,
        "symbols_upserted": symbols_ok,
        "symbols_failed": symbols_failed,
        "current_symbol": (current_symbol or "").strip() or None,
        "errors_sample": errors_sample[:20],
    }
    if pct is not None:
        summary["pct"] = pct
    body: Dict[str, Any] = {
        "ok": True,
        "kind": kind,
        "phase": "running",
        "summary": summary,
    }
    update_job_massive_backfill_result(status_cfg, job_id, "running", body)


def _should_emit_ticker_ref_progress(processed: int, total: int, *, every: int = _TICKER_REF_PROGRESS_EVERY) -> bool:
    if total <= 0:
        return True
    if processed <= 0:
        return True
    if processed >= total:
        return True
    return processed % every == 0


def _config_path_for_task() -> Optional[str]:
    for a in sys.argv[1:]:
        if a.startswith("-"):
            continue
        candidate = Path(a) if os.path.isabs(a) else _project_root / a
        if candidate.is_file() and candidate.suffix.lower() in (".yaml", ".yml"):
            return str(candidate.resolve())
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
    """Parse Massive/Polygon nanosecond or millisecond epoch to UTC."""
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


REST_GAP_SEC = 0.2


def _rest_throttle() -> None:
    time_module.sleep(REST_GAP_SEC)


def _nullable_column_backfill_gap_sec() -> float:
    """Delay between Massive GET /v3/reference/options/contracts/{{ticker}} in nullable backfill.

    Default 0.1s is ~2× faster than the global REST gap (0.2s) while staying conservative for vendor limits.
    Override with env ``BIFROST_NULLABLE_BACKFILL_GAP_SEC`` (clamped 0.02–0.5).
    """
    raw = (os.environ.get("BIFROST_NULLABLE_BACKFILL_GAP_SEC") or "").strip()
    if not raw:
        return 0.1
    try:
        return max(0.02, min(0.5, float(raw)))
    except ValueError:
        return 0.1


def _eod_trade_date_str_et(payload: Dict[str, Any]) -> str:
    """Default EOD trade_date = calendar date in America/New_York."""
    raw = (payload.get("trade_date") or "").strip()
    if raw:
        return raw[:10]
    et = ZoneInfo("America/New_York")
    return datetime.now(et).date().isoformat()


def _apply_oi_daily_from_chain(
    conn: Any,
    underlying: str,
    trade_date: date,
    snap_results: List[Dict[str, Any]],
) -> int:
    """Upsert option_contracts + option_open_interest_daily from chain snapshot items."""
    from bifrost_worker.data.massive.vendor.client import contract_key_from_parts

    underlying = (underlying or "").strip().upper()
    n = 0
    with conn.cursor() as cur:
        for item in snap_results:
            if not isinstance(item, dict):
                continue
            det = item.get("details") or {}
            ticker = (det.get("ticker") or item.get("ticker") or "").strip()
            if not ticker:
                continue
            exp_raw = det.get("expiration_date") or det.get("expiration")
            if not exp_raw:
                continue
            exp = _norm_expiry(str(exp_raw)[:10])
            try:
                strike = float(det.get("strike_price"))
            except (TypeError, ValueError):
                continue
            ort = _right_from_contract_type(det.get("contract_type", "call"))
            ck = contract_key_from_parts(underlying, exp, strike, ort)
            oi = item.get("open_interest")
            if oi is None:
                continue
            try:
                oi = int(oi)
            except (TypeError, ValueError):
                continue
            cur.execute(
                """
                INSERT INTO option_contracts (contract_key, symbol, expiry, strike, option_right, massive_option_ticker, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (contract_key) DO UPDATE SET
                  massive_option_ticker = COALESCE(EXCLUDED.massive_option_ticker, option_contracts.massive_option_ticker)
                """,
                (ck, underlying, exp, strike, ort, ticker),
            )
            cur.execute(
                """
                INSERT INTO option_open_interest_daily (
                  contract_key, symbol, expiry, strike, option_right, trade_date, open_interest, source, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'massive', now())
                ON CONFLICT (contract_key, trade_date, source)
                DO UPDATE SET open_interest = EXCLUDED.open_interest, created_at = now()
                """,
                (ck, underlying, exp, strike, ort, trade_date, oi),
            )
            n += 1
    return n


def _run_oi_watchlist_eod(
    conn: Any,
    client: Any,
    status_cfg: dict,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Fetch full chain snapshots for Watchlist STK symbols; write option_open_interest_daily."""
    from bifrost_worker.data.massive.vendor.reader import get_watchlist_optionable_stk_symbols

    td_s = _eod_trade_date_str_et(payload)
    try:
        trade_date = date.fromisoformat(td_s)
    except ValueError as e:
        raise ValueError(f"invalid trade_date: {td_s}") from e

    symbols = payload.get("symbols")
    if isinstance(symbols, list) and symbols:
        sym_list = [str(s).strip().upper() for s in symbols if s]
    else:
        sym_list = get_watchlist_optionable_stk_symbols(status_cfg)
    if not sym_list:
        logger.info("oi watchlist_eod: no symbols (empty watchlist)")
        return {"ok": True, "kind": "oi", "mode": "watchlist_eod", "rows_upserted": 0, "symbols": []}

    total_rows = 0
    per_symbol: List[Dict[str, Any]] = []
    for sym in sym_list:
        _rest_throttle()
        data = client.fetch_options_snapshot_all_pages(sym, limit=250)
        if data.get("error"):
            err = data.get("error")
            logger.warning("oi snapshot failed for %s: %s", sym, err)
            per_symbol.append({"symbol": sym, "error": str(err), "rows": 0, "pages": data.get("pages", 0)})
            continue
        results = data.get("results") or []
        if not isinstance(results, list):
            results = []
        n = _apply_oi_daily_from_chain(conn, sym, trade_date, results)
        conn.commit()
        total_rows += n
        per_symbol.append(
            {
                "symbol": sym,
                "rows": n,
                "pages": data.get("pages", 0),
                "truncated": bool(data.get("truncated")),
            }
        )
        logger.info(
            "oi watchlist_eod: %s trade_date=%s rows=%s pages=%s",
            sym, td_s, n, data.get("pages"),
        )

    return {
        "ok": True,
        "kind": "oi",
        "mode": "watchlist_eod",
        "trade_date": td_s,
        "rows_upserted": total_rows,
        "per_symbol": per_symbol,
    }


def _strike_map_for_max_pain(rows: List[Dict[str, Any]]) -> Dict[str, Dict[float, Tuple[int, int]]]:
    """Group by expiry -> strike -> (call_oi, put_oi)."""
    by_exp: Dict[str, Dict[float, Tuple[int, int]]] = {}
    for r in rows:
        exp = str(r.get("expiry") or "").strip()
        if not exp:
            continue
        try:
            sk = float(r.get("strike"))
        except (TypeError, ValueError):
            continue
        oi = int(r.get("open_interest") or 0)
        right = (r.get("option_right") or "").strip().upper()
        if exp not in by_exp:
            by_exp[exp] = {}
        d = by_exp[exp]
        c_oi, p_oi = d.get(sk, (0, 0))
        if right == "C":
            d[sk] = (c_oi + oi, p_oi)
        elif right == "P":
            d[sk] = (c_oi, p_oi + oi)
    return by_exp


def _max_pain_for_expiry(strike_oi: Dict[float, Tuple[int, int]]) -> Tuple[float, float, Dict[str, float]]:
    """Return (strike_minimizing_writer_payout, pain_value, detail_by_strike_candidate).

    Writer aggregate payout at underlying close X:
      sum_s call_oi(s)*max(0,X-s)*100 + put_oi(s)*max(0,s-X)*100
    Max pain = X that minimizes this (min payout by writers / max pain to holders).
    """
    if not strike_oi:
        return (0.0, 0.0, {})
    strikes = sorted(strike_oi.keys())
    best_x = strikes[0]
    best_pain: Optional[float] = None
    detail: Dict[str, float] = {}
    for x in strikes:
        pain = 0.0
        for s, (coi, poi) in strike_oi.items():
            pain += float(coi) * max(0.0, x - s) * 100.0
            pain += float(poi) * max(0.0, s - x) * 100.0
        detail[f"{x:g}"] = pain
        if best_pain is None or pain < best_pain:
            best_pain = pain
            best_x = x
    return (best_x, float(best_pain or 0.0), detail)


def _run_max_pain(
    conn: Any,
    status_cfg: dict,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Compute Max Pain from option_open_interest_daily for Watchlist symbols / trade_date."""
    from bifrost_worker.data.massive.vendor.reader import get_watchlist_optionable_stk_symbols

    td_s = _eod_trade_date_str_et(payload)
    try:
        trade_date = date.fromisoformat(td_s)
    except ValueError as e:
        raise ValueError(f"invalid trade_date: {td_s}") from e

    symbols = payload.get("symbols")
    if isinstance(symbols, list) and symbols:
        sym_list = [str(s).strip().upper() for s in symbols if s]
    else:
        sym_list = get_watchlist_optionable_stk_symbols(status_cfg)

    if not sym_list:
        return {"ok": True, "kind": "report_option_max_pain", "rows_upserted": 0, "trade_date": td_s, "message": "no symbols"}

    rows_written = 0
    detail_out: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        for sym in sym_list:
            cur.execute(
                """
                SELECT expiry, strike, option_right, open_interest
                FROM option_open_interest_daily
                WHERE symbol = %s AND trade_date = %s AND source = 'massive'
                """,
                (sym, trade_date),
            )
            raw = [
                {"expiry": row[0], "strike": row[1], "option_right": row[2], "open_interest": row[3]}
                for row in cur.fetchall()
            ]
            by_exp = _strike_map_for_max_pain(raw)
            for exp, skmap in by_exp.items():
                mp_strike, pain_val, comp = _max_pain_for_expiry(skmap)
                total_oi = sum(t[0] + t[1] for t in skmap.values())
                cur.execute(
                    """
                    INSERT INTO report_option_max_pain_daily (
                      symbol, expiry, trade_date, max_pain_strike, underlying_close, total_oi,
                      computation_detail, source, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'massive', now())
                    ON CONFLICT (symbol, expiry, trade_date, source)
                    DO UPDATE SET
                      max_pain_strike = EXCLUDED.max_pain_strike,
                      underlying_close = EXCLUDED.underlying_close,
                      total_oi = EXCLUDED.total_oi,
                      computation_detail = EXCLUDED.computation_detail,
                      created_at = now()
                    """,
                    (
                        sym,
                        exp,
                        trade_date,
                        mp_strike,
                        None,
                        int(total_oi) if total_oi else None,
                        json.dumps({"pain_by_strike": comp, "min_pain_value": pain_val}),
                    ),
                )
                rows_written += 1
                detail_out.append({"symbol": sym, "expiry": exp, "max_pain_strike": mp_strike, "total_oi": total_oi})
    conn.commit()
    logger.info("report_option_max_pain: trade_date=%s rows=%s", td_s, rows_written)
    return {
        "ok": True,
        "kind": "report_option_max_pain",
        "trade_date": td_s,
        "rows_upserted": rows_written,
        "expiries": detail_out[:50],
    }


def _run_reconcile(
    conn: Any,
    client: Any,
    status_cfg: dict,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Compare chain snapshot contract count vs DB OI rows for Watchlist symbols."""
    from bifrost_worker.data.massive.vendor.reader import get_watchlist_optionable_stk_symbols

    td_s = _eod_trade_date_str_et(payload)
    try:
        trade_date = date.fromisoformat(td_s)
    except ValueError as e:
        raise ValueError(f"invalid trade_date: {td_s}") from e

    threshold = float(payload.get("threshold", 0.05))
    symbols = payload.get("symbols")
    if isinstance(symbols, list) and symbols:
        sym_list = [str(s).strip().upper() for s in symbols if s]
    else:
        sym_list = get_watchlist_optionable_stk_symbols(status_cfg)

    results: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        for sym in sym_list:
            _rest_throttle()
            remote = client.fetch_options_snapshot_all_pages(sym, limit=250)
            if remote.get("error"):
                results.append(
                    {
                        "symbol": sym,
                        "status": "error",
                        "message": str(remote.get("error")),
                    }
                )
                logger.warning("reconcile: snapshot failed for %s: %s", sym, remote.get("error"))
                continue
            rem_list = remote.get("results") or []
            remote_with_oi = sum(
                1
                for it in rem_list
                if isinstance(it, dict) and it.get("open_interest") is not None
            )
            cur.execute(
                """
                SELECT count(*)::int FROM option_open_interest_daily
                WHERE symbol = %s AND trade_date = %s AND source = 'massive'
                """,
                (sym, trade_date),
            )
            row = cur.fetchone()
            local_cnt = int(row[0]) if row else 0
            denom = max(remote_with_oi, 1)
            diff_ratio = abs(remote_with_oi - local_cnt) / float(denom)
            st = "pass"
            if diff_ratio > threshold:
                st = "warn" if diff_ratio <= 0.15 else "fail"
                logger.warning(
                    "reconcile: %s trade_date=%s remote_oi_rows=%s local_rows=%s ratio_diff=%.3f status=%s",
                    sym, td_s, remote_with_oi, local_cnt, diff_ratio, st,
                )
            else:
                logger.info(
                    "reconcile: %s trade_date=%s remote=%s local=%s ok",
                    sym, td_s, remote_with_oi, local_cnt,
                )
            results.append(
                {
                    "symbol": sym,
                    "status": st,
                    "remote_contracts_with_oi": remote_with_oi,
                    "local_oi_rows": local_cnt,
                    "diff_ratio": round(diff_ratio, 4),
                }
            )
    return {"ok": True, "kind": "reconcile", "trade_date": td_s, "threshold": threshold, "results": results}


def _run_trim_jobs(conn: Any) -> Dict[str, Any]:
    """Keep the newest 500 rows in job_massive_backfill; delete older."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM job_massive_backfill
            WHERE job_massive_backfill_id NOT IN (
              SELECT job_massive_backfill_id FROM job_massive_backfill
              ORDER BY job_massive_backfill_id DESC
              LIMIT 500
            )
            """
        )
        deleted = cur.rowcount
    conn.commit()
    logger.info("trim job_massive_backfill: deleted %s rows", deleted)
    return {"ok": True, "kind": "trim_jobs", "deleted": int(deleted)}


def _refresh_snapshots_latest(conn: Any) -> None:
    """Best-effort REFRESH of option_snapshots_latest materialized view."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'option_snapshots_latest'"
            )
            if cur.fetchone():
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY option_snapshots_latest")
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def _f_or_none(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _apply_snapshot(
    conn: Any,
    underlying: str,
    snap: Dict[str, Any],
) -> int:
    """Insert option_contracts + option_snapshots rows. Returns count inserted."""
    from bifrost_worker.data.massive.snapshot_chain_ingest import apply_chain_snapshot_item

    results = snap.get("results")
    if not isinstance(results, list):
        return 0
    underlying = (underlying or "").strip().upper()
    n = 0
    with conn.cursor() as cur:
        for item in results:
            if not isinstance(item, dict):
                continue
            if apply_chain_snapshot_item(cur, underlying, item):
                n += 1
    return n


def _option_min_bar_vwap(bar: Dict[str, Any]) -> Optional[float]:
    """Prefer Massive ``vw``; if absent but volume > 0, use typical price (H+L+C)/3."""
    vw = bar.get("vw")
    if vw is not None:
        try:
            return float(vw)
        except (TypeError, ValueError):
            pass
    v = bar.get("v")
    try:
        vol = float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        vol = 0.0
    if vol <= 0:
        return None
    h, l_, c = bar.get("h"), bar.get("l"), bar.get("c")
    try:
        hf = float(h) if h is not None else None
        lf = float(l_) if l_ is not None else None
        cf = float(c) if c is not None else None
    except (TypeError, ValueError):
        return None
    if hf is not None and lf is not None and cf is not None:
        return (hf + lf + cf) / 3.0
    if cf is not None:
        return cf
    if hf is not None and lf is not None:
        return (hf + lf) / 2.0
    return None


def _apply_aggs(
    conn: Any,
    symbol: str,
    expiry: str,
    strike: float,
    option_right: str,
    period: str,
    aggs: Dict[str, Any],
) -> int:
    """Upsert option_min bars from /v2/aggs response."""
    exp = _norm_expiry(expiry)
    r = option_right.strip().upper()
    if r in ("CALL",):
        r = "C"
    if r in ("PUT",):
        r = "P"
    bars = aggs.get("results") or []
    if not isinstance(bars, list):
        return 0
    n = 0
    with conn.cursor() as cur:
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            t = bar.get("t")
            if t is None:
                continue
            try:
                ts_ms = int(t)
                bt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                continue
            o = bar.get("o")
            h = bar.get("h")
            l = bar.get("l")
            c = bar.get("c")
            v = bar.get("v")
            vw = _option_min_bar_vwap(bar)
            cur.execute(
                """
                INSERT INTO option_min (
                  symbol, expiry, strike, option_right, period, bar_time,
                  open, high, low, close, volume, vwap, source, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'massive', now())
                ON CONFLICT (symbol, expiry, strike, option_right, period, bar_time, source)
                DO UPDATE SET
                  open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                  close = EXCLUDED.close, volume = EXCLUDED.volume,
                  vwap = EXCLUDED.vwap
                """,
                (
                    symbol.upper(),
                    exp,
                    float(strike),
                    r,
                    period,
                    bt,
                    float(o) if o is not None else None,
                    float(h) if h is not None else None,
                    float(l) if l is not None else None,
                    float(c) if c is not None else None,
                    float(v) if v is not None else None,
                    float(vw) if vw is not None else None,
                ),
            )
            n += 1
    return n


def _apply_option_day_aggs(
    conn: Any,
    symbol: str,
    expiry: str,
    strike: float,
    option_right: str,
    aggs: Dict[str, Any],
) -> int:
    """Upsert option_day daily bars from /v2/aggs (timespan day) response."""
    exp = _norm_expiry(expiry)
    r = option_right.strip().upper()
    if r in ("CALL",):
        r = "C"
    if r in ("PUT",):
        r = "P"
    bars = aggs.get("results") or []
    if not isinstance(bars, list):
        return 0
    n = 0
    with conn.cursor() as cur:
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            t = bar.get("t")
            if t is None:
                continue
            try:
                ts_ms = int(t)
                bt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                continue
            o = bar.get("o")
            h = bar.get("h")
            l = bar.get("l")
            c = bar.get("c")
            v = bar.get("v")
            vw = bar.get("vw")
            cur.execute(
                """
                INSERT INTO option_day (
                  symbol, expiry, strike, option_right, bar_time,
                  open, high, low, close, volume, vwap, source, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'massive', now())
                ON CONFLICT (symbol, expiry, strike, option_right, bar_time, source)
                DO UPDATE SET
                  open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                  close = EXCLUDED.close, volume = EXCLUDED.volume,
                  vwap = EXCLUDED.vwap
                """,
                (
                    symbol.upper(),
                    exp,
                    float(strike),
                    r,
                    bt,
                    float(o) if o is not None else None,
                    float(h) if h is not None else None,
                    float(l) if l is not None else None,
                    float(c) if c is not None else None,
                    float(v) if v is not None else None,
                    float(vw) if vw is not None else None,
                ),
            )
            n += 1
    return n


def _ny_day_bounds_ms(date_str: str) -> Tuple[int, int]:
    """Start/end ms (UTC epoch) for calendar date ``date_str`` in America/New_York."""
    d = date.fromisoformat((date_str or "")[:10])
    z = ZoneInfo("America/New_York")
    start = datetime.combine(d, time.min, tzinfo=z)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _apply_option_day_open_close_update(
    conn: Any,
    symbol: str,
    expiry: str,
    strike: float,
    option_right: str,
    bar_time: Any,
    data: Dict[str, Any],
) -> int:
    """Update one option_day row from GET /v1/open-close body (top-level open/high/low/close/volume)."""
    exp = _norm_expiry(expiry)
    r = option_right.strip().upper()
    if r in ("CALL",):
        r = "C"
    if r in ("PUT",):
        r = "P"
    o = data.get("open")
    h = data.get("high")
    l = data.get("low")
    c = data.get("close")
    v = data.get("volume")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE option_day SET
              open = %s,
              high = %s,
              low = %s,
              close = %s,
              volume = %s
            WHERE UPPER(TRIM(symbol)) = %s
              AND expiry = %s
              AND strike = %s
              AND option_right = %s
              AND bar_time = %s
              AND source = 'massive'
            """,
            (
                float(o) if o is not None else None,
                float(h) if h is not None else None,
                float(l) if l is not None else None,
                float(c) if c is not None else None,
                float(v) if v is not None else None,
                symbol.upper(),
                exp,
                float(strike),
                r,
                bar_time,
            ),
        )
        return int(cur.rowcount or 0)


def _option_day_patch_vwap_from_day_aggs(
    conn: Any,
    client: Any,
    options_ticker: str,
    symbol: str,
    expiry: str,
    strike: float,
    option_right: str,
    bar_time: Any,
    date_str: str,
) -> int:
    """Set vwap on existing option_day row from /v2/aggs day bucket (single NY session day)."""
    exp = _norm_expiry(expiry)
    ort = option_right.strip().upper()
    if ort in ("CALL",):
        ort = "C"
    if ort in ("PUT",):
        ort = "P"
    start_ms, end_ms = _ny_day_bounds_ms(date_str)
    aggs = client.fetch_option_aggs(options_ticker, 1, "day", start_ms, end_ms)
    if aggs.get("error"):
        return 0
    bars = aggs.get("results") or []
    if not isinstance(bars, list) or not bars:
        return 0
    bar = bars[0] if isinstance(bars[0], dict) else {}
    vw = bar.get("vw")
    if vw is None:
        return 0
    try:
        vw_f = float(vw)
    except (TypeError, ValueError):
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE option_day SET vwap = %s
            WHERE UPPER(TRIM(symbol)) = %s
              AND expiry = %s
              AND strike = %s
              AND option_right = %s
              AND bar_time = %s
              AND source = 'massive'
            """,
            (vw_f, symbol.upper(), exp, float(strike), ort, bar_time),
        )
        return int(cur.rowcount or 0)


def _apply_feed_stocks_corporate_actions(
    conn: Any,
    client: Any,
    symbol: str,
) -> int:
    """Fetch dividends, splits, IPOs, ticker events from Massive/Polygon and upsert into massive_corporate_action."""
    total = 0
    with conn.cursor() as cur:
        divs = client.fetch_dividends(symbol)
        for d in divs.get("results") or []:
            if not isinstance(d, dict):
                continue
            ex = d.get("ex_dividend_date") or ""
            if not ex:
                continue
            desc_parts = [
                x
                for x in (
                    d.get("distribution_type"),
                    d.get("description"),
                    d.get("dividend_type"),
                )
                if x
            ]
            desc = " — ".join(str(x) for x in desc_parts) if desc_parts else None
            cur.execute(
                """
                INSERT INTO massive_corporate_action
                  (symbol, action_type, ex_date, record_date, payment_date,
                   amount, currency, description, source, created_at)
                VALUES (%s, 'dividend', %s, %s, %s, %s, %s, %s, 'massive', now())
                ON CONFLICT (symbol, action_type, ex_date, source)
                DO UPDATE SET
                  record_date   = EXCLUDED.record_date,
                  payment_date  = EXCLUDED.payment_date,
                  amount        = EXCLUDED.amount,
                  currency      = EXCLUDED.currency,
                  description   = EXCLUDED.description
                """,
                (
                    symbol,
                    ex,
                    d.get("record_date"),
                    d.get("pay_date"),
                    float(d["cash_amount"]) if d.get("cash_amount") is not None else None,
                    (d.get("currency") or "").strip() or None,
                    desc,
                ),
            )
            total += 1

        _rest_throttle()
        splits = client.fetch_splits(symbol)
        for s in splits.get("results") or []:
            if not isinstance(s, dict):
                continue
            ex = s.get("execution_date") or ""
            if not ex:
                continue
            adj = (s.get("adjustment_type") or "").strip()
            desc = (
                f"{adj}: {s.get('split_from')}:{s.get('split_to')}"
                if adj
                else f'{s.get("split_from")}:{s.get("split_to")}'
            )
            cur.execute(
                """
                INSERT INTO massive_corporate_action
                  (symbol, action_type, ex_date, ratio_from, ratio_to,
                   description, source, created_at)
                VALUES (%s, 'split', %s, %s, %s, %s, 'massive', now())
                ON CONFLICT (symbol, action_type, ex_date, source)
                DO UPDATE SET
                  ratio_from  = EXCLUDED.ratio_from,
                  ratio_to    = EXCLUDED.ratio_to,
                  description = EXCLUDED.description
                """,
                (
                    symbol,
                    ex,
                    float(s["split_from"]) if s.get("split_from") is not None else None,
                    float(s["split_to"]) if s.get("split_to") is not None else None,
                    desc,
                ),
            )
            total += 1

        _rest_throttle()
        ipos = client.fetch_ipos_for_ticker(symbol)
        if not ipos.get("error"):
            for ipo in ipos.get("results") or []:
                if not isinstance(ipo, dict):
                    continue
                tk = (ipo.get("ticker") or "").strip().upper()
                if tk != symbol:
                    continue
                listing = (ipo.get("listing_date") or "").strip()
                if not listing:
                    continue
                issuer = (ipo.get("issuer_name") or "").strip()
                st = (ipo.get("ipo_status") or "").strip()
                desc = " | ".join(
                    x for x in (issuer, st, f"listing {listing}") if x
                )
                cur.execute(
                    """
                    INSERT INTO massive_corporate_action
                      (symbol, action_type, ex_date, amount, currency, description, source, created_at)
                    VALUES (%s, 'ipo', %s, %s, %s, %s, 'massive', now())
                    ON CONFLICT (symbol, action_type, ex_date, source)
                    DO UPDATE SET
                      amount = EXCLUDED.amount,
                      currency = EXCLUDED.currency,
                      description = EXCLUDED.description
                    """,
                    (
                        symbol,
                        listing,
                        float(ipo["final_issue_price"])
                        if ipo.get("final_issue_price") is not None
                        else None,
                        (ipo.get("currency_code") or "").strip() or None,
                        desc or None,
                    ),
                )
                total += 1

        _rest_throttle()
        tev = client.fetch_ticker_events(symbol)
        if not tev.get("error"):
            res_obj = tev.get("results")
            if isinstance(res_obj, dict):
                events = res_obj.get("events") or []
            else:
                events = []
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                ed = (ev.get("date") or "").strip()
                if not ed:
                    continue
                typ = (ev.get("type") or "").strip() or "event"
                tc = ev.get("ticker_change") if isinstance(ev.get("ticker_change"), dict) else {}
                to_t = (tc.get("ticker") or "").strip()
                desc = f"{typ}: {to_t}" if to_t else typ
                cur.execute(
                    """
                    INSERT INTO massive_corporate_action
                      (symbol, action_type, ex_date, description, source, created_at)
                    VALUES (%s, 'ticker_event', %s, %s, 'massive', now())
                    ON CONFLICT (symbol, action_type, ex_date, source)
                    DO UPDATE SET
                      description = EXCLUDED.description
                    """,
                    (symbol, ed, desc),
                )
                total += 1
    return total


@app.task(bind=True, name="src.massive.tasks.run_massive_job")
def run_massive_job(self, job_id: int) -> Dict[str, Any]:
    """Execute one job_massive_backfill row."""
    from bifrost_core.config.startup import read_config
    from bifrost_worker.data.massive.vendor.client import MassiveClient
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.reader import (
        get_and_claim_massive_backfill_for_run,
        update_job_massive_backfill_result,
    )
    import psycopg2
    from bifrost_core.persistence.postgres.connection import _get_conn_params

    cfg_path = _config_path_for_task()
    config, _ = read_config(cfg_path)
    status_cfg = config
    if not status_cfg.get("postgres") and status_cfg.get("sink") != "postgres":
        return {"ok": False, "error": "postgres not configured"}

    celery_rid_raw = getattr(getattr(self, "request", None), "id", None)
    celery_rid = str(celery_rid_raw).strip() if celery_rid_raw else ""

    job, claim_outcome = get_and_claim_massive_backfill_for_run(
        status_cfg, job_id, celery_rid or None,
    )
    if not job:
        return {"ok": False, "error": "job not found"}
    from bifrost_core.persistence.postgres.ticker_reference import normalize_ticker_ref_kind

    # Coerce to plain ASCII str (DB / clients may pass non-str or odd whitespace).
    kind = str(
        normalize_ticker_ref_kind(str((job.get("kind") or "")).strip())
    ).strip().lower()

    payload = job.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    if not isinstance(payload, dict):
        payload = {}

    from bifrost_worker.data.massive.celery_queues import celery_queue_for_massive_job

    priority_high = str(payload.get("priority") or "").strip().lower() == "high"
    dispatch_qname = celery_queue_for_massive_job(kind, priority_high=priority_high)

    if claim_outcome == "skip_done":
        return {"ok": True, "skipped": "done"}

    if claim_outcome == "skip_failed":
        return {"ok": True, "skipped": "failed_stale"}

    if claim_outcome == "skip_duplicate":
        return {"ok": True, "skipped": "duplicate_dispatch"}

    ms = get_massive_settings(config)
    client = MassiveClient(ms["api_key"], ms["rest_base"])

    try:
        if not client.configured and kind not in ("trim_jobs", "report_option_max_pain"):
            update_job_massive_backfill_result(status_cfg, job_id, "failed", {"ok": False, "error": "Massive API key not configured"})
            return {"ok": False, "error": "no api key"}

        params = _get_conn_params(status_cfg)
        conn = psycopg2.connect(**params)
        try:
            # SEPA fundamentals → PostgreSQL. Route by *_fin_runners* keys — not only
            # FEED_STOCKS_FINANCIALS_KINDS — so a stale worker frozenset cannot skip
            # registered jobs (see unknown_kind: feed_stocks_balance_sheets in prod logs).
            from bifrost_worker.data.massive.celery_queues import FEED_STOCKS_FINANCIALS_KINDS
            from bifrost_api.research.sepa import financials_data as _fd_massive_fin

            _fin_runners: Dict[str, Any] = {
                "feed_stocks_income_statements": _fd_massive_fin.run_feed_stocks_income_statements_job,
                "feed_stocks_balance_sheets": _fd_massive_fin.run_feed_stocks_balance_sheets_job,
                "feed_stocks_cash_flows": _fd_massive_fin.run_feed_stocks_cash_flows_job,
                "feed_stocks_ratios": _fd_massive_fin.run_feed_stocks_ratios_job,
                "feed_stocks_short_interest": _fd_massive_fin.run_feed_stocks_short_interest_job,
                "feed_stocks_short_volume": _fd_massive_fin.run_feed_stocks_short_volume_job,
            }

            if kind in _fin_runners:
                result = _fin_runners[kind](conn, client, payload)
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "feed_option_snapshots":
                # Align with other kinds: payload.mode; snapshot_type kept as legacy alias.
                snap_mode = (payload.get("mode") or payload.get("snapshot_type") or "chain").strip().lower()

                if snap_mode == "contract":
                    from bifrost_worker.data.massive.snapshot_chain_ingest import (
                        apply_chain_snapshot_item,
                        contract_snapshot_api_response_to_chain_item,
                    )

                    u = (payload.get("underlying") or "").strip().upper()
                    oc = (payload.get("option_contract") or "").strip()
                    if not u or not oc:
                        raise ValueError("payload.underlying and payload.option_contract required for contract snapshot")
                    snap = client.fetch_option_contract_snapshot(u, oc)
                    if snap.get("error"):
                        raise RuntimeError(str(snap.get("error")))
                    res_obj = snap.get("results") if isinstance(snap.get("results"), dict) else {}
                    greeks = res_obj.get("greeks") if isinstance(res_obj.get("greeks"), dict) else {}
                    det = res_obj.get("details") if isinstance(res_obj.get("details"), dict) else {}
                    rows_written = 0
                    persist = payload.get("persist")
                    if persist is None or persist:
                        item = contract_snapshot_api_response_to_chain_item(snap)
                        if item:
                            with conn.cursor() as cur:
                                if apply_chain_snapshot_item(cur, u, item):
                                    rows_written = 1
                            conn.commit()
                            if rows_written > 0:
                                _refresh_snapshots_latest(conn)
                    result = {
                        "ok": True,
                        "kind": kind,
                        "mode": "contract",
                        "rows_written": rows_written,
                        "summary": {
                            "underlying": u,
                            "option_contract": oc,
                            "contract_type": det.get("contract_type"),
                            "expiration_date": det.get("expiration_date"),
                            "strike_price": det.get("strike_price"),
                            "break_even_price": res_obj.get("break_even_price"),
                            "implied_volatility": res_obj.get("implied_volatility"),
                            "open_interest": res_obj.get("open_interest"),
                            "has_greeks": bool(greeks),
                            "has_last_trade": isinstance(res_obj.get("last_trade"), dict),
                            "has_last_quote": isinstance(res_obj.get("last_quote"), dict),
                        },
                        "content": res_obj,
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if snap_mode == "unified":
                    tickers = (payload.get("tickers") or "").strip()
                    asset_type = (payload.get("asset_type") or "").strip() or None
                    lim = payload.get("limit")
                    sort_f = (payload.get("sort") or "").strip() or None
                    order_f = (payload.get("order") or "").strip() or None
                    snap = client.fetch_unified_snapshot(
                        tickers=tickers or None,
                        asset_type=asset_type,
                        limit=int(lim) if lim else None,
                        sort=sort_f,
                        order=order_f,
                    )
                    if snap.get("error"):
                        raise RuntimeError(str(snap.get("error")))
                    results_list = snap.get("results") or []
                    if not isinstance(results_list, list):
                        results_list = []
                    asset_types = sorted(set(
                        r.get("type", "unknown") for r in results_list if isinstance(r, dict)
                    ))
                    per_ticker_errors = [
                        {"ticker": r.get("ticker"), "error": r.get("error"), "message": r.get("message")}
                        for r in results_list if isinstance(r, dict) and r.get("error")
                    ]
                    content_items = results_list[:100]
                    result = {
                        "ok": True, "kind": kind, "mode": "unified",
                        "summary": {
                            "tickers_requested": tickers,
                            "results_count": len(results_list),
                            "asset_types": asset_types,
                            "has_next_page": bool(snap.get("next_url")),
                            "errors": per_ticker_errors,
                        },
                        "content": content_items,
                        "content_truncated": len(results_list) > 100,
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                # Default: chain snapshot (backward compatible)
                u = (payload.get("underlying") or payload.get("symbol") or "").strip().upper()
                if not u:
                    raise ValueError("payload.underlying required")
                chain_kwargs: Dict[str, Any] = {}
                for fk in ("strike_price", "strike_price_gte", "strike_price_lte"):
                    v = payload.get(fk)
                    if v is not None:
                        try:
                            chain_kwargs[fk] = float(v)
                        except (TypeError, ValueError):
                            pass
                for fk in ("expiration_date", "expiration_date_gte", "expiration_date_lte"):
                    v = payload.get(fk)
                    if v:
                        chain_kwargs[fk] = str(v)
                if payload.get("contract_type"):
                    chain_kwargs["contract_type"] = str(payload["contract_type"])
                if payload.get("limit"):
                    try:
                        chain_kwargs["limit"] = int(payload["limit"])
                    except (TypeError, ValueError):
                        pass
                if payload.get("sort"):
                    chain_kwargs["sort"] = str(payload["sort"])
                if payload.get("order"):
                    chain_kwargs["order"] = str(payload["order"])

                pg = client.fetch_options_snapshot_all_pages(u, **chain_kwargs)
                if pg.get("error"):
                    raise RuntimeError(str(pg.get("error")))
                snap = {"results": pg.get("results") or []}
                pages_fetched = int(pg.get("pages") or 0)
                count = _apply_snapshot(conn, u, snap)
                conn.commit()
                if count > 0:
                    _refresh_snapshots_latest(conn)
                    try:
                        from bifrost_core.monitor.reader.market import get_stock_day_fallback_price

                        from bifrost_api.research.iv_atm_rollup import (
                            norm_expiry_yyyymmdd,
                            rebuild_report_atm_iv_daily_for_symbol_expiry,
                        )

                        exp_roll = norm_expiry_yyyymmdd(str(payload.get("expiration_date") or ""))
                        fb = get_stock_day_fallback_price(conn, u)
                        lp = float(fb[0]) if fb and fb[0] is not None and float(fb[0]) > 0 else None
                        if exp_roll and lp:
                            rebuild_report_atm_iv_daily_for_symbol_expiry(
                                status_cfg, conn, u, exp_roll, "massive", 90, lp
                            )
                            conn.commit()
                    except Exception as roll_ex:
                        logger.debug("report_option_atm_iv_daily rollup: %s", roll_ex)
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                raw_results = snap.get("results") or []
                if not isinstance(raw_results, list):
                    raw_results = []
                content_items = raw_results[:100]
                rows_with_iv = 0
                rows_with_any_greeks = 0
                rows_with_full_greeks = 0
                merged_count = len(raw_results)
                for _item in raw_results:
                    if not isinstance(_item, dict):
                        continue
                    _g = _item.get("greeks") if isinstance(_item.get("greeks"), dict) else {}
                    _iv = _g.get("iv") if _g else None
                    if _iv is None:
                        _iv = _item.get("implied_volatility")
                    if _iv is not None:
                        rows_with_iv += 1
                    _has = [_g.get(k) is not None for k in ("delta", "gamma", "theta", "vega")] if _g else []
                    if any(_has):
                        rows_with_any_greeks += 1
                    if all(_has):
                        rows_with_full_greeks += 1
                result = {
                    "ok": True, "kind": kind, "mode": "chain",
                    "rows_written": count,
                    "massive_request_id": snap.get("request_id"),
                    "massive_status": snap.get("status"),
                    "next_url": None,
                    "summary": {
                        "underlying": u,
                        "results_count": merged_count,
                        "merged_results_count": merged_count,
                        "pages": pages_fetched,
                        "rows_written": count,
                        "has_next_page": bool(pg.get("truncated")),
                        "filters": dict(chain_kwargs) if chain_kwargs else {},
                        "rows_with_iv": rows_with_iv,
                        "rows_with_any_greeks": rows_with_any_greeks,
                        "rows_with_full_greeks": rows_with_full_greeks,
                    },
                    "content": content_items,
                    "content_truncated": len(raw_results) > 100,
                }
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "feed_stocks_aggregate":
                from bifrost_worker.data.massive.polygon_stock_tickers import polygon_ticker_for_massive_aggs
                from bifrost_core.persistence.postgres.stock_ohlc_massive import (
                    apply_stock_custom_bars,
                    apply_stock_daily_ticker_summary,
                    apply_stock_grouped_daily,
                    apply_stock_previous_day_bar,
                    get_massive_stock_day_max_date,
                )

                # Align with IB Stock Coverage periods: 1 D, 1 min, 5 mins, 1 hour (same time window).
                # Daily vs intraday use different Massive semantics; callers may sync groups separately
                # via payload.custom_bars_period_group: daily | intraday | all (omit = all).
                _CUSTOM_BARS_ALL_PERIODS: Tuple[Tuple[str, int], ...] = (
                    ("day", 1),
                    ("minute", 1),
                    ("minute", 5),
                    ("hour", 1),
                )
                _CUSTOM_BARS_DAILY_ONLY: Tuple[Tuple[str, int], ...] = (("day", 1),)
                _CUSTOM_BARS_INTRADAY_ONLY: Tuple[Tuple[str, int], ...] = (
                    ("minute", 1),
                    ("minute", 5),
                    ("hour", 1),
                )

                mode = (payload.get("mode") or "custom_bars").strip().lower()
                adj_raw = payload.get("adjusted")
                adjusted_bool: Optional[bool]
                if adj_raw is None:
                    adjusted_bool = None
                else:
                    adjusted_bool = bool(adj_raw)

                with conn.cursor() as cur:
                    if mode == "custom_bars":
                        mult = int(payload.get("multiplier") or 1)
                        ts = (payload.get("timespan") or "minute").strip()
                        start_ms = int(payload.get("start_ms") or 0)
                        end_ms = int(payload.get("end_ms") or 0)
                        sync_all_periods = bool(payload.get("sync_all_periods"))
                        period_group_raw = (
                            (payload.get("custom_bars_period_group") or "")
                            .strip()
                            .lower()
                        )
                        sync_mode_raw = (
                            (payload.get("custom_bars_sync_mode") or "window")
                            .strip()
                            .lower()
                        )
                        is_daily_smart = (
                            sync_mode_raw == "daily_smart"
                            and period_group_raw == "daily"
                            and sync_all_periods
                        )
                        if not is_daily_smart and (not start_ms or not end_ms):
                            raise ValueError("payload.start_ms and end_ms required")
                        ref_indices = status_cfg.get("reference_indices")

                        raw_syms = payload.get("symbols")
                        tickers: list[str] = []
                        if raw_syms is not None:
                            if not isinstance(raw_syms, list):
                                raise ValueError("payload.symbols must be an array")
                            for s in raw_syms:
                                u = (str(s) if s is not None else "").strip().upper()
                                if u:
                                    tickers.append(u)
                            if not tickers:
                                raise ValueError(
                                    "payload.symbols must list at least one ticker"
                                )
                        else:
                            t_one = (payload.get("ticker") or "").strip().upper()
                            if not t_one:
                                raise ValueError("payload.ticker required")
                            tickers = [t_one]

                        multi_payload = raw_syms is not None or len(tickers) > 1
                        failures: list[dict[str, str]] = []
                        per_symbol: list[dict[str, object]] = []
                        total_n = 0
                        periods_for_sync: Tuple[Tuple[str, int], ...] = _CUSTOM_BARS_ALL_PERIODS
                        if sync_all_periods and not is_daily_smart:
                            if period_group_raw in ("", "all"):
                                periods_for_sync = _CUSTOM_BARS_ALL_PERIODS
                            elif period_group_raw == "daily":
                                periods_for_sync = _CUSTOM_BARS_DAILY_ONLY
                            elif period_group_raw == "intraday":
                                periods_for_sync = _CUSTOM_BARS_INTRADAY_ONLY
                            else:
                                raise ValueError(
                                    "custom_bars_period_group must be daily, intraday, all, or omitted"
                                )
                        if is_daily_smart:
                            from bifrost_worker.data.massive.stock_ohlc_daily_smart import (
                                compute_daily_smart_range,
                            )

                            end_cap_ms: Optional[int] = None
                            er_end = payload.get("end_ms")
                            if er_end is not None and int(er_end or 0) > 0:
                                end_cap_ms = int(er_end)
                            gap_start_date_raw = (
                                str(payload.get("gap_start_date") or "").strip()[:10]
                            )

                            for t in tickers:
                                try:
                                    fetch_t = polygon_ticker_for_massive_aggs(
                                        t, ref_indices
                                    )
                                    max_d = get_massive_stock_day_max_date(cur, t)
                                    gap_start_date = None
                                    if gap_start_date_raw:
                                        try:
                                            gap_start_date = date.fromisoformat(
                                                gap_start_date_raw
                                            )
                                        except ValueError:
                                            gap_start_date = None
                                    eff_start_ms, eff_end_ms, policy, meta_ds = (
                                        compute_daily_smart_range(
                                            status_cfg,
                                            max_d,
                                            end_cap_ms,
                                            float(ms["daily_full_backfill_years"]),
                                            gap_start_date=gap_start_date,
                                        )
                                    )
                                    sym_total = 0
                                    period_rows_ds: list[dict[str, object]] = []
                                    period_errors_ds: list[dict[str, object]] = []
                                    data_ds = client.fetch_stock_aggs(
                                        fetch_t, 1, "day", eff_start_ms, eff_end_ms
                                    )
                                    if data_ds.get("error"):
                                        err_one = str(data_ds.get("error"))
                                        if not multi_payload:
                                            raise RuntimeError(err_one)
                                        failures.append({"ticker": t, "error": err_one})
                                        continue
                                    try:
                                        n_p = apply_stock_custom_bars(
                                            cur,
                                            t,
                                            "day",
                                            1,
                                            data_ds,
                                            adjusted=adjusted_bool,
                                        )
                                    except Exception as ex:
                                        if not multi_payload:
                                            raise
                                        failures.append({"ticker": t, "error": str(ex)})
                                        continue
                                    sym_total += n_p
                                    period_rows_ds.append(
                                        {
                                            "timespan": "day",
                                            "multiplier": 1,
                                            "rows_upserted": n_p,
                                        }
                                    )
                                    patch_open_close_date = meta_ds.get(
                                        "patch_open_close_date"
                                    )
                                    if patch_open_close_date:
                                        try:
                                            use_adj = (
                                                True
                                                if adjusted_bool is None
                                                else adjusted_bool
                                            )
                                            oc_data = client.fetch_stock_open_close(
                                                t,
                                                str(patch_open_close_date),
                                                adjusted=use_adj,
                                            )
                                            if oc_data.get("error"):
                                                period_errors_ds.append(
                                                    {
                                                        "timespan": "day",
                                                        "multiplier": 1,
                                                        "mode": "open_close_patch",
                                                        "error": str(oc_data.get("error")),
                                                    }
                                                )
                                            else:
                                                rows_oc = apply_stock_daily_ticker_summary(
                                                    cur,
                                                    t,
                                                    oc_data,
                                                    adjusted=adjusted_bool,
                                                )
                                                sym_total += rows_oc
                                                period_rows_ds.append(
                                                    {
                                                        "timespan": "day",
                                                        "multiplier": 1,
                                                        "mode": "open_close_patch",
                                                        "bar_date": str(
                                                            patch_open_close_date
                                                        ),
                                                        "rows_upserted": rows_oc,
                                                    }
                                                )
                                        except Exception as ex:
                                            period_errors_ds.append(
                                                {
                                                    "timespan": "day",
                                                    "multiplier": 1,
                                                    "mode": "open_close_patch",
                                                    "error": str(ex),
                                                }
                                            )
                                    per_symbol.append(
                                        {
                                            "ticker": t,
                                            "rows_upserted": sym_total,
                                            "sync_all_periods": True,
                                            "custom_bars_period_group": "daily",
                                            "custom_bars_sync_mode": "daily_smart",
                                            "daily_sync_policy": policy,
                                            "resolved_start_ms": eff_start_ms,
                                            "resolved_end_ms": eff_end_ms,
                                            "resolved_start_date": meta_ds.get(
                                                "resolved_start_date"
                                            ),
                                            "resolved_end_date": meta_ds.get(
                                                "resolved_end_date"
                                            ),
                                            "polygon_fetch_ticker": fetch_t,
                                            "periods": period_rows_ds,
                                            "period_errors": period_errors_ds,
                                        }
                                    )
                                    total_n += sym_total
                                except Exception as ex:
                                    if not multi_payload:
                                        raise
                                    failures.append({"ticker": t, "error": str(ex)})
                                    continue
                        if not is_daily_smart:
                            for t in tickers:
                                fetch_t = polygon_ticker_for_massive_aggs(t, ref_indices)
                                if sync_all_periods:
                                    sym_total = 0
                                    period_rows: list[dict[str, object]] = []
                                    period_errors: list[dict[str, object]] = []
                                    for ts_p, mult_p in periods_for_sync:
                                        data = client.fetch_stock_aggs(
                                            fetch_t, mult_p, ts_p, start_ms, end_ms
                                        )
                                        if data.get("error"):
                                            period_errors.append(
                                                {
                                                    "timespan": ts_p,
                                                    "multiplier": mult_p,
                                                    "error": str(data.get("error")),
                                                }
                                            )
                                            continue
                                        try:
                                            n_p = apply_stock_custom_bars(
                                                cur,
                                                t,
                                                ts_p,
                                                mult_p,
                                                data,
                                                adjusted=adjusted_bool,
                                            )
                                        except Exception as ex:
                                            period_errors.append(
                                                {
                                                    "timespan": ts_p,
                                                    "multiplier": mult_p,
                                                    "error": str(ex),
                                                }
                                            )
                                            continue
                                        sym_total += n_p
                                        period_rows.append(
                                            {
                                                "timespan": ts_p,
                                                "multiplier": mult_p,
                                                "rows_upserted": n_p,
                                            }
                                        )
                                    if len(period_rows) == 0:
                                        err_one = "all period fetches failed"
                                        if period_errors:
                                            err_one = str(
                                                period_errors[0].get("error") or err_one
                                            )
                                        if not multi_payload:
                                            raise RuntimeError(err_one)
                                        failures.append({"ticker": t, "error": err_one})
                                        continue
                                    per_symbol.append(
                                        {
                                            "ticker": t,
                                            "rows_upserted": sym_total,
                                            "sync_all_periods": True,
                                            "custom_bars_period_group": period_group_raw
                                            or "all",
                                            "polygon_fetch_ticker": fetch_t,
                                            "periods": period_rows,
                                            "period_errors": period_errors,
                                        }
                                    )
                                    total_n += sym_total
                                else:
                                    data = client.fetch_stock_aggs(
                                        fetch_t, mult, ts, start_ms, end_ms
                                    )
                                    if data.get("error"):
                                        err_s = str(data.get("error"))
                                        if not multi_payload:
                                            raise RuntimeError(err_s)
                                        failures.append({"ticker": t, "error": err_s})
                                        continue
                                    try:
                                        n = apply_stock_custom_bars(
                                            cur, t, ts, mult, data, adjusted=adjusted_bool
                                        )
                                    except Exception as ex:
                                        if not multi_payload:
                                            raise
                                        failures.append({"ticker": t, "error": str(ex)})
                                        continue
                                    total_n += n
                                    per_symbol.append(
                                        {
                                            "ticker": t,
                                            "rows_upserted": n,
                                            "polygon_fetch_ticker": fetch_t,
                                        }
                                    )

                        conn.commit()
                        if multi_payload:
                            result = {
                                "ok": len(failures) == 0,
                                "kind": kind,
                                "mode": mode,
                                "rows_upserted": total_n,
                                "summary": {
                                    "timespan": ts,
                                    "multiplier": mult,
                                    "sync_all_periods": sync_all_periods,
                                    "custom_bars_period_group": period_group_raw
                                    or ("all" if sync_all_periods else ""),
                                    "custom_bars_sync_mode": sync_mode_raw,
                                    "symbols_requested": len(tickers),
                                    "symbols_ok": len(per_symbol),
                                    "failures": failures,
                                    "per_symbol": per_symbol,
                                },
                            }
                        else:
                            t = tickers[0]
                            sum_row: Dict[str, Any] = {
                                "ticker": t,
                                "timespan": ts,
                                "multiplier": mult,
                                "sync_all_periods": sync_all_periods,
                                "custom_bars_period_group": period_group_raw
                                or ("all" if sync_all_periods else ""),
                                "custom_bars_sync_mode": sync_mode_raw,
                            }
                            if sync_all_periods and per_symbol:
                                sum_row["period_detail"] = per_symbol[0]
                            result = {
                                "ok": True,
                                "kind": kind,
                                "mode": mode,
                                "rows_upserted": total_n,
                                "summary": sum_row,
                            }
                    elif mode == "daily_market_summary":
                        d = (payload.get("date") or "").strip()
                        if not d:
                            raise ValueError("payload.date required (YYYY-MM-DD)")
                        use_adj = True if adjusted_bool is None else adjusted_bool
                        data = client.fetch_stock_grouped_daily(d, adjusted=use_adj)
                        if data.get("error"):
                            raise RuntimeError(str(data.get("error")))
                        n = apply_stock_grouped_daily(
                            cur, d, data, adjusted=adjusted_bool
                        )
                        conn.commit()
                        result = {
                            "ok": True,
                            "kind": kind,
                            "mode": mode,
                            "rows_upserted": n,
                            "summary": {"date": d[:10], "results_count": len(data.get("results") or [])},
                        }
                    elif mode == "daily_ticker_summary":
                        t = (payload.get("ticker") or "").strip().upper()
                        d = (payload.get("date") or "").strip()
                        if not t:
                            raise ValueError("payload.ticker required")
                        if not d:
                            raise ValueError("payload.date required (YYYY-MM-DD)")
                        use_adj = True if adjusted_bool is None else adjusted_bool
                        data = client.fetch_stock_open_close(t, d, adjusted=use_adj)
                        if data.get("error"):
                            raise RuntimeError(str(data.get("error")))
                        n = apply_stock_daily_ticker_summary(
                            cur, t, data, adjusted=adjusted_bool
                        )
                        conn.commit()
                        result = {
                            "ok": True,
                            "kind": kind,
                            "mode": mode,
                            "rows_upserted": n,
                            "summary": {"ticker": t, "date": d[:10]},
                        }
                    elif mode == "previous_day_bar":
                        t = (payload.get("ticker") or "").strip().upper()
                        if not t:
                            raise ValueError("payload.ticker required")
                        use_adj = True if adjusted_bool is None else adjusted_bool
                        data = client.fetch_stock_previous_day(t, adjusted=use_adj)
                        if data.get("error"):
                            raise RuntimeError(str(data.get("error")))
                        n = apply_stock_previous_day_bar(
                            cur, t, data, adjusted=adjusted_bool
                        )
                        conn.commit()
                        result = {
                            "ok": True,
                            "kind": kind,
                            "mode": mode,
                            "rows_upserted": n,
                            "summary": {"ticker": t},
                        }
                    else:
                        raise ValueError(f"unknown feed_stocks_aggregate mode: {mode}")
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "feed_options_aggregate":
                mode = (payload.get("mode") or "custom_bars").strip()

                if mode == "open_close":
                    ot = (payload.get("options_ticker") or "").strip()
                    date_str = (payload.get("date") or "").strip()
                    if not ot:
                        raise ValueError("payload.options_ticker required")
                    if not date_str:
                        raise ValueError("payload.date required (YYYY-MM-DD)")
                    data = client.fetch_option_open_close(ot, date_str)
                    if data.get("error"):
                        raise RuntimeError(str(data["error"]))
                    result: Dict[str, Any] = {
                        "ok": True, "kind": kind, "mode": mode,
                        "endpoint": f"/v1/open-close/{ot}/{date_str}",
                        "summary": {
                            "options_ticker": ot, "date": date_str,
                            "open": data.get("open"), "high": data.get("high"),
                            "low": data.get("low"), "close": data.get("close"),
                            "volume": data.get("volume"),
                            "preMarket": data.get("preMarket"),
                            "afterHours": data.get("afterHours"),
                        },
                        "content": data,
                    }
                    persist = bool(payload.get("persist"))
                    if persist:
                        sym_p = (payload.get("symbol") or "").strip().upper()
                        exp_p = (payload.get("expiry") or "").strip()
                        strike_p = float(payload.get("strike") or 0)
                        ort_p = (payload.get("option_right") or "C").strip()
                        bt_raw = payload.get("bar_time")
                        rows_u = 0
                        if sym_p and exp_p and bt_raw is not None:
                            if isinstance(bt_raw, str):
                                btr = bt_raw.strip()
                                if btr.endswith("Z"):
                                    bar_time_val = datetime.fromisoformat(btr.replace("Z", "+00:00"))
                                else:
                                    bar_time_val = datetime.fromisoformat(btr)
                            else:
                                bar_time_val = bt_raw
                            rows_u = _apply_option_day_open_close_update(
                                conn, sym_p, exp_p, strike_p, ort_p, bar_time_val, data
                            )
                            conn.commit()
                        result["rows_updated"] = rows_u
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if mode in ("option_day_pool_row_gap", "option_day_pool_column_fill"):
                    from bifrost_worker.data.massive.option_day_pool_fill import run_option_day_pool_aggregates

                    result = run_option_day_pool_aggregates(
                        conn,
                        client,
                        payload,
                        mode=mode,
                        apply_open_close_update=_apply_option_day_open_close_update,
                        apply_option_day_aggs=_apply_option_day_aggs,
                        patch_vwap=_option_day_patch_vwap_from_day_aggs,
                        rest_throttle=_rest_throttle,
                    )
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if mode == "prev":
                    ot = (payload.get("options_ticker") or "").strip()
                    if not ot:
                        raise ValueError("payload.options_ticker required")
                    data = client.fetch_option_previous_day(ot)
                    if data.get("error"):
                        raise RuntimeError(str(data["error"]))
                    bars = data.get("results") or []
                    bar = bars[0] if bars else {}
                    result = {
                        "ok": True, "kind": kind, "mode": mode,
                        "endpoint": f"/v2/aggs/ticker/{ot}/prev",
                        "summary": {
                            "options_ticker": ot,
                            "open": bar.get("o"), "high": bar.get("h"),
                            "low": bar.get("l"), "close": bar.get("c"),
                            "volume": bar.get("v"), "vwap": bar.get("vw"),
                            "transactions": bar.get("n"),
                            "timestamp": bar.get("t"),
                        },
                        "content": data,
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if mode in ("option_min_pool_row_gap", "option_min_pool_column_fill"):
                    from bifrost_worker.data.massive.option_min_pool_fill import run_option_min_pool_aggregates

                    result = run_option_min_pool_aggregates(
                        conn, client, payload, mode=mode
                    )
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if mode == "option_snapshots_pool_contract_fill":
                    from bifrost_worker.data.massive.option_snapshots_pool_fill import run_option_snapshots_pool_contract_fill

                    result = run_option_snapshots_pool_contract_fill(conn, client, payload)
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                # default: custom_bars (backward-compatible)
                ot = (payload.get("options_ticker") or "").strip()
                if not ot:
                    raise ValueError("payload.options_ticker required")
                mult = int(payload.get("multiplier") or 1)
                ts = (payload.get("timespan") or "minute").strip()
                start_ms = int(payload.get("start_ms") or 0)
                end_ms = int(payload.get("end_ms") or 0)
                sym = (payload.get("symbol") or "").strip().upper()
                exp = (payload.get("expiry") or "").strip()
                strike = float(payload.get("strike") or 0)
                opt_right = (payload.get("option_right") or "C").strip()
                if not start_ms or not end_ms:
                    raise ValueError("start_ms and end_ms required")
                ts_norm = ts.lower()
                if ts_norm == "day":
                    mult = max(1, mult)
                    aggs = client.fetch_option_aggs(ot, mult, "day", start_ms, end_ms)
                    if aggs.get("error"):
                        raise RuntimeError(str(aggs.get("error")))
                    count = _apply_option_day_aggs(conn, sym, exp, strike, opt_right, aggs)
                else:
                    # Align option_min.period with Massive multiplier (e.g. 5 + minute → "5 mins", not "1 min").
                    period = timespan_to_stock_period(ts, mult)
                    aggs = client.fetch_option_aggs(ot, mult, ts, start_ms, end_ms)
                    if aggs.get("error"):
                        raise RuntimeError(str(aggs.get("error")))
                    count = _apply_aggs(conn, sym, exp, strike, opt_right, period, aggs)
                conn.commit()
                result = {"ok": True, "kind": kind, "mode": "custom_bars", "bars_upserted": count}
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "oi":
                mode = (payload.get("mode") or "watchlist_eod").strip().lower()
                if mode == "watchlist_eod":
                    result = _run_oi_watchlist_eod(conn, client, status_cfg, payload)
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result
                result = {
                    "ok": True,
                    "kind": kind,
                    "message": "Use mode=watchlist_eod for EOD OI, or snapshot chain for point-in-time",
                }
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "eod_pipeline":
                from bifrost_core.monitor.reader.market import get_is_us_trading_day

                td_s = _eod_trade_date_str_et(payload)
                if not get_is_us_trading_day(status_cfg, td_s):
                    result = {
                        "ok": True,
                        "skipped": True,
                        "reason": "not a US trading day",
                        "trade_date": td_s,
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result
                oi_res = _run_oi_watchlist_eod(conn, client, status_cfg, payload)
                mp_res = _run_max_pain(conn, status_cfg, payload)
                result = {"ok": True, "kind": kind, "oi": oi_res, "report_option_max_pain": mp_res}
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "report_option_max_pain":
                result = _run_max_pain(conn, status_cfg, payload)
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "reconcile":
                result = _run_reconcile(conn, client, status_cfg, payload)
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "trim_jobs":
                result = _run_trim_jobs(conn)
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "feed_stocks_corporate_action":
                syms = payload.get("symbols")
                if isinstance(syms, list) and syms:
                    total = 0
                    for raw_s in syms:
                        sym_one = str(raw_s).strip().upper()
                        if not sym_one:
                            continue
                        total += _apply_feed_stocks_corporate_actions(conn, client, sym_one)
                        _rest_throttle()
                    conn.commit()
                    result = {
                        "ok": True,
                        "kind": kind,
                        "rows_upserted": total,
                        "symbols_count": len(syms),
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result
                sym = (payload.get("symbol") or "").strip().upper()
                if not sym:
                    raise ValueError("payload.symbol or payload.symbols required")
                count = _apply_feed_stocks_corporate_actions(conn, client, sym)
                conn.commit()
                result = {"ok": True, "kind": kind, "rows_upserted": count}
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "feed_option_contracts":
                mode = (payload.get("mode") or "list").strip().lower()
                if mode == "detail":
                    ot = (payload.get("options_ticker") or "").strip()
                    if not ot:
                        raise ValueError("payload.options_ticker required")
                    data = client.fetch_option_contract_detail(ot)
                    if data.get("error"):
                        raise RuntimeError(str(data["error"]))
                    res_obj = data.get("results") if isinstance(data.get("results"), dict) else {}
                    result = {
                        "ok": True, "kind": kind, "mode": "detail",
                        "summary": {
                            "options_ticker": ot,
                            "ticker": res_obj.get("ticker"),
                            "underlying_ticker": res_obj.get("underlying_ticker"),
                            "expiration_date": res_obj.get("expiration_date"),
                            "strike_price": res_obj.get("strike_price"),
                            "contract_type": res_obj.get("contract_type"),
                            "exercise_style": res_obj.get("exercise_style"),
                            "shares_per_contract": res_obj.get("shares_per_contract"),
                        },
                        "content": res_obj,
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if mode == "reference_upsert":
                    u = (payload.get("underlying") or payload.get("symbol") or "").strip().upper()
                    if not u:
                        raise ValueError("payload.underlying required")
                    from bifrost_worker.data.massive.vendor.reader import refresh_expirations_from_massive_api

                    exp_raw = payload.get("expiration_date")
                    exp_opt = str(exp_raw).strip() if exp_raw not in (None, "") else None
                    out = refresh_expirations_from_massive_api(
                        status_cfg, config, u, expiration_date=exp_opt, skip_persist=False
                    )
                    if out.get("error"):
                        raise RuntimeError(str(out["error"]))
                    rs = int(out.get("rows_upserted") or 0)
                    expi = out.get("expirations")
                    expi_out = expi[:80] if isinstance(expi, list) else expi
                    result = {
                        "ok": True,
                        "kind": kind,
                        "mode": "reference_upsert",
                        "summary": {
                            "underlying": u,
                            "endpoint": "GET /v3/reference/options/contracts (paginated; rows upserted to option_contracts)",
                            "rows_upserted": rs,
                            "expirations_returned": len(expi) if isinstance(expi, list) else 0,
                        },
                        "content": {
                            "expirations": expi_out,
                            "massive_debug": out.get("massive_debug"),
                        },
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if mode == "nullable_column_backfill":
                    u = (payload.get("underlying") or payload.get("symbol") or "").strip().upper()
                    if not u:
                        raise ValueError("payload.underlying required")
                    col_one = (payload.get("column") or "").strip().lower()
                    raw_cols = payload.get("columns")
                    want: Set[str] = set()
                    if isinstance(raw_cols, list) and raw_cols:
                        for c in raw_cols:
                            s = str(c).strip().lower()
                            if s in ("exercise_style", "shares_per_contract"):
                                want.add(s)
                    elif col_one in ("exercise_style", "shares_per_contract"):
                        want.add(col_one)
                    elif col_one in ("both", "nullable", "optional", ""):
                        want.update(["exercise_style", "shares_per_contract"])
                    else:
                        want.update(["exercise_style", "shares_per_contract"])
                    if not want:
                        raise ValueError(
                            "payload.column or payload.columns must list exercise_style and/or shares_per_contract",
                        )
                    try:
                        max_c_i = int(payload.get("max_contracts") or 2000)
                    except (TypeError, ValueError):
                        max_c_i = 2000
                    max_c_i = max(1, min(max_c_i, 10000))
                    need_ex = "exercise_style" in want
                    need_sh = "shares_per_contract" in want
                    or_parts: List[str] = []
                    if need_ex:
                        or_parts.append("(exercise_style IS NULL OR TRIM(exercise_style) = '')")
                    if need_sh:
                        or_parts.append("shares_per_contract IS NULL")
                    where_fill = " OR ".join(or_parts) if or_parts else "FALSE"
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            SELECT contract_key, massive_option_ticker
                            FROM option_contracts
                            WHERE UPPER(TRIM(symbol::text)) = %s
                              AND massive_option_ticker IS NOT NULL
                              AND TRIM(massive_option_ticker::text) <> ''
                              AND ({where_fill})
                            ORDER BY contract_key
                            LIMIT %s
                            """,
                            (u, max_c_i),
                        )
                        rows_bf = cur.fetchall()
                    n_api = 0
                    upd_ex = 0
                    upd_sh = 0
                    err_sample: List[str] = []
                    _gap_bf = _nullable_column_backfill_gap_sec()
                    _commit_every = 40
                    _rows_uncommitted = 0
                    _total_cand = len(rows_bf)
                    for crow in rows_bf:
                        ck = crow[0]
                        ot = (crow[1] or "").strip()
                        if not ck or not ot:
                            continue
                        time_module.sleep(_gap_bf)
                        n_api += 1
                        if _total_cand > 0 and n_api % 250 == 0:
                            update_job_massive_backfill_result(
                                status_cfg,
                                job_id,
                                "running",
                                {
                                    "ok": True,
                                    "kind": kind,
                                    "phase": "running",
                                    "summary": {
                                        "mode": "nullable_column_backfill",
                                        "underlying": u,
                                        "processed_detail_calls": n_api,
                                        "total_candidates": _total_cand,
                                        "pct": round(100.0 * n_api / _total_cand, 2),
                                    },
                                },
                            )
                        data = client.fetch_option_contract_detail(ot)
                        if data.get("error"):
                            if len(err_sample) < 15:
                                err_sample.append(f"{ot}: {data.get('error')}")
                            continue
                        res_obj = data.get("results") if isinstance(data.get("results"), dict) else {}
                        with conn.cursor() as cur:
                            if need_ex:
                                ex_raw = res_obj.get("exercise_style")
                                exv = (str(ex_raw).strip() if ex_raw is not None else "") or None
                                if exv:
                                    cur.execute(
                                        """
                                        UPDATE option_contracts
                                        SET exercise_style = %s
                                        WHERE contract_key = %s
                                          AND (exercise_style IS NULL OR TRIM(exercise_style) = '')
                                        """,
                                        (exv, ck),
                                    )
                                    upd_ex += int(cur.rowcount)
                            if need_sh:
                                spc = res_obj.get("shares_per_contract")
                                shares_i: Optional[int] = None
                                if spc is not None:
                                    try:
                                        shares_i = int(spc)
                                    except (TypeError, ValueError):
                                        shares_i = None
                                if shares_i is not None:
                                    cur.execute(
                                        """
                                        UPDATE option_contracts
                                        SET shares_per_contract = %s
                                        WHERE contract_key = %s
                                          AND shares_per_contract IS NULL
                                        """,
                                        (shares_i, ck),
                                    )
                                    upd_sh += int(cur.rowcount)
                        _rows_uncommitted += 1
                        if _rows_uncommitted >= _commit_every:
                            conn.commit()
                            _rows_uncommitted = 0
                    if _rows_uncommitted:
                        conn.commit()
                    result = {
                        "ok": True,
                        "kind": kind,
                        "mode": "nullable_column_backfill",
                        "summary": {
                            "underlying": u,
                            "columns": sorted(want),
                            "candidates_queried": len(rows_bf),
                            "detail_api_calls": n_api,
                            "rows_updated_exercise_style": upd_ex,
                            "rows_updated_shares_per_contract": upd_sh,
                            "errors_sample": err_sample,
                        },
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                u = (payload.get("underlying") or payload.get("symbol") or "").strip().upper()
                if not u:
                    raise ValueError("payload.underlying required")
                list_kwargs: Dict[str, Any] = {}
                if payload.get("expiration_date"):
                    list_kwargs["expiration_date"] = str(payload["expiration_date"])
                if payload.get("contract_type"):
                    list_kwargs["contract_type"] = str(payload["contract_type"])
                for fk in ("limit",):
                    v = payload.get(fk)
                    if v is not None:
                        try:
                            list_kwargs[fk] = int(v)
                        except (TypeError, ValueError):
                            pass
                for fk in ("sort", "order"):
                    v = payload.get(fk)
                    if v:
                        list_kwargs[fk] = str(v)
                for fk in ("strike_price", "strike_price_gte", "strike_price_lte"):
                    v = payload.get(fk)
                    if v is not None:
                        try:
                            list_kwargs[fk] = float(v)
                        except (TypeError, ValueError):
                            pass
                data = client.fetch_option_contracts_list(u, **list_kwargs)
                if data.get("error"):
                    raise RuntimeError(str(data["error"]))
                raw_results = data.get("results") or []
                if not isinstance(raw_results, list):
                    raw_results = []
                contracts_total = len(raw_results)
                contracts_with_ticker = sum(
                    1 for c in raw_results if isinstance(c, dict) and c.get("ticker")
                )
                contracts_with_identity = sum(
                    1 for c in raw_results if isinstance(c, dict)
                    and c.get("underlying_ticker")
                    and c.get("expiration_date")
                    and c.get("strike_price") is not None
                    and c.get("contract_type")
                )
                content_items = raw_results[:100]
                result = {
                    "ok": True, "kind": kind, "mode": "list",
                    "summary": {
                        "underlying": u,
                        "results_count": contracts_total,
                        "has_next_page": bool(data.get("next_url")),
                        "filters": dict(list_kwargs) if list_kwargs else {},
                        "contracts_with_ticker": contracts_with_ticker,
                        "contracts_with_complete_identity": contracts_with_identity,
                    },
                    "content": content_items,
                    "content_truncated": len(raw_results) > 100,
                }
                update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                return result

            if kind == "feed_options_trades_quotes":
                mode = (payload.get("mode") or "last_trade").strip().lower()
                ot = (payload.get("options_ticker") or "").strip()
                if not ot:
                    raise ValueError("payload.options_ticker required")

                if mode == "last_trade":
                    data = client.fetch_last_trade(ot)
                    if data.get("error"):
                        raise RuntimeError(str(data["error"]))
                    res_obj = data.get("results") if isinstance(data.get("results"), dict) else {}
                    result = {
                        "ok": True, "kind": kind, "mode": mode,
                        "summary": {
                            "options_ticker": ot,
                            "price": res_obj.get("p") or res_obj.get("price"),
                            "size": res_obj.get("s") or res_obj.get("size"),
                            "exchange": res_obj.get("x") or res_obj.get("exchange"),
                            "sip_timestamp": res_obj.get("t") or res_obj.get("sip_timestamp"),
                        },
                        "content": data,
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if mode == "quotes":
                    ts_gte = (payload.get("timestamp_gte") or "").strip() or None
                    ts_lte = (payload.get("timestamp_lte") or "").strip() or None
                    lim = int(payload.get("limit") or 100)
                    sort_order = (payload.get("sort") or "asc").strip()
                    data = client.fetch_option_quotes(
                        ot, timestamp_gte=ts_gte, timestamp_lte=ts_lte,
                        limit=lim, order=sort_order,
                    )
                    if data.get("error"):
                        raise RuntimeError(str(data["error"]))
                    raw = data.get("results") or []
                    if not isinstance(raw, list):
                        raw = []
                    content_items = raw[:100]
                    result = {
                        "ok": True, "kind": kind, "mode": mode,
                        "summary": {
                            "options_ticker": ot,
                            "results_count": len(raw),
                        },
                        "content": content_items,
                        "content_truncated": len(raw) > 100,
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if mode == "trades":
                    ts_gte = (payload.get("timestamp_gte") or "").strip() or None
                    ts_lte = (payload.get("timestamp_lte") or "").strip() or None
                    lim = int(payload.get("limit") or 100)
                    sort_order = (payload.get("sort") or "asc").strip()
                    data = client.fetch_option_trades(
                        ot, timestamp_gte=ts_gte, timestamp_lte=ts_lte,
                        limit=lim, order=sort_order,
                    )
                    if data.get("error"):
                        raise RuntimeError(str(data["error"]))
                    raw = data.get("results") or []
                    if not isinstance(raw, list):
                        raw = []
                    content_items = raw[:100]
                    result = {
                        "ok": True, "kind": kind, "mode": mode,
                        "summary": {
                            "options_ticker": ot,
                            "results_count": len(raw),
                        },
                        "content": content_items,
                        "content_truncated": len(raw) > 100,
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                raise ValueError(f"unknown feed_options_trades_quotes mode: {mode}")

            _ticker_ref_kinds = frozenset(
                {
                    "feed_stocks_tickers_reference_universe",
                    "feed_stocks_tickers_overview",
                    "feed_stocks_tickers_related",
                    "feed_stocks_tickers_types",
                }
            )
            if kind in _ticker_ref_kinds:
                from bifrost_core.persistence.postgres.ticker_reference import (
                    SYNC_KIND_UNIVERSE,
                    all_ticker_symbols,
                    get_reference_state,
                    get_tickers_id_for_ticker,
                    next_cursor_from_api_response,
                    normalize_ticker_ref_kind,
                    overview_stub_cols_api_not_found,
                    replace_related_for_tickers_id,
                    replace_ticker_types,
                    row_from_ticker_detail,
                    row_from_ticker_list_item,
                    symbols_missing_overview_only,
                    symbols_missing_related_only,
                    symbols_needing_overview,
                    symbols_needing_related_stale,
                    upsert_reference_state,
                    upsert_ticker_overview_row,
                    upsert_ticker_row,
                )
                from bifrost_worker.data.massive.vendor.reference_cache_keys import (
                    invalidate_search_caches,
                    invalidate_ticker_cache,
                    key_peers,
                    key_ticker_types,
                    redis_client_from_status_config,
                )

                kind = normalize_ticker_ref_kind(kind)
                rds = redis_client_from_status_config(status_cfg)
                cur = conn.cursor()

                if kind == "feed_stocks_tickers_reference_universe":
                    reset = bool(payload.get("reset_cursor"))
                    full_u = bool(payload.get("full_universe"))
                    mp_raw = payload.get("max_pages")
                    try:
                        mp_int = int(mp_raw) if mp_raw is not None else None
                    except (TypeError, ValueError):
                        mp_int = None
                    if full_u or (mp_int is not None and mp_int <= 0):
                        max_pages: Optional[int] = None
                    else:
                        max_pages = max(1, int(mp_raw or 50))
                    cursor_in = (payload.get("cursor") or "").strip() or None
                    if reset:
                        cursor = None
                        upsert_reference_state(cur, SYNC_KIND_UNIVERSE, None, "running")
                        conn.commit()
                    else:
                        cursor = cursor_in
                        if cursor is None:
                            st = get_reference_state(cur, SYNC_KIND_UNIVERSE)
                            if st and st.get("last_cursor"):
                                cursor = (st.get("last_cursor") or "").strip() or None
                    total = 0
                    pages = 0
                    last_next: Optional[str] = None
                    while max_pages is None or pages < max_pages:
                        lim = min(1000, max(1, int(payload.get("limit") or 1000)))
                        sort_f = (payload.get("sort") or "ticker").strip() or "ticker"
                        order_f = (payload.get("order") or "asc").strip() or "asc"
                        data = client.fetch_reference_tickers(
                            market="stocks",
                            limit=lim,
                            sort=sort_f,
                            order=order_f,
                            cursor=cursor,
                        )
                        if data.get("error"):
                            raise RuntimeError(str(data["error"]))
                        results = data.get("results") or []
                        if not isinstance(results, list):
                            results = []
                        for row in results:
                            m = row_from_ticker_list_item(row if isinstance(row, dict) else {})
                            if not m:
                                continue
                            upsert_ticker_row(cur, m)
                            total += 1
                        conn.commit()
                        last_next = next_cursor_from_api_response(data)
                        upsert_reference_state(cur, SYNC_KIND_UNIVERSE, last_next, "running")
                        conn.commit()
                        if rds:
                            invalidate_search_caches(rds)
                        if not last_next:
                            upsert_reference_state(cur, SYNC_KIND_UNIVERSE, None, "done")
                            conn.commit()
                            break
                        cursor = last_next
                        pages += 1
                        _rest_throttle()
                    if rds:
                        invalidate_search_caches(rds)
                    result = {
                        "ok": True,
                        "kind": kind,
                        "summary": {
                            "rows_upserted": total,
                            "pages": pages,
                            "next_cursor": last_next,
                        },
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if kind == "feed_stocks_tickers_overview":
                    mode = (payload.get("mode") or "stale").strip().lower()
                    stale_h = int(payload.get("stale_hours") or 720)
                    if mode == "all":
                        syms = all_ticker_symbols(cur)
                    elif mode == "symbols":
                        syms_raw = payload.get("symbols") or []
                        if isinstance(syms_raw, str):
                            syms = [
                                x.strip().upper()
                                for x in syms_raw.replace(",", " ").split()
                                if x.strip()
                            ]
                        elif isinstance(syms_raw, list):
                            syms = [str(x).strip().upper() for x in syms_raw if str(x).strip()]
                        else:
                            syms = []
                    elif mode == "missing":
                        syms = symbols_missing_overview_only(cur)
                    elif mode == "stale":
                        syms = symbols_needing_overview(cur, stale_h)
                    else:
                        syms = symbols_needing_overview(cur, stale_h)
                    total_sym = len(syms)
                    n_ok = 0
                    n_stub_not_found = 0
                    errors = []
                    if _should_emit_ticker_ref_progress(0, total_sym):
                        _emit_massive_job_running_progress(
                            status_cfg,
                            job_id,
                            kind="feed_stocks_tickers_overview",
                            work_mode=mode,
                            total=total_sym,
                            processed=0,
                            current_symbol=None,
                            symbols_ok=0,
                            symbols_failed=0,
                            errors_sample=errors,
                        )
                    for idx, sym in enumerate(syms):
                        _rest_throttle()
                        det = client.fetch_ticker_detail(sym)
                        if det.get("error"):
                            err_raw = det.get("error")
                            err_s = err_raw if isinstance(err_raw, str) else json.dumps(
                                err_raw, default=str
                            )
                            if "NOT_FOUND" in err_s.upper() or "Ticker not found" in err_s:
                                tid_nf = get_tickers_id_for_ticker(cur, sym)
                                if tid_nf:
                                    upsert_ticker_overview_row(
                                        cur, tid_nf, overview_stub_cols_api_not_found()
                                    )
                                    conn.commit()
                                    if rds:
                                        invalidate_ticker_cache(rds, sym)
                                    n_stub_not_found += 1
                                else:
                                    errors.append(f"{sym}: NOT_FOUND (no tickers row)")
                            else:
                                errors.append(f"{sym}: {det.get('error')}")
                        else:
                            tcols, dcols = row_from_ticker_detail(det)
                            if not tcols.get("ticker"):
                                errors.append(f"{sym}: no ticker in response")
                            else:
                                tid = upsert_ticker_row(cur, tcols)
                                upsert_ticker_overview_row(cur, tid, dcols)
                                conn.commit()
                                if rds:
                                    invalidate_ticker_cache(rds, sym)
                                n_ok += 1
                        processed = idx + 1
                        if _should_emit_ticker_ref_progress(processed, total_sym):
                            _emit_massive_job_running_progress(
                                status_cfg,
                                job_id,
                                kind="feed_stocks_tickers_overview",
                                work_mode=mode,
                                total=total_sym,
                                processed=processed,
                                current_symbol=sym,
                                symbols_ok=n_ok + n_stub_not_found,
                                symbols_failed=len(errors),
                                errors_sample=errors,
                            )
                    result = {
                        "ok": True,
                        "kind": kind,
                        "phase": "done",
                        "summary": {
                            "overview_mode": mode,
                            "symbols_requested": total_sym,
                            "symbols_upserted": n_ok,
                            "symbols_overview_stub_not_found": n_stub_not_found,
                            "symbols_failed": len(errors),
                            "errors_sample": errors[:20],
                        },
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if kind == "feed_stocks_tickers_related":
                    mode = (payload.get("mode") or "symbols").strip().lower()
                    stale_h = int(payload.get("stale_hours") or 720)
                    if mode == "all":
                        syms = all_ticker_symbols(cur)
                    elif mode == "symbols":
                        syms_raw = payload.get("symbols") or []
                        if isinstance(syms_raw, str):
                            syms = [
                                x.strip().upper()
                                for x in syms_raw.replace(",", " ").split()
                                if x.strip()
                            ]
                        elif isinstance(syms_raw, list):
                            syms = [str(x).strip().upper() for x in syms_raw if str(x).strip()]
                        else:
                            syms = []
                    elif mode == "missing":
                        syms = symbols_missing_related_only(cur)
                    elif mode == "stale":
                        syms = symbols_needing_related_stale(cur, stale_h)
                    else:
                        syms_raw = payload.get("symbols") or []
                        if isinstance(syms_raw, str):
                            syms = [
                                x.strip().upper()
                                for x in syms_raw.replace(",", " ").split()
                                if x.strip()
                            ]
                        elif isinstance(syms_raw, list):
                            syms = [str(x).strip().upper() for x in syms_raw if str(x).strip()]
                        else:
                            syms = []
                    total_rel = len(syms)
                    n_ok = 0
                    fetched_at = datetime.now(timezone.utc)
                    if _should_emit_ticker_ref_progress(0, total_rel):
                        _emit_massive_job_running_progress(
                            status_cfg,
                            job_id,
                            kind="feed_stocks_tickers_related",
                            work_mode=mode,
                            total=total_rel,
                            processed=0,
                            current_symbol=None,
                            symbols_ok=0,
                            symbols_failed=0,
                            errors_sample=[],
                        )
                    for idx, sym in enumerate(syms):
                        _rest_throttle()
                        tid = get_tickers_id_for_ticker(cur, sym)
                        if tid:
                            rel = client.fetch_related_companies(sym)
                            if not rel.get("error"):
                                raw = rel.get("results") or []
                                if not isinstance(raw, list):
                                    raw = []
                                replace_related_for_tickers_id(cur, tid, raw, fetched_at)
                                conn.commit()
                                if rds:
                                    invalidate_ticker_cache(rds, sym)
                                    try:
                                        rds.delete(key_peers(sym))
                                    except Exception:
                                        pass
                                n_ok += 1
                        processed = idx + 1
                        if _should_emit_ticker_ref_progress(processed, total_rel):
                            _emit_massive_job_running_progress(
                                status_cfg,
                                job_id,
                                kind="feed_stocks_tickers_related",
                                work_mode=mode,
                                total=total_rel,
                                processed=processed,
                                current_symbol=sym,
                                symbols_ok=n_ok,
                                symbols_failed=0,
                                errors_sample=[],
                            )
                    result = {
                        "ok": True,
                        "kind": kind,
                        "phase": "done",
                        "summary": {
                            "related_mode": mode,
                            "symbols_processed": n_ok,
                            "total_requested": total_rel,
                            "rows_attempted": total_rel,
                        },
                    }
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

                if kind == "feed_stocks_tickers_types":
                    ac = (payload.get("asset_class") or "").strip() or None
                    loc = (payload.get("locale") or "").strip() or None
                    data = client.fetch_ticker_types(asset_class=ac, locale=loc)
                    if data.get("error"):
                        raise RuntimeError(str(data["error"]))
                    res = data.get("results") or []
                    if not isinstance(res, list):
                        res = []
                    n = replace_ticker_types(cur, res)
                    conn.commit()
                    if rds:
                        try:
                            rds.delete(key_ticker_types(loc or "*", ac or "*"))
                        except Exception:
                            pass
                    result = {"ok": True, "kind": kind, "summary": {"rows": n}}
                    update_job_massive_backfill_result(status_cfg, job_id, "done", result)
                    return result

            from bifrost_worker.data.massive.celery_queues import MASSIVE_STOCKS_QUEUE_KINDS

            all_known = sorted(MASSIVE_STOCKS_QUEUE_KINDS)
            in_fundamentals = kind in FEED_STOCKS_FINANCIALS_KINDS
            in_fin_runners = kind in _fin_runners

            raise ValueError(
                f"unknown kind: {kind!r}. "
                f"kind_in_fin_runners={in_fin_runners}; "
                f"kind_in_fundamentals_frozenset={in_fundamentals}; "
                f"fin_runner_keys_this_worker={sorted(_fin_runners.keys())!r}; "
                f"massive_stocks_union_kinds_this_worker={all_known}. "
                f"If this kind should exist, update run_massive_job in src/massive/tasks.py "
                f"or restart workers after pulling code."
            )
        except Exception as e:
            conn.rollback()
            logger.exception("run_massive_job failed: %s", e)
            err = {"ok": False, "error": str(e)}
            update_job_massive_backfill_result(status_cfg, job_id, "failed", err)
            return err
        finally:
            conn.close()
    except Exception as e:
        from bifrost_worker.data.massive.vendor.reader import release_massive_job_to_pending_for_redispatch

        transient = isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError))
        if transient:
            logger.warning(
                "run_massive_job transient DB error job_id=%s (row left pending for redispatch): %s",
                job_id,
                e,
            )
            release_massive_job_to_pending_for_redispatch(status_cfg, job_id)
            return {"ok": False, "error": str(e), "retry": "pending_redispatch"}
        logger.exception("run_massive_job outer: %s", e)
        err = {"ok": False, "error": str(e)}
        update_job_massive_backfill_result(status_cfg, job_id, "failed", err)
        return err
    finally:
        if dispatch_qname:
            try:
                from bifrost_worker.data.massive.pending_dispatch import dispatch_pending_massive_topup

                dispatch_pending_massive_topup(status_cfg, dispatch_qname)
            except Exception:
                logger.exception("dispatch_pending_massive_topup after job_id=%s", job_id)



def apply_async_massive_pending_job(
    control_via_db: dict,
    job_id: int,
    queue_name: str,
    *,
    countdown: Optional[float] = None,
    pre_dispatch_token: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Reserve ``dispatch:…`` (unless ``pre_dispatch_token``), ``apply_async``, then bind Celery UUID (or revoke on failure).

    Returns ``(ok, error, celery_task_id_or_none)`` so callers can log Celery correlation id.
    """
    from bifrost_worker.data.massive.vendor.reader import (
        clear_massive_dispatch_token,
        finalize_massive_dispatch_celery_id,
        reserve_massive_dispatch_token,
    )

    pre = (pre_dispatch_token or "").strip()
    if pre.startswith("dispatch:"):
        dispatch_token = pre
    else:
        locked = reserve_massive_dispatch_token(control_via_db, job_id)
        if locked is None:
            return False, "reserve_dispatch_failed_or_not_pending", None
        dispatch_token = str(locked.get("celery_task_id") or "").strip()

    ftok = dispatch_token if dispatch_token.startswith("dispatch:") else None
    try:
        if countdown is not None:
            async_result = run_massive_job.apply_async(
                args=[job_id], queue=queue_name, countdown=countdown,
            )
        else:
            async_result = run_massive_job.apply_async(args=[job_id], queue=queue_name)
    except Exception as e:
        logger.exception(
            "apply_async_massive_pending_job enqueue failed job_id=%s: %s",
            job_id,
            e,
        )
        if ftok:
            clear_massive_dispatch_token(control_via_db, job_id, dispatch_token)
        return False, str(e), None

    celery_rid = getattr(async_result, "id", None)
    celery_rid_s = str(celery_rid).strip() if celery_rid else ""

    if not finalize_massive_dispatch_celery_id(
        control_via_db, job_id, ftok, celery_rid_s,
    ):
        try:
            app.control.revoke(async_result.id, terminate=False)
        except Exception:
            pass
        if ftok:
            clear_massive_dispatch_token(control_via_db, job_id, dispatch_token)
        return False, "finalize_dispatch_failed", None

    return True, None, celery_rid_s


def _enqueue_massive_job(kind: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Insert ``job_massive_backfill`` row and ``apply_async`` to the correct broker queue."""
    from bifrost_core.config.startup import read_config
    from bifrost_worker.data.massive.celery_queues import celery_queue_for_massive_job
    from bifrost_worker.data.massive.vendor.reader import insert_job_massive_backfill

    cfg_path = _config_path_for_task()
    config, _ = read_config(cfg_path)
    jid, dedup = insert_job_massive_backfill(config, kind, payload or {})
    if jid is None:
        return {"ok": False, "error": "enqueue failed"}
    if dedup:
        logger.info("Massive beat: deduplicated kind=%s job_id=%s", kind, jid)
        return {"ok": True, "deduplicated": True, "job_id": jid}
    queue_name = celery_queue_for_massive_job(kind, priority_high=False)
    ok, err, cid = apply_async_massive_pending_job(config, jid, queue_name)
    if not ok:
        return {"ok": False, "error": err or "enqueue failed"}
    try:
        from bifrost_worker.data.massive.pending_dispatch import dispatch_pending_massive_topup

        dispatch_pending_massive_topup(config, queue_name)
    except Exception:
        logger.debug("dispatch_pending_massive_topup after _enqueue_massive_job", exc_info=True)
    return {"ok": True, "job_id": jid, "celery_task_id": cid}


@app.task(name="src.massive.tasks.beat_eod_pipeline")
def beat_eod_pipeline() -> Dict[str, Any]:
    """Celery Beat: enqueue ``eod_pipeline`` (Watchlist EOD OI + Max Pain)."""
    return _enqueue_massive_job("eod_pipeline", {})


@app.task(name="src.massive.tasks.beat_corporate_watchlist")
def beat_corporate_watchlist() -> Dict[str, Any]:
    """Celery Beat: enqueue ``feed_stocks_corporate_action`` for all Watchlist optionable STK symbols."""
    from bifrost_core.config.startup import read_config
    from bifrost_worker.data.massive.vendor.reader import get_watchlist_optionable_stk_symbols

    cfg_path = _config_path_for_task()
    config, _ = read_config(cfg_path)
    symbols = get_watchlist_optionable_stk_symbols(config)
    if not symbols:
        logger.info("beat_corporate_watchlist: empty watchlist, skip")
        return {"ok": True, "skipped": True, "reason": "empty watchlist"}
    return _enqueue_massive_job("feed_stocks_corporate_action", {"symbols": symbols})


@app.task(name="src.massive.tasks.beat_reconcile")
def beat_reconcile() -> Dict[str, Any]:
    """Celery Beat: enqueue ``reconcile`` (Watchlist vs DB OI counts)."""
    return _enqueue_massive_job("reconcile", {})


@app.task(name="src.massive.tasks.beat_trim_massive_jobs")
def beat_trim_massive_jobs() -> Dict[str, Any]:
    """Celery Beat: enqueue ``trim_jobs`` (keep newest 500 rows in job_massive_backfill)."""
    return _enqueue_massive_job("trim_jobs", {})


@app.task(name="src.massive.tasks.beat_refresh_expirations")
def beat_refresh_expirations() -> Dict[str, Any]:
    """Celery Beat: refresh option expiration cache + option_contracts for Watchlist optionable STK symbols."""
    from bifrost_core.config.startup import read_config
    from bifrost_worker.data.massive.vendor.config import get_expiration_cache_settings
    from bifrost_worker.data.massive.vendor.reader import (
        get_watchlist_optionable_stk_symbols,
        refresh_expirations_watchlist_batch,
    )

    cfg_path = _config_path_for_task()
    config, _ = read_config(cfg_path)
    symbols = get_watchlist_optionable_stk_symbols(config)
    if not symbols:
        logger.info("beat_refresh_expirations: empty watchlist, skip")
        return {"ok": True, "skipped": True, "reason": "empty watchlist"}
    batch = get_expiration_cache_settings(config)["beat_batch_size"]
    return refresh_expirations_watchlist_batch(config, config, symbols, max_symbols=batch)


@app.task(name="src.massive.tasks.beat_stock_day_eod")
def beat_stock_day_eod() -> Dict[str, Any]:
    """Celery Beat: enqueue feed_stocks_aggregate daily_smart after market close (UTC 21:30)."""
    from bifrost_core.config.startup import read_config
    from bifrost_worker.data.massive.stock_ohlc_daily_smart import is_ny_session_safely_closed, ny_calendar_today
    from bifrost_core.monitor.reader.market import get_is_us_trading_day
    from bifrost_worker.data.massive.vendor.reader import get_watchlist_optionable_stk_symbols

    cfg_path = _config_path_for_task()
    config, _ = read_config(cfg_path)

    today_et = ny_calendar_today()
    if not get_is_us_trading_day(config, today_et.isoformat()):
        logger.info("beat_stock_day_eod: not a trading day (%s), skip", today_et.isoformat())
        return {"ok": True, "skipped": True, "reason": "not_trading_day", "date": today_et.isoformat()}

    if not is_ny_session_safely_closed():
        logger.warning("beat_stock_day_eod: session not yet closed, skip")
        return {"ok": True, "skipped": True, "reason": "session_not_closed"}

    symbols = get_watchlist_optionable_stk_symbols(config)
    if not symbols:
        logger.info("beat_stock_day_eod: empty watchlist, skip")
        return {"ok": True, "skipped": True, "reason": "empty_watchlist"}

    logger.info("beat_stock_day_eod: enqueuing daily_smart for %d symbols (%s)", len(symbols), today_et.isoformat())
    return _enqueue_massive_job("feed_stocks_aggregate", {
        "mode": "custom_bars",
        "custom_bars_sync_mode": "daily_smart",
        "custom_bars_period_group": "daily",
        "sync_all_periods": True,
        "symbols": symbols,
    })


@app.task(name="src.massive.tasks.beat_sepa_universe_grouped_daily")
def beat_sepa_universe_grouped_daily() -> Dict[str, Any]:
    """Celery Beat: nightly full-market OHLCV update via Grouped Daily Bars API (UTC 22:00).

    One job = one API call = OHLCV for ALL 5,000+ US stocks for today's date.
    Designed for SEPA Screener universe maintenance — covers the full equity universe,
    not just watchlist symbols. Complements beat_stock_day_eod (watchlist gap-fill).
    """
    from bifrost_core.config.startup import read_config
    from bifrost_worker.data.massive.stock_ohlc_daily_smart import is_ny_session_safely_closed, ny_calendar_today
    from bifrost_core.monitor.reader.market import get_is_us_trading_day

    cfg_path = _config_path_for_task()
    config, _ = read_config(cfg_path)

    today_et = ny_calendar_today()
    if not get_is_us_trading_day(config, today_et.isoformat()):
        logger.info(
            "beat_sepa_universe_grouped_daily: not a trading day (%s), skip", today_et.isoformat()
        )
        return {"ok": True, "skipped": True, "reason": "not_trading_day", "date": today_et.isoformat()}

    if not is_ny_session_safely_closed():
        logger.warning("beat_sepa_universe_grouped_daily: session not yet closed, skip")
        return {"ok": True, "skipped": True, "reason": "session_not_closed"}

    date_str = today_et.isoformat()
    logger.info("beat_sepa_universe_grouped_daily: enqueuing daily_market_summary for %s", date_str)
    return _enqueue_massive_job("feed_stocks_aggregate", {
        "mode": "daily_market_summary",
        "date": date_str,
        "adjusted": True,
    })


def reenqueue_massive_job_from_row(control_via_db: dict, row: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """``apply_async`` a pending row to the correct broker queue (standard Celery dispatch)."""
    try:
        jid = int(row["job_massive_backfill_id"])
    except (TypeError, ValueError, KeyError):
        return False, "invalid_job_id"
    from bifrost_core.persistence.postgres.ticker_reference import normalize_ticker_ref_kind

    kind = normalize_ticker_ref_kind(str(row.get("kind") or "").strip())
    if not kind:
        return False, "missing_kind"
    payload = row.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    if not isinstance(payload, dict):
        payload = {}
    priority_high = str(payload.get("priority") or "").strip().lower() == "high"
    from bifrost_worker.data.massive.celery_queues import celery_queue_for_massive_job

    qname = celery_queue_for_massive_job(kind, priority_high=priority_high)
    celery_row = str(row.get("celery_task_id") or "").strip()
    ok, err, _cid = apply_async_massive_pending_job(
        control_via_db,
        jid,
        qname,
        pre_dispatch_token=celery_row if celery_row.startswith("dispatch:") else None,
    )
    if not ok:
        return False, err or "Re-enqueue failed"
    return True, None
