"""Shared backfill logic for API and independent Worker. See ARCHITECTURE §2.7, §4.4."""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from bifrost_worker.data.bars.ib_errors import IBConnectionDroppedError

if TYPE_CHECKING:
    from bifrost_core.monitor.reader import StatusReader
    from bifrost_core.monitor.integrations.ib_clients import MarketIbClient

logger = logging.getLogger(__name__)

# 与 src/connector/ib.py 中 get_historical_bars_range 的 chunk 规则一致，用于 skip_fetch 时打印计划请求
_BAR_SETTING_MAP = {"1 D": "1 day", "1 min": "1 min", "5 mins": "5 mins", "1 hour": "1 hour"}
_CHUNK_MAP = {
    "1 day": (365 * 24 * 60 * 60, "1 Y"),
    "1 min": (24 * 60 * 60, "1 D"),
    "5 mins": (7 * 24 * 60 * 60, "1 W"),
    "1 hour": (7 * 24 * 60 * 60, "1 W"),
}


def _backfill_ib_request_plan(
    symbol: str,
    period: str,
    start_ts: float,
    end_ts: float,
) -> List[Dict[str, Any]]:
    """返回若不 skip 时将会发给 IB 的请求列表（每段一条）。用于 bars_skip_ib 时仅打印、不真实拉取。"""
    per = (period or "1 D").strip()
    bar_setting = _BAR_SETTING_MAP.get(per, "1 day")
    chunk_seconds, duration_str = _CHUNK_MAP.get(bar_setting, (7 * 24 * 60 * 60, "1 W"))
    plan: List[Dict[str, Any]] = []
    cur_end = end_ts
    loops = 0
    while loops < 2000:
        loops += 1
        if cur_end <= start_ts:
            break
        seg_start = max(start_ts, cur_end - chunk_seconds)
        end_dt = datetime.fromtimestamp(cur_end, tz=timezone.utc)
        end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")
        plan.append({
            "symbol": symbol,
            "period": period,
            "barSizeSetting": bar_setting,
            "durationStr": duration_str,
            "endDateTime": end_str,
            "seg_start_ts": seg_start,
            "seg_end_ts": cur_end,
        })
        cur_end = seg_start
        if cur_end <= start_ts:
            break
    return plan


def _backfill_resolve_span(
    period_key: str,
    config: dict,
    years: Optional[float],
    days: Optional[int],
    span_hours: Optional[float] = None,
) -> tuple:
    """Return (start_ts, end_ts) for backfill. period_key: 1D | 1min | 5min | 1h."""
    now = datetime.now(tz=timezone.utc)
    if span_hours is not None and span_hours > 0:
        span = timedelta(hours=span_hours)
    elif days is not None and days > 0:
        span = timedelta(days=days)
    elif years is not None and years > 0:
        span = timedelta(days=365 * years)
    else:
        hb = (config.get("history_backfill") or {}).get("stock") or {}
        if period_key == "1D":
            span = timedelta(days=365 * float(hb.get("daily_years", 10.0)))
        elif period_key == "1min":
            span = timedelta(days=7 * float(hb.get("min_weeks", 1.0)))
        elif period_key == "5min":
            span = timedelta(days=30 * float(hb.get("5min_months", 1.0)))
        else:  # 1h
            span = timedelta(days=30 * float(hb.get("1hour_months", 3.0)))
    start_dt = now - span
    start_ts_out = start_dt.timestamp()
    end_ts_out = now.timestamp()
    logger.info(
        "backfill_resolve_span period_key=%s -> span=%s start_ts=%s end_ts=%s (years=%s days=%s span_hours=%s)",
        period_key, span, start_ts_out, end_ts_out, years, days, span_hours,
    )
    return start_ts_out, end_ts_out


def build_backfill_preview(
    reader: "StatusReader",
    symbol: str,
    period: str,
    years: Optional[float] = None,
    days: Optional[int] = None,
    override_days: Optional[float] = None,
    span_hours: Optional[float] = None,
) -> Dict[str, Any]:
    """Preview what a backfill would overwrite/fill and which IB requests it would use."""
    sym = (symbol or "").strip().upper()
    per = (period or "1 D").strip()
    period_map = {"1 D": "1D", "1 min": "1min", "5 mins": "5min", "1 hour": "1h"}
    period_key = period_map.get(per) or "1D"
    try:
        try:
            from bifrost_core.config.startup import read_config

            config, _ = read_config()
        except Exception:
            config = {}

        latest_ts = reader.get_bars_latest(symbol=sym, period=per)
        end_ts = time.time()
        override_sec = (override_days or 0.0) * 86400.0
        mode = "initial_backfill"
        if latest_ts is not None:
            mode = "incremental_override"
            start_ts = float(latest_ts) - override_sec
            if start_ts > end_ts:
                start_ts = end_ts
        else:
            start_ts, end_ts = _backfill_resolve_span(period_key, config, years, days, span_hours=span_hours)

        override_times = (
            reader.get_bar_times_in_range(symbol=sym, period=per, start_ts=start_ts, end_ts=float(latest_ts))
            if latest_ts is not None
            else []
        )
        ib_request_plan = _backfill_ib_request_plan(sym, per, start_ts, end_ts) if end_ts > start_ts else []
        gap_start_ts = float(latest_ts) if latest_ts is not None else start_ts
        return {
            "symbol": sym,
            "period": per,
            "mode": mode,
            "latest_ts": float(latest_ts) if latest_ts is not None else None,
            "fetch_start_ts": start_ts,
            "fetch_end_ts": end_ts,
            "override_days": float(override_days) if override_days is not None else None,
            "override_records": {
                "count": len(override_times),
                "times": override_times,
                "first_ts": override_times[0] if override_times else None,
                "last_ts": override_times[-1] if override_times else None,
            },
            "gap_to_fill": {
                "start_ts": gap_start_ts,
                "end_ts": end_ts,
                "has_gap": bool(end_ts > gap_start_ts),
                "span_seconds": max(0.0, end_ts - gap_start_ts),
            },
            "ib_request_plan": ib_request_plan,
        }
    except Exception as e:
        logger.warning("build_backfill_preview failed: %s", e, exc_info=True)
        return {"symbol": sym, "period": per, "ok": False, "error": str(e)}


async def run_one_backfill(
    reader: "StatusReader",
    ib_client: Optional["MarketIbClient"],
    control_via_db: Optional[dict],
    symbol: str,
    period: str,
    years: Optional[float] = None,
    days: Optional[int] = None,
    override_days: Optional[float] = None,
    span_hours: Optional[float] = None,
    *,
    skip_fetch: bool = False,
    api_interval_sec: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute one backfill. Used by API (queue=False) and by independent Worker.

    skip_fetch: if True, keep connection but do not call fetch_bars_range/write (test mode).
    Returns {ok, count?, message?} or {ok: False, error}.
    """
    from bifrost_core.monitor.reader import write_stock_bars

    sym = (symbol or "").strip().upper()
    per = (period or "1 D").strip()
    period_map = {"1 D": "1D", "1 min": "1min", "5 mins": "5min", "1 hour": "1h"}
    period_key = period_map.get(per) or "1D"
    try:
        try:
            from bifrost_core.config.startup import read_config
            config, _ = read_config()
        except Exception:
            config = {}
        latest_ts = reader.get_bars_latest(symbol=sym, period=per)
        end_ts = time.time()
        if latest_ts is not None:
            override_sec = (override_days or 0.0) * 86400.0
            start_ts = float(latest_ts) - override_sec
            if start_ts >= end_ts:
                return {"ok": True, "count": 0, "message": "Already have data and no new bars in range; nothing to backfill."}
        else:
            start_ts, end_ts = _backfill_resolve_span(period_key, config, years, days, span_hours=span_hours)
        if ib_client is None:
            return {"ok": False, "error": "IB transport is not initialized."}
        if skip_fetch:
            plan = _backfill_ib_request_plan(sym, per, start_ts, end_ts)
            # 打印将要发给 IB 的参数，便于预先查看、避免超限
            logger.info(
                "[bars_skip_ib] IB fetch skipped, planned requests only: symbol=%s period=%s start_ts=%.0f end_ts=%.0f chunks=%s",
                sym, per, start_ts, end_ts, len(plan),
            )
            for i, req in enumerate(plan):
                logger.info(
                    "[bars_skip_ib] planned request #%s: barSizeSetting=%s durationStr=%s endDateTime=%s (seg %.0f..%.0f)",
                    i + 1, req["barSizeSetting"], req["durationStr"], req["endDateTime"],
                    req["seg_start_ts"], req["seg_end_ts"],
                )
            print(
                f"[bars_skip_ib] symbol={sym} period={per} start_ts={start_ts:.0f} end_ts={end_ts:.0f} "
                f"num_chunks={len(plan)} (preview only, no fetch)"
            )
            for i, req in enumerate(plan):
                print(
                    f"  #{i+1}: barSizeSetting={req['barSizeSetting']} durationStr={req['durationStr']} "
                    f"endDateTime={req['endDateTime']} seg_ts={req['seg_start_ts']:.0f}..{req['seg_end_ts']:.0f}"
                )
            return {"ok": True, "count": 0, "message": "Fetch skipped (bars_skip_ib / BIFROST_BARS_SKIP_IB, test mode)."}
        try:
            await ib_client.ensure_connected()
        except Exception as e:
            return {"ok": False, "error": f"Failed to connect to IB: {e}"}
        interval_sec = float(api_interval_sec) if api_interval_sec is not None and api_interval_sec > 0 else None
        bars = await ib_client.fetch_bars_range(symbol=sym, period=per, start_ts=start_ts, end_ts=end_ts, interval_sec=interval_sec)
        if not bars:
            return {"ok": True, "count": 0, "message": "IB returned no data for this range."}
        if not control_via_db:
            return {"ok": False, "error": "PostgreSQL is required to write bar tables."}
        if not write_stock_bars(control_via_db, sym, per, bars):
            return {"ok": False, "error": "Failed to write bar tables."}
        return {"ok": True, "count": len(bars), "message": f"Backfilled {len(bars)} bar(s)."}
    except IBConnectionDroppedError:
        raise
    except Exception as e:
        logger.warning("run_one_backfill failed: %s", e, exc_info=True)
        return {"ok": False, "error": str(e)}
