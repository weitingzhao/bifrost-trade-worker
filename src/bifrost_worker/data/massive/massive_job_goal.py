"""Human-readable English summaries for ``job_massive_backfill`` rows (Celery / Ops UI).

When adding a new ``run_massive_job`` ``kind`` / ``payload.mode``, update :func:`describe_massive_job_goal`
and extend tests. Matrix SSOT: ``src/massive/run_massive_job_manifest.py`` (``RUN_MASSIVE_JOB_MATRIX``).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

GOAL_MAX_LEN = 480


def _normalize_payload(payload: Any) -> Dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        s = payload.strip()
        if not s:
            return {}
        try:
            out = json.loads(s)
            return out if isinstance(out, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _trunc(text: str, max_len: int = GOAL_MAX_LEN) -> str:
    t = (text or "").strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


def _str_field(d: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _symbols_snippet(d: Dict[str, Any], max_items: int = 4) -> str:
    raw = d.get("symbols")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()[:80]
    if not isinstance(raw, list) or not raw:
        return ""
    items = [str(x).strip() for x in raw if x and str(x).strip()]
    if not items:
        return ""
    head = items[:max_items]
    extra = len(items) - len(head)
    out = ", ".join(head)
    if extra > 0:
        out += f" (+{extra} more)"
    return _trunc(out, 120)


def describe_massive_job_goal(kind: str, payload: Any) -> str:
    """Return a short English description of what this Massive job intends to do."""
    k = (kind or "").strip().lower()
    p = _normalize_payload(payload)
    mode = _str_field(p, "mode", "snapshot_type").lower() or ""

    # --- feed_options_aggregate (incl. legacy "aggregates") ---
    if k in ("feed_options_aggregate", "aggregates"):
        u = _str_field(p, "underlying", "symbol")
        ot = _str_field(p, "options_ticker")
        if mode == "open_close":
            ds = _str_field(p, "date")
            parts = ["Option open/close"]
            if ot:
                parts.append(ot)
            if ds:
                parts.append(ds)
            return _trunc(" · ".join(parts))

        if mode == "prev":
            return _trunc(f"Previous-day bar for {ot}" if ot else "Previous-day option bar (Massive prev)")

        if mode == "option_day_pool_row_gap":
            parts = ["option_day row gap fill (daily aggs)"]
            if u:
                parts.append(f"underlying {u}")
            rl = p.get("row_lookback_days")
            if rl is not None:
                parts.append(f"lookback {rl}d")
            mc = p.get("max_contracts")
            if mc is not None:
                parts.append(f"max_contracts {mc}")
            if p.get("chunk_size") is not None:
                parts.append(f"chunk_size {p.get('chunk_size')}")
            ci = p.get("fan_out_chunk_index")
            ct = p.get("fan_out_chunks_total")
            if ci is not None and ct is not None:
                parts.append(f"chunk {ci}/{ct}")
            exp = _str_field(p, "expiration_date")
            if exp:
                parts.append(f"expiry {exp}")
            if p.get("row_gap_targets") and isinstance(p.get("row_gap_targets"), list):
                n = len(p["row_gap_targets"])
                parts.append(f"explicit_targets {n}")
            return _trunc(" · ".join(parts))

        if mode == "option_day_pool_column_fill":
            parts = ["option_day column fill (open-close / VWAP patch)"]
            if u:
                parts.append(f"underlying {u}")
            return _trunc(" · ".join(parts))

        if mode == "option_min_pool_row_gap":
            parts = ["option_min row gap (intraday aggs)"]
            if u:
                parts.append(f"underlying {u}")
            per = _str_field(p, "period")
            if per:
                parts.append(per)
            return _trunc(" · ".join(parts))

        if mode == "option_min_pool_column_fill":
            parts = ["option_min column fill"]
            if u:
                parts.append(f"underlying {u}")
            per = _str_field(p, "period")
            if per:
                parts.append(per)
            return _trunc(" · ".join(parts))

        if mode == "option_snapshots_pool_contract_fill":
            parts = ["option_snapshots per-contract column fill"]
            if u:
                parts.append(f"underlying {u}")
            return _trunc(" · ".join(parts))

        if mode == "custom_bars":
            parts = ["Custom option/stock bars (aggregates)"]
            if ot:
                parts.append(ot)
            if u:
                parts.append(f"underlying {u}")
            return _trunc(" · ".join(parts))

        return _trunc(f"Options aggregates · mode={mode or '?'}" + (f" · {u}" if u else ""))

    # --- feed_option_snapshots ---
    if k in ("feed_option_snapshots", "snapshot"):
        u = _str_field(p, "underlying")
        exp = _str_field(p, "expiration_date")
        base = f"Option chain snapshots · {mode or 'chain'}"
        if u:
            base += f" · {u}"
        if exp:
            base += f" · exp {exp}"
        return _trunc(base)

    # --- feed_stocks_aggregate ---
    if k in ("feed_stocks_aggregate", "stock_ohlc_sync"):
        sy = _symbols_snippet(p)
        t = _str_field(p, "ticker")
        if mode == "custom_bars":
            return _trunc(f"Stock custom bars · {sy or t or 'symbols'}")
        if mode == "daily_market_summary":
            return _trunc("Stock daily market summary")
        if mode == "daily_ticker_summary":
            return _trunc(f"Stock daily ticker summary · {t or sy or '?'}")
        if mode == "previous_day_bar":
            return _trunc(f"Stock previous-day bar · {t or sy or '?'}")
        return _trunc(f"Stock aggregates · mode={mode or '?'} · {sy or t or ''}".strip())

    # --- feed_option_contracts ---
    if k in ("feed_option_contracts", "contracts"):
        u = _str_field(p, "underlying")
        ot = _str_field(p, "options_ticker")
        exp = _str_field(p, "expiration_date")
        col = _str_field(p, "column")
        if mode == "list":
            return _trunc(f"Option contracts list · {u or '?'}")
        if mode == "detail":
            return _trunc(f"Option contract detail · {ot or u or '?'}")
        if mode == "reference_upsert":
            return _trunc(f"Reference contracts upsert · {u or '?'} {exp}".strip())
        if mode == "nullable_column_backfill":
            return _trunc(f"Nullable column backfill · {u or '?'} {col}".strip())
        return _trunc(f"Option contracts · mode={mode or '?'} · {u or ot or ''}".strip())

    # --- feed_options_trades_quotes ---
    if k in ("feed_options_trades_quotes", "trades_quotes"):
        ot = _str_field(p, "options_ticker")
        u = _str_field(p, "underlying")
        if mode == "last_trade":
            return _trunc(f"Option last trade · {ot or u or '?'}")
        if mode == "quotes":
            return _trunc(f"Option quotes · {ot or u or '?'}")
        if mode == "trades":
            return _trunc(f"Option trades · {ot or u or '?'}")
        return _trunc(f"Options trades/quotes · mode={mode or '?'} · {ot or u or ''}".strip())

    # --- oi ---
    if k == "oi":
        td = _str_field(p, "trade_date")
        sy = _symbols_snippet(p)
        return _trunc(f"Watchlist EOD open interest · {td or 'latest'} · {sy or 'symbols'}")

    # --- pipeline / maintenance ---
    if k == "eod_pipeline":
        return "EOD pipeline (watchlist OI + max pain, etc.)"
    if k == "report_option_max_pain":
        sy = _symbols_snippet(p)
        td = _str_field(p, "trade_date")
        bits = ["Max pain report"]
        if td:
            bits.append(td)
        if sy:
            bits.append(sy)
        return _trunc(" · ".join(bits))
    if k == "reconcile":
        return "Reconcile watchlist vs DB OI counts"
    if k == "trim_jobs":
        return "Trim job_massive_backfill history"

    # --- stocks fundamentals v1 (SEPA raw tables) ---
    if k == "feed_stocks_income_statements":
        return _trunc(f"Income statements v1 · {_symbols_snippet(p) or 'symbols'}")
    if k == "feed_stocks_balance_sheets":
        return _trunc(f"Balance sheets v1 · {_symbols_snippet(p) or 'symbols'}")
    if k == "feed_stocks_cash_flows":
        return _trunc(f"Cash flow statements v1 · {_symbols_snippet(p) or 'symbols'}")
    if k == "feed_stocks_ratios":
        return _trunc(f"Ratios ingest · {_symbols_snippet(p) or 'symbols'}")
    if k == "feed_stocks_short_interest":
        return _trunc(f"Short interest · {_symbols_snippet(p) or 'symbols'}")
    if k == "feed_stocks_short_volume":
        return _trunc(f"Short volume · {_symbols_snippet(p) or 'symbols'}")

    # --- corporate action ---
    if k in ("feed_stocks_corporate_action", "corporate_action"):
        sym = _str_field(p, "symbol")
        return _trunc(f"Corporate actions sync · {sym or 'symbol?'}")

    # --- ticker reference (stocks_massive*) ---
    if k in (
        "feed_stocks_tickers_reference_universe",
        "ticker_reference_universe",
        "stock_reference_universe",
    ):
        return "Ticker reference universe sync (full list)"
    if k in ("feed_stocks_tickers_types", "ticker_reference_ticker_types", "ticker_reference_instrument_types", "stock_reference_instrument_types"):
        return "Ticker types dictionary sync"
    if k in ("feed_stocks_tickers_overview", "ticker_reference_overview", "stock_reference_overview"):
        return _trunc(f"Ticker overview · mode={mode or 'all'} · {_symbols_snippet(p) or 'symbols'}")
    if k in ("feed_stocks_tickers_related", "ticker_reference_related", "stock_reference_related"):
        return _trunc(f"Ticker related companies · mode={mode or 'all'} · {_symbols_snippet(p) or 'symbols'}")

    # --- fallback ---
    parts: List[str] = [f"Massive job · {k or 'unknown'}"]
    if mode:
        parts.append(f"mode={mode}")
    u = _str_field(p, "underlying", "symbol", "ticker")
    if u:
        parts.append(u)
    sy = _symbols_snippet(p)
    if sy:
        parts.append(sy)
    return _trunc(" · ".join(parts))
