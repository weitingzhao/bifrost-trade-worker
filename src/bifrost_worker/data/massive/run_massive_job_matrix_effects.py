"""Per-row side effects for ``run_massive_job`` matrix: Massive REST, PostgreSQL, Redis.

Documented for Ops UI / Support Tasks — keep in sync with ``tasks.py`` implementations.
English strings (monitor UI convention).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class MatrixRowEffects:
    """External I/O summary for one matrix row (kind + mode)."""

    feed_apis: Tuple[str, ...]
    db_tables: Tuple[str, ...]
    redis_nodes: Tuple[str, ...]


def _e(
    apis: Tuple[str, ...],
    tables: Tuple[str, ...],
    redis: Tuple[str, ...],
) -> MatrixRowEffects:
    return MatrixRowEffects(feed_apis=apis, db_tables=tables, redis_nodes=redis)


# Key: (kind, mode or "" for null mode)
_REGISTRY: Dict[Tuple[str, str], MatrixRowEffects] = {
    # — feed_option_snapshots
    ("feed_option_snapshots", "chain"): _e(
        ("GET /v3/snapshot/options/{underlying} (paginated)",),
        (
            "option_contracts",
            "option_snapshots",
            "option_snapshots_latest (REFRESH)",
        ),
        ("—",),
    ),
    ("feed_option_snapshots", "contract"): _e(
        ("GET /v3/snapshot/options/{underlying}/{optionContract}",),
        ("option_contracts", "option_snapshots", "option_snapshots_latest (REFRESH)"),
        ("—",),
    ),
    ("feed_option_snapshots", "unified"): _e(
        ("GET /v3/snapshot",),
        ("option_contracts", "option_snapshots", "option_snapshots_latest (REFRESH)"),
        ("—",),
    ),
    # — feed_stocks_aggregate
    ("feed_stocks_aggregate", "custom_bars"): _e(
        ("GET /v2/aggs/ticker/{ticker}/range/... (stock or index)",),
        ("stock_day", "stock_min (per config)"),
        ("—",),
    ),
    ("feed_stocks_aggregate", "daily_market_summary"): _e(
        ("GET /v2/aggs/grouped/locale/us/market/{date}",),
        ("stock_day",),
        ("—",),
    ),
    ("feed_stocks_aggregate", "daily_ticker_summary"): _e(
        ("GET /v1/open-close/{ticker}/{date}",),
        ("stock_day",),
        ("—",),
    ),
    ("feed_stocks_aggregate", "previous_day_bar"): _e(
        ("GET /v2/aggs/ticker/{ticker}/prev",),
        ("stock_day",),
        ("—",),
    ),
    # — feed_options_aggregate
    ("feed_options_aggregate", "open_close"): _e(
        ("GET /v1/open-close/{optionsTicker}/{date}",),
        ("option_day",),
        ("—",),
    ),
    ("feed_options_aggregate", "option_day_pool_row_gap"): _e(
        ("GET /v2/aggs/ticker/{optionsTicker}/range/... (day)",),
        ("option_day",),
        ("—",),
    ),
    ("feed_options_aggregate", "option_day_pool_column_fill"): _e(
        ("GET /v1/open-close/{optionsTicker}/{date}", "GET /v2/aggs/ticker/.../day (vwap)"),
        ("option_day",),
        ("—",),
    ),
    ("feed_options_aggregate", "prev"): _e(
        ("GET /v2/aggs/ticker/{optionsTicker}/prev",),
        ("option_day",),
        ("—",),
    ),
    ("feed_options_aggregate", "option_min_pool_row_gap"): _e(
        ("GET /v2/aggs/ticker/{optionsTicker}/range/... (minute)",),
        ("option_min",),
        ("—",),
    ),
    ("feed_options_aggregate", "option_min_pool_column_fill"): _e(
        ("GET /v2/aggs/ticker/{optionsTicker}/range/... (minute)",),
        ("option_min",),
        ("—",),
    ),
    ("feed_options_aggregate", "option_snapshots_pool_contract_fill"): _e(
        ("GET /v3/snapshot/options/{underlying} (per contract gap fill)",),
        ("option_snapshots",),
        ("—",),
    ),
    ("feed_options_aggregate", "custom_bars"): _e(
        ("GET /v2/aggs/ticker/{optionsTicker}/range/... (option bars)",),
        ("option_min", "option_day"),
        ("—",),
    ),
    # — oi
    ("oi", "watchlist_eod"): _e(
        ("GET /v3/snapshot/options/{underlying} (paginated chain for OI)",),
        ("option_open_interest_daily",),
        ("—",),
    ),
    # — pipelines (no Massive REST for report_option_max_pain / trim)
    ("eod_pipeline", ""): _e(
        (
            "GET /v3/snapshot/options/{underlying} (OI path)",
            "(internal) report_option_max_pain uses DB only",
        ),
        ("option_open_interest_daily", "report_option_max_pain_daily"),
        ("—",),
    ),
    ("report_option_max_pain", ""): _e(
        ("(no REST — reads option_open_interest_daily)",),
        ("report_option_max_pain_daily",),
        ("—",),
    ),
    ("reconcile", ""): _e(
        ("GET /v3/snapshot/options/{underlying} (count vs DB)",),
        ("(read-only compare to option_open_interest_daily)",),
        ("—",),
    ),
    ("trim_jobs", ""): _e(
        ("—",),
        ("job_massive_backfill (DELETE old rows)",),
        ("—",),
    ),
    ("feed_stocks_corporate_action", ""): _e(
        (
            "GET /stocks/v1/dividends?ticker=…",
            "GET /stocks/v1/splits?ticker=…",
            "GET /v3/reference/ipos?ticker=…",
            "GET /v3/reference/tickers/{ticker}/events",
        ),
        ("massive_corporate_action",),
        ("—",),
    ),
    ("feed_stocks_income_statements", ""): _e(
        ("GET /stocks/financials/v1/income-statements",),
        ("stock_income_statements",),
        ("—",),
    ),
    ("feed_stocks_balance_sheets", ""): _e(
        ("GET /stocks/financials/v1/balance-sheets",),
        ("stock_balance_sheets",),
        ("—",),
    ),
    ("feed_stocks_cash_flows", ""): _e(
        ("GET /stocks/financials/v1/cash-flow-statements",),
        ("stock_cash_flows",),
        ("—",),
    ),
    ("feed_stocks_ratios", ""): _e(
        ("GET /stocks/financials/v1/ratios (or GET /vX/reference/financials fallback)",),
        ("stock_ratios",),
        ("—",),
    ),
    ("feed_stocks_short_interest", ""): _e(
        ("GET /stocks/v1/short-interest",),
        ("stock_short_interest",),
        ("—",),
    ),
    ("feed_stocks_short_volume", ""): _e(
        ("GET /stocks/v1/short-volume",),
        ("stock_short_volume",),
        ("—",),
    ),
    # — feed_option_contracts
    ("feed_option_contracts", "list"): _e(
        ("GET /v3/reference/options/contracts",),
        ("(typically read-only result payload)",),
        ("—",),
    ),
    ("feed_option_contracts", "detail"): _e(
        ("GET /v3/reference/options/contracts/{options_ticker}",),
        ("(read-only job result)",),
        ("—",),
    ),
    ("feed_option_contracts", "reference_upsert"): _e(
        ("GET /v3/reference/options/contracts (paginated per expiry)",),
        ("option_contracts", "option_expiration_cache (if used)"),
        ("—",),
    ),
    ("feed_option_contracts", "nullable_column_backfill"): _e(
        ("GET /v3/reference/options/contracts/{options_ticker} (per row)",),
        ("option_contracts (UPDATE exercise_style / shares_per_contract)",),
        ("—",),
    ),
    # — feed_options_trades_quotes
    ("feed_options_trades_quotes", "last_trade"): _e(
        ("GET /v2/last/trade/{optionsTicker}",),
        ("(read-only job result)",),
        ("—",),
    ),
    ("feed_options_trades_quotes", "quotes"): _e(
        ("GET /v3/quotes/{optionsTicker}",),
        ("(read-only job result)",),
        ("—",),
    ),
    ("feed_options_trades_quotes", "trades"): _e(
        ("GET /v3/trades/{optionsTicker}",),
        ("(read-only job result)",),
        ("—",),
    ),
    # — ticker / stock reference
    ("feed_stocks_tickers_reference_universe", ""): _e(
        ("GET /v3/reference/tickers (cursor pagination)",),
        ("tickers", "job_ticker_reference_state"),
        ("massive:ingestor:cache:search:* (invalidate_search_caches)",),
    ),
    ("feed_stocks_tickers_types", ""): _e(
        ("GET /v3/reference/tickers/types",),
        ("ticker_types",),
        ("massive:ingestor:cache:ticker_types:{locale}:{asset_class}",),
    ),
    ("feed_stocks_tickers_overview", "all"): _e(
        ("GET /v3/reference/tickers/{ticker} (per ticker)",),
        ("tickers", "ticker_overview"),
        (
            "massive:ingestor:cache:ticker:{SYMBOL}",
            "massive:ingestor:cache:search:*",
        ),
    ),
    ("feed_stocks_tickers_overview", "symbols"): _e(
        ("GET /v3/reference/tickers/{ticker}",),
        ("tickers", "ticker_overview"),
        (
            "massive:ingestor:cache:ticker:{SYMBOL}",
            "massive:ingestor:cache:search:*",
        ),
    ),
    ("feed_stocks_tickers_overview", "missing"): _e(
        ("GET /v3/reference/tickers/{ticker}",),
        ("tickers", "ticker_overview"),
        (
            "massive:ingestor:cache:ticker:{SYMBOL}",
            "massive:ingestor:cache:search:*",
        ),
    ),
    ("feed_stocks_tickers_overview", "stale"): _e(
        ("GET /v3/reference/tickers/{ticker}",),
        ("tickers", "ticker_overview"),
        (
            "massive:ingestor:cache:ticker:{SYMBOL}",
            "massive:ingestor:cache:search:*",
        ),
    ),
    ("feed_stocks_tickers_related", "all"): _e(
        ("GET /v1/related-companies/{ticker}",),
        ("ticker_related_tickers",),
        (
            "massive:ingestor:cache:ticker:{SYMBOL}",
            "massive:ingestor:cache:peers:{SYMBOL}",
        ),
    ),
    ("feed_stocks_tickers_related", "symbols"): _e(
        ("GET /v1/related-companies/{ticker}",),
        ("ticker_related_tickers",),
        (
            "massive:ingestor:cache:ticker:{SYMBOL}",
            "massive:ingestor:cache:peers:{SYMBOL}",
        ),
    ),
    ("feed_stocks_tickers_related", "missing"): _e(
        ("GET /v1/related-companies/{ticker}",),
        ("ticker_related_tickers",),
        (
            "massive:ingestor:cache:ticker:{SYMBOL}",
            "massive:ingestor:cache:peers:{SYMBOL}",
        ),
    ),
    ("feed_stocks_tickers_related", "stale"): _e(
        ("GET /v1/related-companies/{ticker}",),
        ("ticker_related_tickers",),
        (
            "massive:ingestor:cache:ticker:{SYMBOL}",
            "massive:ingestor:cache:peers:{SYMBOL}",
        ),
    ),
}


def effects_for_matrix_row(kind: str, mode: Optional[str]) -> MatrixRowEffects:
    """Return documented Feed API / DB / Redis side effects for one matrix row."""
    k = (kind or "").strip()
    if k == "corporate_action":
        k = "feed_stocks_corporate_action"
    m = (mode or "").strip() if mode is not None else ""
    key = (k, m)
    if key in _REGISTRY:
        return _REGISTRY[key]
    # Fallback — should not happen if registry matches RUN_MASSIVE_JOB_MATRIX
    return _e(
        ("(unmapped — see src/massive/tasks.py)",),
        ("job_massive_backfill",),
        ("—",),
    )


def matrix_row_effects_to_api(e: MatrixRowEffects) -> Dict[str, object]:
    return {
        "feed_apis": list(e.feed_apis),
        "db_tables": list(e.db_tables),
        "redis_nodes": list(e.redis_nodes),
    }
