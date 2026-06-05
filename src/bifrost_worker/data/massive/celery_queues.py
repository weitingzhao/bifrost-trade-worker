"""Celery queue names for Massive jobs.

Options sync uses ``options_massive`` / ``options_massive_high``. Ticker reference jobs use
``stocks_massive`` / ``stocks_massive_high`` so workers can scale or isolate pipelines without
sharing the same Redis list as options.
"""

from __future__ import annotations

from typing import Final

from bifrost_worker.celery.celery_queue_names import (
    BROKER_QUEUE_OPTIONS_MASSIVE,
    BROKER_QUEUE_OPTIONS_MASSIVE_HIGH,
    BROKER_QUEUE_STOCKS_MASSIVE,
    BROKER_QUEUE_STOCKS_MASSIVE_HIGH,
)

# Full tickers universe sync (Massive ref tickers list). Canonical ``feed_stocks_tickers_reference_universe``;
# legacy ``ticker_reference_universe`` / ``stock_reference_universe`` still route here.
FEED_STOCKS_TICKERS_REFERENCE_UNIVERSE_KINDS: Final[frozenset[str]] = frozenset(
    {
        "feed_stocks_tickers_reference_universe",
        "ticker_reference_universe",
        "stock_reference_universe",
    }
)

# Ticker types dictionary (GET /v3/reference/tickers/types). Canonical ``feed_stocks_tickers_types``;
# legacy ``ticker_reference_ticker_types`` / ``*_instrument_types`` still route here.
FEED_STOCKS_TICKERS_TYPES_KINDS: Final[frozenset[str]] = frozenset(
    {
        "feed_stocks_tickers_types",
        "ticker_reference_ticker_types",
        "ticker_reference_instrument_types",
        "stock_reference_instrument_types",
    }
)

# Kinds routed to stocks_massive* (see run_massive_job + insert_job_massive_backfill).
TICKER_REFERENCE_KINDS: Final[frozenset[str]] = frozenset(
    {
        "feed_stocks_tickers_reference_universe",
        "feed_stocks_tickers_overview",
        "ticker_reference_overview",
        "feed_stocks_tickers_related",
        "ticker_reference_related",
        "feed_stocks_tickers_types",
        "stock_reference_overview",
        "stock_reference_related",
    }
)

# Stock OHLC → PostgreSQL (Massive REST); same worker pool as reference for isolation from options.
FEED_STOCKS_AGGREGATE_KINDS: Final[frozenset[str]] = frozenset(
    {"feed_stocks_aggregate", "stock_ohlc_sync"}
)

# Stocks corporate actions (dividends / splits / IPOs / ticker events → massive_corporate_action).
FEED_STOCKS_CORPORATE_ACTION_KINDS: Final[frozenset[str]] = frozenset(
    {
        "feed_stocks_corporate_action",
        "corporate_action",
    }
)

# SEPA fundamentals raw ingest (Massive Stocks REST → PostgreSQL).
FEED_STOCKS_FINANCIALS_KINDS: Final[frozenset[str]] = frozenset(
    {
        "feed_stocks_income_statements",
        "feed_stocks_balance_sheets",
        "feed_stocks_cash_flows",
        "feed_stocks_ratios",
        "feed_stocks_short_interest",
        "feed_stocks_short_volume",
    }
)

STOCK_REFERENCE_KINDS = TICKER_REFERENCE_KINDS  # backward compat for reader/tests

MASSIVE_STOCKS_QUEUE_KINDS: Final[frozenset[str]] = (
    TICKER_REFERENCE_KINDS
    | FEED_STOCKS_AGGREGATE_KINDS
    | FEED_STOCKS_TICKERS_REFERENCE_UNIVERSE_KINDS
    | FEED_STOCKS_TICKERS_TYPES_KINDS
    | FEED_STOCKS_CORPORATE_ACTION_KINDS
    | FEED_STOCKS_FINANCIALS_KINDS
)

# Option contract OHLC / pool fills on Massive options queues (``options_massive`` / ``options_massive_high``).
FEED_OPTIONS_AGGREGATE_KINDS: Final[frozenset[str]] = frozenset(
    {"feed_options_aggregate", "aggregates"}
)

# Options last trade / quotes / historical trades proxy jobs (same queues as other option Massive work).
FEED_OPTIONS_TRADES_QUOTES_KINDS: Final[frozenset[str]] = frozenset(
    {"feed_options_trades_quotes", "trades_quotes"}
)

# Option reference contracts (list/detail/reference_upsert/nullable backfill) on options queues.
FEED_OPTION_CONTRACTS_KINDS: Final[frozenset[str]] = frozenset(
    {"feed_option_contracts", "contracts"}
)


def celery_queue_for_massive_job(kind: str, *, priority_high: bool) -> str:
    """Return broker queue for ``run_massive_job`` given job kind and API priority."""
    k = (kind or "").strip().lower()
    if k in MASSIVE_STOCKS_QUEUE_KINDS:
        return BROKER_QUEUE_STOCKS_MASSIVE_HIGH if priority_high else BROKER_QUEUE_STOCKS_MASSIVE
    return BROKER_QUEUE_OPTIONS_MASSIVE_HIGH if priority_high else BROKER_QUEUE_OPTIONS_MASSIVE
