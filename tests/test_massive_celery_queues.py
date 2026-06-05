"""Routing for Massive Celery jobs — queue selection by kind."""

from __future__ import annotations

from bifrost_worker.data.massive.celery_queues import (
    FEED_STOCKS_FINANCIALS_KINDS,
    FEED_STOCKS_TICKERS_REFERENCE_UNIVERSE_KINDS,
    FEED_STOCKS_TICKERS_TYPES_KINDS,
    celery_queue_for_massive_job,
)


def test_stock_kinds_use_massive_stocks_queues() -> None:
    for k in FEED_STOCKS_TICKERS_REFERENCE_UNIVERSE_KINDS:
        assert celery_queue_for_massive_job(k, priority_high=False) == "stocks_massive"
        assert celery_queue_for_massive_job(k, priority_high=True) == "stocks_massive_high"
    for k in FEED_STOCKS_TICKERS_TYPES_KINDS:
        assert celery_queue_for_massive_job(k, priority_high=False) == "stocks_massive"
        assert celery_queue_for_massive_job(k, priority_high=True) == "stocks_massive_high"


def test_options_kinds_use_massive_queues() -> None:
    assert celery_queue_for_massive_job("feed_option_snapshots", priority_high=False) == "options_massive"
    assert celery_queue_for_massive_job("feed_option_snapshots", priority_high=True) == "options_massive_high"
    assert celery_queue_for_massive_job("snapshot", priority_high=False) == "options_massive"
    assert celery_queue_for_massive_job("feed_options_aggregate", priority_high=False) == "options_massive"
    assert celery_queue_for_massive_job("aggregates", priority_high=False) == "options_massive"
    assert celery_queue_for_massive_job("feed_options_trades_quotes", priority_high=False) == "options_massive"
    assert celery_queue_for_massive_job("trades_quotes", priority_high=False) == "options_massive"
    assert celery_queue_for_massive_job("feed_option_contracts", priority_high=False) == "options_massive"
    assert celery_queue_for_massive_job("contracts", priority_high=False) == "options_massive"


def test_feed_stocks_aggregate_routes_to_stocks_massive_queues() -> None:
    assert celery_queue_for_massive_job("feed_stocks_aggregate", priority_high=False) == "stocks_massive"
    assert celery_queue_for_massive_job("feed_stocks_aggregate", priority_high=True) == "stocks_massive_high"
    assert celery_queue_for_massive_job("stock_ohlc_sync", priority_high=False) == "stocks_massive"


def test_feed_stocks_financials_kinds_route_to_stocks_massive_queues() -> None:
    for k in FEED_STOCKS_FINANCIALS_KINDS:
        assert celery_queue_for_massive_job(k, priority_high=False) == "stocks_massive"
        assert celery_queue_for_massive_job(k, priority_high=True) == "stocks_massive_high"
