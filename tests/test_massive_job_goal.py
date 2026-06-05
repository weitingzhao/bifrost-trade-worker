"""Tests for ``describe_massive_job_goal`` (Ops / Celery UI job summaries)."""

from __future__ import annotations

import json

from bifrost_worker.data.massive.massive_job_goal import GOAL_MAX_LEN, describe_massive_job_goal


def test_option_day_pool_row_gap_with_fan_out():
    payload = {
        "mode": "option_day_pool_row_gap",
        "underlying": "NVDA",
        "row_lookback_days": 5,
        "max_contracts": 100,
        "chunk_size": 10,
        "fan_out_chunk_index": 2,
        "fan_out_chunks_total": 8,
    }
    g = describe_massive_job_goal("feed_options_aggregate", payload)
    assert "option_day row gap" in g
    assert "NVDA" in g
    assert "chunk 2/8" in g
    assert "lookback 5d" in g


def test_feed_option_snapshots():
    g = describe_massive_job_goal(
        "feed_option_snapshots",
        {"mode": "chain", "underlying": "NVDA", "expiration_date": "2026-05-15"},
    )
    assert "snapshots" in g.lower()
    assert "NVDA" in g


def test_feed_stocks_aggregate_custom_bars():
    g = describe_massive_job_goal(
        "feed_stocks_aggregate",
        {"mode": "custom_bars", "symbols": ["AAPL", "MSFT", "GOOG", "META", "AMZN"]},
    )
    assert "custom bars" in g.lower()
    assert "+2 more" in g or "AAPL" in g


def test_bad_json_payload_falls_back_to_kind_only():
    g = describe_massive_job_goal("feed_options_aggregate", "{not json")
    assert "Options aggregates" in g
    assert "mode=" in g


def test_unknown_kind_truncation_and_safe_fields():
    long_sym = "X" * 600
    payload = {"mode": "weird", "underlying": long_sym, "symbols": ["A", "B", "C", "D", "E"]}
    g = describe_massive_job_goal("totally_unknown_kind_xyz", payload)
    assert len(g) <= GOAL_MAX_LEN
    assert g.endswith("…")


def test_payload_as_json_string():
    payload = json.dumps({"mode": "open_close", "options_ticker": "O:NVDA260515C00100000", "date": "2026-04-18"})
    g = describe_massive_job_goal("feed_options_aggregate", payload)
    assert "open" in g.lower() or "Option" in g


def test_trim_jobs_and_reconcile_literals():
    assert "Trim" in describe_massive_job_goal("trim_jobs", {})
    assert "Reconcile" in describe_massive_job_goal("reconcile", {})


def test_ticker_reference_universe():
    g = describe_massive_job_goal("feed_stocks_tickers_reference_universe", {})
    assert "universe" in g.lower()
