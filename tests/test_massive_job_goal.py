"""Tests for slim ``describe_massive_job_goal`` after Massive Celery retirement."""

from __future__ import annotations

from bifrost_worker.data.massive.massive_job_goal import GOAL_MAX_LEN, describe_massive_job_goal


def test_describe_mentions_kind_and_plugin() -> None:
    g = describe_massive_job_goal("feed_option_snapshots", {"mode": "chain"})
    assert "feed_option_snapshots" in g
    assert "market-data plugin" in g
    assert len(g) <= GOAL_MAX_LEN


def test_empty_kind() -> None:
    g = describe_massive_job_goal("", None)
    assert "unknown" in g
