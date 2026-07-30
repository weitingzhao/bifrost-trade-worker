"""P8 Critical C2: Massive queue disable gate."""

from __future__ import annotations

from bifrost_worker.data.massive.celery_queues import (
    MASSIVE_QUEUES_DISABLED,
    MASSIVE_QUEUES_DISABLED_ERROR,
    massive_enqueue_refused_payload,
    massive_insert_failed_payload,
)
from bifrost_worker.data.massive.pending_dispatch import dispatch_pending_massive_topup
from bifrost_worker.data.massive.vendor.reader import insert_job_massive_backfill


def test_massive_queues_disabled_flag() -> None:
    assert MASSIVE_QUEUES_DISABLED is True
    assert "market-data plugin" in MASSIVE_QUEUES_DISABLED_ERROR


def test_massive_insert_refused_when_disabled() -> None:
    jid, dedup = insert_job_massive_backfill(
        {"sink": "postgres", "postgres": {"host": "localhost"}},
        "feed_option_snapshots",
        {"symbols": ["AAPL"]},
    )
    assert jid is None
    assert dedup is False


def test_pending_dispatch_noop_when_disabled() -> None:
    n = dispatch_pending_massive_topup(
        {"sink": "postgres", "postgres": {"host": "localhost"}},
        "options_massive",
    )
    assert n == 0


def test_refused_payload_helpers() -> None:
    p = massive_enqueue_refused_payload()
    assert p["ok"] is False
    assert p["reason"] == "massive_queues_disabled"
    assert massive_insert_failed_payload()["error"] == MASSIVE_QUEUES_DISABLED_ERROR
