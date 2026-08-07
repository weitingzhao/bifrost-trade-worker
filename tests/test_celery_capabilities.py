"""Canonical broker queue helpers (Massive queues removed in Wave 7-B)."""

from __future__ import annotations

from bifrost_worker.celery.celery_queue_names import (
    CANONICAL_BROKER_QUEUE_NAMES,
    BROKER_QUEUE_STOCKS_IB,
    load_canonical_broker_queue_names,
    ops_celery_config_validation_errors,
)


def test_canonical_broker_queues_stocks_ib_only() -> None:
    assert CANONICAL_BROKER_QUEUE_NAMES == (BROKER_QUEUE_STOCKS_IB,)
    assert load_canonical_broker_queue_names(None) == (BROKER_QUEUE_STOCKS_IB,)
    assert load_canonical_broker_queue_names({}) == (BROKER_QUEUE_STOCKS_IB,)


def test_ops_celery_config_validation_errors_detects_unknown_queue() -> None:
    bad = {
        "ops": {
            "worker_profiles": {
                "stocks_ib": {"label": "IB", "queues": ["stocks_ib"]},
            },
            "celery": {"canonical_queue_order": ["stocks_ib", "options_massive"]},
        }
    }
    errs = ops_celery_config_validation_errors(bad)
    assert any("options_massive" in e for e in errs)
