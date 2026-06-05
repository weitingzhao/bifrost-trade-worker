"""Tests for :func:`src.workers.celery_queue_names.build_celery_worker_pool_argv`."""

from __future__ import annotations

import pytest


def test_no_instance_always_solo() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    assert build_celery_worker_pool_argv(
        instance_profile_resolved=False,
        profile_key=None,
        worker_profile_entry=None,
        ops_celery={},
    ) == ["--pool=solo"]


def test_stocks_ib_profile_always_solo() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    assert build_celery_worker_pool_argv(
        instance_profile_resolved=True,
        profile_key="stocks_ib",
        worker_profile_entry={"queues": ["stocks_ib"], "pool": "prefork"},
        ops_celery={"massive_worker_concurrency": 8},
    ) == ["--pool=solo"]


def test_massive_default_prefork_and_global_concurrency() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    out = build_celery_worker_pool_argv(
        instance_profile_resolved=True,
        profile_key="options_massive",
        worker_profile_entry={"queues": ["options_massive"]},
        ops_celery={"massive_worker_concurrency": 6},
    )
    assert out == ["--pool=prefork", "--concurrency=6"]


def test_massive_profile_concurrency_override() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    out = build_celery_worker_pool_argv(
        instance_profile_resolved=True,
        profile_key="stocks_massive",
        worker_profile_entry={"queues": ["stocks_massive"], "concurrency": 2},
        ops_celery={"massive_worker_concurrency": 99},
    )
    assert out == ["--pool=prefork", "--concurrency=2"]


def test_massive_explicit_solo() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    assert build_celery_worker_pool_argv(
        instance_profile_resolved=True,
        profile_key="options_massive_high",
        worker_profile_entry={"queues": ["options_massive_high"], "pool": "solo"},
        ops_celery={},
    ) == ["--pool=solo"]
