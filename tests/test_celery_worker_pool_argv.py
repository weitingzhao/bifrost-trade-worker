"""Tests for :func:`bifrost_worker.celery.celery_queue_names.build_celery_worker_pool_argv`."""

from __future__ import annotations


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
        ops_celery={"worker_concurrency": 8},
    ) == ["--pool=solo"]


def test_non_ib_default_prefork_and_global_concurrency() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    out = build_celery_worker_pool_argv(
        instance_profile_resolved=True,
        profile_key="data_jobs",
        worker_profile_entry={"queues": ["data_jobs"]},
        ops_celery={"worker_concurrency": 6},
    )
    assert out == ["--pool=prefork", "--concurrency=6"]


def test_legacy_massive_worker_concurrency_key_still_honored() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    out = build_celery_worker_pool_argv(
        instance_profile_resolved=True,
        profile_key="data_jobs",
        worker_profile_entry={"queues": ["data_jobs"]},
        ops_celery={"massive_worker_concurrency": 6},
    )
    assert out == ["--pool=prefork", "--concurrency=6"]


def test_profile_concurrency_override() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    out = build_celery_worker_pool_argv(
        instance_profile_resolved=True,
        profile_key="data_jobs",
        worker_profile_entry={"queues": ["data_jobs"], "concurrency": 2},
        ops_celery={"worker_concurrency": 99},
    )
    assert out == ["--pool=prefork", "--concurrency=2"]


def test_non_ib_explicit_solo() -> None:
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    assert build_celery_worker_pool_argv(
        instance_profile_resolved=True,
        profile_key="data_jobs",
        worker_profile_entry={"queues": ["data_jobs"], "pool": "solo"},
        ops_celery={},
    ) == ["--pool=solo"]
