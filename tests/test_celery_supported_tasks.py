"""Celery app task registration after Massive package removal (Wave 7-B)."""

from __future__ import annotations

import importlib
import pytest


def test_massive_package_is_removed() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("bifrost_worker.data.massive")
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("bifrost_worker.data.massive.tasks")


def test_celery_app_registers_bars_only() -> None:
    import bifrost_worker.data.bars.tasks  # noqa: F401
    from bifrost_worker.celery.celery_app import app

    names = set(app.tasks.keys())
    assert "src.bars.tasks.backfill_bars" in names
    assert "src.massive.tasks.run_massive_job" not in names
    assert not any(n.startswith("src.massive.") for n in names)

    routes = app.conf.task_routes or {}
    assert routes.get("src.bars.tasks.backfill_bars", {}).get("queue") == "stocks_ib"
    assert "src.massive.tasks.run_massive_job" not in routes
    assert app.conf.beat_schedule == {}
    assert app.conf.task_default_queue == "stocks_ib"
