"""Tests for Ops Celery supported-tasks payload builder."""

from __future__ import annotations

from unittest.mock import MagicMock


def test_build_supported_tasks_payload_filters_and_queues() -> None:
    from bifrost_api.ops.services.celery_supported_tasks import build_supported_tasks_payload

    app = MagicMock()
    app.tasks.keys.return_value = [
        "celery.backend_cleanup",
        "src.bars.tasks.backfill_bars",
        "src.massive.tasks.run_massive_job",
        "kombu.foo",
    ]
    app.conf.task_default_queue = "stocks_ib"
    app.conf.task_routes = {
        "src.bars.tasks.backfill_bars": {"queue": "stocks_ib"},
        "src.massive.tasks.run_massive_job": {"queue": "options_massive"},
    }

    out = build_supported_tasks_payload(app)
    assert out["ok"] is True
    assert out["count"] == 2
    by_name = {t["name"]: t for t in out["tasks"]}
    assert by_name["src.bars.tasks.backfill_bars"]["default_queue"] == "stocks_ib"
    assert by_name["src.bars.tasks.backfill_bars"]["task_route_default_queue"] == "stocks_ib"
    assert by_name["src.massive.tasks.run_massive_job"]["default_queue"] == "options_massive"
    assert by_name["src.massive.tasks.run_massive_job"]["task_route_default_queue"] == "options_massive"


def test_build_supported_tasks_payload_real_app_lists_project_tasks() -> None:
    """Integration: same Celery app as workers registers src.* tasks after includes load."""
    import bifrost_worker.data.bars.tasks  # noqa: F401
    import bifrost_worker.data.massive.tasks  # noqa: F401
    from bifrost_api.ops.services.celery_supported_tasks import build_supported_tasks_payload
    from bifrost_worker.celery.celery_app import app

    out = build_supported_tasks_payload(app)
    assert out["ok"] is True
    names = {t["name"] for t in out["tasks"]}
    assert "src.bars.tasks.backfill_bars" in names
    assert "src.massive.tasks.run_massive_job" in names
    assert len(out["tasks"]) >= 7
