"""Ops Celery capabilities payload (matrix + canonical queues)."""

from __future__ import annotations


def test_build_celery_capabilities_payload_has_matrix_and_canonical_queues() -> None:
    import bifrost_worker.data.bars.tasks  # noqa: F401
    import bifrost_worker.data.massive.tasks  # noqa: F401
    from bifrost_api.ops.services.celery_capabilities import build_celery_capabilities_payload
    from bifrost_core.config.startup import read_config
    from bifrost_worker.data.massive.beat_schedule_public import beat_tasks_payload_for_capabilities
    from bifrost_worker.celery.celery_app import app
    from bifrost_worker.celery.celery_queue_names import load_canonical_broker_queue_names

    cfg, _ = read_config()
    out = build_celery_capabilities_payload(app)
    assert out["ok"] is True
    assert out["canonical_broker_queues"] == list(load_canonical_broker_queue_names(cfg))
    assert out["broker_queue_labels"].get("options_massive") == "Options Massive"
    assert out["broker_queue_labels"].get("stocks_ib") == "Stocks IB"
    assert len(out["run_massive_job_matrix"]) >= 1
    assert out["run_massive_job_matrix"][0]["broker_queue_standard"]
    assert out["beat_tasks"] == beat_tasks_payload_for_capabilities()
    assert out["beat_tasks"] and len(out["beat_tasks"]) == len(beat_tasks_payload_for_capabilities())
    assert out["registered_tasks"] and out["count"] == len(out["registered_tasks"])
    first = out["registered_tasks"][0]
    assert first["name"].startswith("src.")
    assert first["task_route_default_queue"] == first["default_queue"]


def test_ops_celery_config_validation_errors_detects_unknown_queue() -> None:
    from bifrost_worker.celery.celery_queue_names import ops_celery_config_validation_errors

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
