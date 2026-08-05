"""Single source of truth for Celery Beat schedules (Massive-related tasks).

Used by ``src.workers.celery_app`` and ``GET /research/massive/celery-beat-schedule``.

P8 retirement: Massive ingest migrated to bifrost-platform-plugin-market-data
(plugin CronJobs). Beat schedule is permanently empty — no rollback list retained.
"""

from __future__ import annotations

from typing import Any, Dict, List

from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED

# Permanently empty — Polygon ingest owned by plugin-market-data CronJobs.
_MASSIVE_BEAT_SCHEDULE_SPEC_FULL: List[Dict[str, Any]] = []

MASSIVE_BEAT_SCHEDULE_SPEC: List[Dict[str, Any]] = (
    [] if MASSIVE_QUEUES_DISABLED else list(_MASSIVE_BEAT_SCHEDULE_SPEC_FULL)
)


def beat_tasks_payload_for_capabilities() -> List[Dict[str, str]]:
    """Rows for GET /ops/celery/capabilities ``beat_tasks`` (task path + note)."""
    out: List[Dict[str, str]] = []
    for spec in MASSIVE_BEAT_SCHEDULE_SPEC:
        out.append(
            {
                "name": str(spec["task"]),
                "note": str(spec.get("note", "")),
            }
        )
    return out


def build_celery_beat_schedule() -> Dict[str, Any]:
    """Return ``beat_schedule`` dict for ``app.conf.update(beat_schedule=...)``."""
    from celery.schedules import crontab

    out: Dict[str, Any] = {}
    for spec in MASSIVE_BEAT_SCHEDULE_SPEC:
        name = str(spec["name"])
        kw = dict(spec["crontab_kwargs"])
        out[name] = {
            "task": str(spec["task"]),
            "schedule": crontab(**kw),
        }
    return out


def public_celery_beat_schedule_response() -> Dict[str, Any]:
    """JSON-serializable payload for Research API (no Celery runtime required)."""
    entries = []
    for spec in MASSIVE_BEAT_SCHEDULE_SPEC:
        entries.append(
            {
                "name": spec["name"],
                "task": spec["task"],
                "label": spec["label"],
                "crontab": dict(spec["crontab_kwargs"]),
            }
        )
    return {
        "ok": True,
        "timezone": "UTC",
        "entries": entries,
        "massive_queues_disabled": bool(MASSIVE_QUEUES_DISABLED),
    }
