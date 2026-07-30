"""Celery task name registry for Massive jobs (P9 S3 no-op bodies).

Task names stay registered for Ops inspect / capabilities. Bodies refuse work when
``MASSIVE_QUEUES_DISABLED`` (market-data plugin owns ingest).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from bifrost_worker.celery.celery_app import app

logger = logging.getLogger(__name__)


def _disabled_result(job_id: Optional[int] = None) -> Dict[str, Any]:
    from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED_ERROR

    out: Dict[str, Any] = {
        "ok": False,
        "skipped": True,
        "reason": "massive_queues_disabled",
        "error": MASSIVE_QUEUES_DISABLED_ERROR,
    }
    if job_id is not None:
        out["job_id"] = job_id
    return out


@app.task(bind=True, name="src.massive.tasks.run_massive_job")
def run_massive_job(self, job_id: int) -> Dict[str, Any]:
    """Refuse Massive Celery jobs — plugin owns Polygon ingest (P8/P9)."""
    from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED

    if not MASSIVE_QUEUES_DISABLED:
        logger.error(
            "run_massive_job(%s): ingest body removed; enable market-data plugin",
            job_id,
        )
        return {
            "ok": False,
            "error": "Massive Celery ingest removed; use market-data plugin",
            "reason": "massive_retired",
            "job_id": job_id,
        }

    logger.warning(
        "run_massive_job(%s) skipped: MASSIVE_QUEUES_DISABLED (use market-data plugin)",
        job_id,
    )
    skip_result = _disabled_result(int(job_id))
    try:
        from bifrost_core.config.startup import read_config
        from bifrost_worker.data.massive.vendor.reader import update_job_massive_backfill_result

        config, _ = read_config()
        if config.get("postgres") or config.get("sink") == "postgres":
            update_job_massive_backfill_result(
                config, int(job_id), "failed", skip_result
            )
    except Exception:
        logger.exception(
            "run_massive_job(%s): failed to mark job failed after disable skip", job_id
        )
    return skip_result


def apply_async_massive_pending_job(
    control_via_db: dict,
    job_id: int,
    queue_name: str,
    *,
    countdown: Optional[float] = None,
    pre_dispatch_token: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """No-op enqueue while Massive queues are disabled."""
    del control_via_db, queue_name, countdown, pre_dispatch_token
    from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED

    if MASSIVE_QUEUES_DISABLED:
        return False, "massive_queues_disabled", None
    return False, "massive_retired", None


def reenqueue_massive_job_from_row(
    control_via_db: dict, row: Dict[str, Any]
) -> Tuple[bool, Optional[str]]:
    """Refuse re-enqueue while Massive queues are disabled."""
    del control_via_db, row
    from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED

    if MASSIVE_QUEUES_DISABLED:
        return False, "massive_queues_disabled"
    return False, "massive_retired"


def _beat_skip() -> Dict[str, Any]:
    return {"ok": True, "skipped": True, "reason": "massive_queues_disabled"}


@app.task(name="src.massive.tasks.beat_eod_pipeline")
def beat_eod_pipeline() -> Dict[str, Any]:
    return _beat_skip()


@app.task(name="src.massive.tasks.beat_corporate_watchlist")
def beat_corporate_watchlist() -> Dict[str, Any]:
    return _beat_skip()


@app.task(name="src.massive.tasks.beat_reconcile")
def beat_reconcile() -> Dict[str, Any]:
    return _beat_skip()


@app.task(name="src.massive.tasks.beat_trim_massive_jobs")
def beat_trim_massive_jobs() -> Dict[str, Any]:
    return _beat_skip()


@app.task(name="src.massive.tasks.beat_refresh_expirations")
def beat_refresh_expirations() -> Dict[str, Any]:
    return _beat_skip()


@app.task(name="src.massive.tasks.beat_stock_day_eod")
def beat_stock_day_eod() -> Dict[str, Any]:
    return _beat_skip()


@app.task(name="src.massive.tasks.beat_sepa_universe_grouped_daily")
def beat_sepa_universe_grouped_daily() -> Dict[str, Any]:
    return _beat_skip()
