"""Cap in-flight Celery-backed Massive jobs per broker queue slice.

After batch ``retry-failed`` only ``dispatch_pending_massive_topup`` runs (avoid broker flood).
After each ``run_massive_job`` finishes, ``finally`` calls top-up so idle capacity pulls more pending rows.
Bulk API producers still use staggered ``countdown``; this module covers retry + steady-state fill.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_DEFAULT_INFLIGHT_CAP = 12


def massive_pending_dispatch_inflight_cap(status_cfg: Dict[str, Any]) -> int:
    ops = status_cfg.get("ops") or {}
    celery_ops = ops.get("celery") or {}
    raw = celery_ops.get("massive_pending_dispatch_inflight_cap", _DEFAULT_INFLIGHT_CAP)
    try:
        return max(1, min(int(raw), 256))
    except (TypeError, ValueError):
        return _DEFAULT_INFLIGHT_CAP


def _count_inflight_for_queue_slice(
    status_cfg: Dict[str, Any],
    qcond: str,
    qparams: List[Any],
) -> int:
    try:
        from bifrost_core.persistence.postgres.connection import _get_conn_params
    except Exception:
        return 0
    try:
        params = _get_conn_params(status_cfg)
    except Exception:
        return 0
    sql = f"""
        SELECT COUNT(*)::bigint
        FROM job_massive_backfill
        WHERE (
                 status = 'running'
                 OR (
                   status = 'pending'
                   AND coalesce(trim(celery_task_id), '') <> ''
                   AND NOT (
                     trim(celery_task_id::text) LIKE 'dispatch:%%'
                     AND updated_at < (now() - interval '15 minutes')
                   )
                 )
               )
          AND ({qcond})
    """
    import psycopg2

    try:
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(qparams))
                row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        finally:
            conn.close()
    except Exception as e:
        logger.warning("count in-flight massive jobs: %s", e)
        return 0


def _dispatch_one_queue(status_cfg: Dict[str, Any], celery_queue: str) -> int:
    from bifrost_worker.data.massive.vendor.reader import (
        _massive_celery_queue_condition,
        reserve_next_pending_massive_job_for_queue_slice,
    )

    cq = (celery_queue or "").strip()
    if not cq:
        return 0
    qcond, qparams = _massive_celery_queue_condition(cq)
    if not qcond:
        return 0
    cap = massive_pending_dispatch_inflight_cap(status_cfg)
    did = 0
    while True:
        inflight = _count_inflight_for_queue_slice(status_cfg, qcond, qparams)
        if inflight >= cap:
            break
        row = reserve_next_pending_massive_job_for_queue_slice(status_cfg, qcond, qparams)
        if not row:
            break
        if str(row.get("status") or "").strip().lower() != "pending":
            continue
        from bifrost_worker.data.massive.tasks import reenqueue_massive_job_from_row

        ok, err = reenqueue_massive_job_from_row(status_cfg, dict(row))
        if ok:
            did += 1
        else:
            logger.info("dispatch_pending: stop after enqueue failure job_id=%s err=%s", row.get("job_massive_backfill_id"), err)
            break
    return did


def dispatch_pending_massive_topup(
    status_cfg: Dict[str, Any],
    celery_queue: Optional[str] = None,
) -> int:
    """Top-up broker tasks for pending rows until in-flight cap (per queue slice or all canonical massive queues)."""
    if not status_cfg or (status_cfg.get("sink") != "postgres" and not status_cfg.get("postgres")):
        return 0
    cq = (celery_queue or "").strip()
    if cq:
        return _dispatch_one_queue(status_cfg, cq)
    total = 0
    for q in ("options_massive_high", "options_massive", "stocks_massive_high", "stocks_massive"):
        total += _dispatch_one_queue(status_cfg, q)
    return total
