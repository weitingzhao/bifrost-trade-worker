"""Pending Massive dispatch — retired (P9 S3)."""

from __future__ import annotations

from typing import Any, Dict, Optional


def massive_pending_dispatch_inflight_cap(status_cfg: Dict[str, Any]) -> int:
    del status_cfg
    return 0


def dispatch_pending_massive_topup(
    status_cfg: Dict[str, Any],
    celery_queue: Optional[str] = None,
) -> int:
    """No-op while Massive Celery queues are disabled / retired."""
    del status_cfg, celery_queue
    return 0
