"""Job goal strings for Ops UI — slim stub after Massive Celery retirement (P9 S3)."""

from __future__ import annotations

from typing import Any

GOAL_MAX_LEN = 480


def describe_massive_job_goal(kind: str, payload: Any) -> str:
    """Short English label; ingest details live in market-data plugin."""
    del payload
    k = (kind or "").strip() or "unknown"
    text = f"Massive job {k} (retired Celery path; use market-data plugin)"
    if len(text) <= GOAL_MAX_LEN:
        return text
    return text[: GOAL_MAX_LEN - 1].rstrip() + "…"
