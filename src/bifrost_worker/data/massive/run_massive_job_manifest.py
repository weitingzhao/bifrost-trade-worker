"""``run_massive_job`` matrix SSOT — empty after Massive Celery retirement (P9 S3)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from bifrost_worker.data.massive.celery_queues import celery_queue_for_massive_job
from bifrost_worker.data.massive.run_massive_job_matrix_effects import (
    effects_for_matrix_row,
    matrix_row_effects_to_api,
)

RUN_MASSIVE_JOB_CELERY_TASK_NAME = "src.massive.tasks.run_massive_job"


def matrix_row_task_name_and_job_style(kind: str) -> Tuple[str, str]:
    del kind
    return RUN_MASSIVE_JOB_CELERY_TASK_NAME, "on_demand"


@dataclass(frozen=True)
class RunMassiveJobMatrixRow:
    kind: str
    mode: Optional[str]
    mode_source: str
    broker_queue_standard: str
    broker_queue_high: str

    def to_api_dict(self) -> Dict[str, Any]:
        task_name, job_style = matrix_row_task_name_and_job_style(self.kind)
        out: Dict[str, Any] = {
            "kind": self.kind,
            "mode": self.mode,
            "mode_source": self.mode_source,
            "broker_queue_standard": self.broker_queue_standard,
            "broker_queue_high": self.broker_queue_high,
            "task_name": task_name,
            "job_style": job_style,
        }
        out.update(matrix_row_effects_to_api(effects_for_matrix_row(self.kind, self.mode)))
        return out


def queue_for_row(kind: str, mode: Optional[str], *, priority_high: bool) -> str:
    del mode
    return celery_queue_for_massive_job(kind, priority_high=priority_high)


def build_run_massive_job_matrix() -> List[RunMassiveJobMatrixRow]:
    """Empty: Celery Massive kinds retired; plugin owns ingest."""
    return []


RUN_MASSIVE_JOB_MATRIX: List[RunMassiveJobMatrixRow] = build_run_massive_job_matrix()
RUN_MASSIVE_JOB_TOP_LEVEL_KINDS: frozenset[str] = frozenset()
