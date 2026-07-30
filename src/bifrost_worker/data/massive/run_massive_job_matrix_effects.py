"""Matrix row effects — retired stub (P9 S3)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class MatrixRowEffects:
    feed_apis: Tuple[str, ...]
    db_tables: Tuple[str, ...]
    redis_nodes: Tuple[str, ...]


def effects_for_matrix_row(kind: str, mode: Optional[str]) -> MatrixRowEffects:
    del kind, mode
    return MatrixRowEffects(
        feed_apis=("Massive Celery ingest retired; use market-data plugin",),
        db_tables=(),
        redis_nodes=(),
    )


def matrix_row_effects_to_api(e: MatrixRowEffects) -> Dict[str, object]:
    return {
        "feed_apis": list(e.feed_apis),
        "db_tables": list(e.db_tables),
        "redis_nodes": list(e.redis_nodes),
    }
