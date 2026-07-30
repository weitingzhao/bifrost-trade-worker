"""Option day pool fill — retired stub (P9 S3)."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

from bifrost_worker.data.massive._retired import retired_payload


def option_day_has_incomplete_rows(*_args: Any, **_kwargs: Any) -> bool:
    return False


def list_option_day_row_gap_targets(*_args: Any, **_kwargs: Any) -> List[Any]:
    return []


def chunk_option_day_row_gap_targets(
    targets: Sequence[Any], *_args: Any, **_kwargs: Any
) -> List[List[Any]]:
    del targets
    return []


def row_gap_targets_to_payload_dicts(targets: Sequence[Any]) -> List[Dict[str, Any]]:
    del targets
    return []


def run_option_day_pool_aggregates(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return retired_payload()
