"""Option min pool fill — retired stub (P9 S3)."""

from __future__ import annotations

from typing import Any, Dict

from bifrost_worker.data.massive._retired import retired_payload


def option_min_has_incomplete_rows(*_args: Any, **_kwargs: Any) -> bool:
    return False


def run_option_min_pool_aggregates(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return retired_payload()
