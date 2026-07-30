"""Stock day gap — retired stub (P9 S3)."""

from __future__ import annotations

from typing import Any, Dict

from bifrost_worker.data.massive._retired import retired_payload


def compute_stock_day_gap(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return retired_payload()


def compute_stock_day_quality_detail(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return retired_payload()
