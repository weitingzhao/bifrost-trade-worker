"""Market holidays sync — retired stub (P9 S3)."""

from __future__ import annotations

from typing import Any, Dict

from bifrost_worker.data.massive._retired import retired_payload


def sync_market_holidays_from_massive(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return retired_payload()
