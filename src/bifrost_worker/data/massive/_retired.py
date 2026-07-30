"""Shared retired-response helpers for Massive ingest stubs (P9 S3)."""

from __future__ import annotations

from typing import Any, Dict

RETIRED_ERROR = "Massive queues disabled; use market-data plugin"
RETIRED_REASON = "massive_retired"


def retired_payload(**extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": False,
        "error": RETIRED_ERROR,
        "reason": RETIRED_REASON,
    }
    out.update(extra)
    return out
