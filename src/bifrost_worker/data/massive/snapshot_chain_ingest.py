"""Snapshot chain ingest — retired stub (P9 S3)."""

from __future__ import annotations

from typing import Any, Dict

from bifrost_worker.data.massive._retired import retired_payload


def contract_snapshot_api_response_to_chain_item(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return retired_payload()


def apply_chain_snapshot_item(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return retired_payload()
