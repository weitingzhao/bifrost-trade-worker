"""TIBM3 — Celery bars Platform Gateway transport."""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bifrost_worker.data.bars.ib_operator_transport import IbOperatorBarsAdapter


@pytest.mark.asyncio
async def test_ib_operator_bars_adapter_ping_sets_connected() -> None:
    gw = MagicMock()
    gw.request_async = AsyncMock(
        return_value={"ok": True, "data": {"host": {"client_id": 42}}},
    )
    adapter = IbOperatorBarsAdapter(gw, backfill_timeout_sec=60.0)
    assert adapter.connected is False
    await adapter.ensure_connected()
    assert adapter.connected is True
    assert adapter.client_id == 42
    gw.request_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_ib_operator_bars_adapter_fetch_bars_range() -> None:
    gw = MagicMock()
    gw.request_async = AsyncMock(
        side_effect=[
            {"ok": True, "data": {"host": {"client_id": 1}}},
            {"ok": True, "data": {"bars": [{"time": 1.0, "close": 100.0}]}},
        ],
    )
    adapter = IbOperatorBarsAdapter(gw, backfill_timeout_sec=60.0)
    await adapter.ensure_connected()
    bars = await adapter.fetch_bars_range("SPY", "1 D", start_ts=1.0, end_ts=2.0)
    assert len(bars) == 1
    assert bars[0]["close"] == 100.0


@pytest.mark.asyncio
async def test_get_or_create_bars_ib_client_uses_operator_adapter() -> None:
    from bifrost_worker.data.bars import tasks

    tasks._worker_ib_client = None
    cfg: Dict[str, Any] = {
        "redis_ib": {"url": "redis://127.0.0.1:6379/0"},
        "ib_operator": {"enabled": True},
    }
    mock_gw = MagicMock()
    mock_gw.request_async = AsyncMock(return_value={"ok": True, "data": {"host": {"client_id": 7}}})

    with patch(
        "bifrost_core.ib_operator.client.IbOperatorClient.from_merged_config",
        return_value=mock_gw,
    ):
        client = await tasks._get_or_create_bars_ib_client(cfg)

    assert isinstance(client, IbOperatorBarsAdapter)
    tasks._worker_ib_client = None


@pytest.mark.asyncio
async def test_get_or_create_bars_ib_client_rejects_disabled_operator() -> None:
    from bifrost_worker.data.bars import tasks

    tasks._worker_ib_client = None
    cfg: Dict[str, Any] = {
        "redis_ib": {"url": "redis://127.0.0.1:6379/0"},
        "ib_operator": {"enabled": False},
    }
    with pytest.raises(RuntimeError, match="direct TWS was removed"):
        await tasks._get_or_create_bars_ib_client(cfg)
    tasks._worker_ib_client = None
