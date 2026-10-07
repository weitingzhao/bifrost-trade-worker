"""TD-240 plan A: daemon.quote_mirror writes contract_quote_live and does not hedge."""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from bifrost_worker.daemon.app.gs_trading import GsTrading
from bifrost_worker.daemon.app.quote_mirror import (
    observe_quote_mirror,
    quote_mirror_from_config,
)

_ORDER_NAMES = ("_eval_hedge_sync", "_eval_hedge", "place_order")


def test_quote_mirror_defaults_off() -> None:
    assert quote_mirror_from_config(None) is False
    assert quote_mirror_from_config({}) is False
    assert quote_mirror_from_config({"daemon": {}}) is False
    assert quote_mirror_from_config({"daemon": {"quote_mirror": False}}) is False
    assert quote_mirror_from_config({"daemon": {"quote_mirror": "false"}}) is False
    assert quote_mirror_from_config({"daemon": {"quote_mirror": True}}) is True
    assert quote_mirror_from_config({"daemon": {"quote_mirror": "on"}}) is True


def test_config_reload_keeps_hedge_guard_and_reads_quote_mirror() -> None:
    init_src = inspect.getsource(GsTrading.__init__)
    reload_src = inspect.getsource(GsTrading._reload_config)
    for src in (init_src, reload_src):
        assert "self.mock_hedging = True" in src
        assert "self.quote_mirror = quote_mirror_from_config(config)" in src
    mirror_src = inspect.getsource(observe_quote_mirror)
    for name in _ORDER_NAMES:
        assert name not in mirror_src
    assert "ib:operator:cmd" not in mirror_src


def _app(*, quote_mirror: bool, mock_hedging: bool) -> SimpleNamespace:
    return SimpleNamespace(
        quote_mirror=quote_mirror,
        mock_hedging=mock_hedging,
        _contract_quote_live_initialized=False,
        _redis_quotes_reader=SimpleNamespace(available=True),
        _refresh_position_prices=AsyncMock(),
        _sync_contract_quote_live_from_redis=MagicMock(),
        _eval_hedge_sync=AsyncMock(),
        _eval_hedge=AsyncMock(),
        place_order=MagicMock(),
    )


@pytest.mark.asyncio
async def test_quote_mirror_writes_and_does_not_hedge_when_mock_hedging_off() -> None:
    app = _app(quote_mirror=True, mock_hedging=False)
    await observe_quote_mirror(app)
    app._sync_contract_quote_live_from_redis.assert_called_once()
    app._refresh_position_prices.assert_awaited()
    app._eval_hedge_sync.assert_not_called()
    app._eval_hedge.assert_not_called()
    app.place_order.assert_not_called()


@pytest.mark.asyncio
async def test_quote_mirror_off_does_not_write() -> None:
    app = _app(quote_mirror=False, mock_hedging=False)
    await observe_quote_mirror(app)
    app._sync_contract_quote_live_from_redis.assert_not_called()
    app._refresh_position_prices.assert_not_called()
    app._eval_hedge_sync.assert_not_called()
    app.place_order.assert_not_called()
