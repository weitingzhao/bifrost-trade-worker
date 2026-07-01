"""Tests for BIFROST_DAEMON_HARD_NO_ORDERS safety latch."""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from bifrost_worker.daemon.guards.order_safety import (
    apply_hard_order_block,
    hard_block_ib_orders,
)


class TestHardBlockIbOrders:
    def test_unset_is_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("BIFROST_DAEMON_HARD_NO_ORDERS", raising=False)
        assert hard_block_ib_orders() is False

    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
    def test_truthy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("BIFROST_DAEMON_HARD_NO_ORDERS", value)
        assert hard_block_ib_orders() is True


class TestApplyHardOrderBlock:
    def test_no_env_leaves_operator(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("BIFROST_DAEMON_HARD_NO_ORDERS", raising=False)
        op = object()
        app = SimpleNamespace(
            paper_trade=False,
            mock_hedging=False,
            _operator_client=op,
        )
        apply_hard_order_block(app)
        assert app._hard_block_ib_orders is False
        assert app.paper_trade is False
        assert app._operator_client is op

    def test_env_forces_safe_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BIFROST_DAEMON_HARD_NO_ORDERS", "1")
        app = SimpleNamespace(
            paper_trade=False,
            mock_hedging=False,
            _operator_client=MagicMock(),
        )
        apply_hard_order_block(app)
        assert app._hard_block_ib_orders is True
        assert app.paper_trade is True
        assert app.mock_hedging is True
        assert app._operator_client is None


class TestEntrySkipsDbGates:
    def test_inject_gates_skipped_when_hard_block(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("BIFROST_DAEMON_HARD_NO_ORDERS", "1")
        from bifrost_worker.daemon.app.entry import _inject_gates_from_db_if_configured

        config = {"sink": "postgres", "gates": {"guard": {"risk": {"paper_trade": True}}}}
        with patch(
            "bifrost_core.monitor.reader.gate_safety.get_active_gate_safety_strategy_id"
        ) as mock_gid:
            mock_gid.return_value = 99
            out = _inject_gates_from_db_if_configured(dict(config))
        assert out is config or out.get("gates") == config.get("gates")
        mock_gid.assert_not_called()
