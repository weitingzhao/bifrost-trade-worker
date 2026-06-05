"""Unit tests for Store (Phase 1: quote, spread, last_hedge_price)."""

import threading
import time

import pytest

from bifrost_worker.daemon.core.store import Store


class TestStore:
    def test_set_get_underlying_price(self):
        store = Store()
        store.set_underlying_price(500.0)
        assert store.get_underlying_price() == 500.0

    def test_set_underlying_quote_updates_mid(self):
        store = Store()
        store.set_underlying_quote(99.0, 101.0)
        assert store.get_underlying_price() == 100.0

    def test_get_spread_pct(self):
        store = Store()
        store.set_underlying_quote(99.0, 101.0)
        spread = store.get_spread_pct()
        assert spread is not None
        assert abs(spread - 2.0) < 0.01

    def test_get_spread_pct_no_quote_returns_none(self):
        store = Store()
        assert store.get_spread_pct() is None

    def test_last_hedge_price(self):
        store = Store()
        assert store.get_last_hedge_price() is None
        store.set_last_hedge_price(500.5)
        assert store.get_last_hedge_price() == 500.5

    def test_positions_and_stock_position(self):
        store = Store()
        store.set_positions([{"a": 1}], stock_position=50)
        assert len(store.get_positions()) == 1
        assert store.get_stock_position() == 50

    def test_account_summary_r_a1(self):
        """R-A1: account id and summary stored and retrieved."""
        store = Store()
        assert store.get_account_id() is None
        assert store.get_account_summary() is None
        store.set_account_summary("DU123", {"NetLiquidation": "100000", "TotalCashValue": "50000", "BuyingPower": "80000"})
        assert store.get_account_id() == "DU123"
        summary = store.get_account_summary()
        assert summary is not None
        assert summary.get("NetLiquidation") == "100000"
        assert summary.get("BuyingPower") == "80000"
        store.set_account_summary(None, None)
        assert store.get_account_id() is None
        assert store.get_account_summary() is None

    def test_accounts_data_multi_account(self):
        """R-A1 multi-account: list of account_id + summary + positions for monitoring."""
        store = Store()
        assert store.get_accounts_data() == []
        store.set_accounts_data([
            {"account_id": "DU1", "summary": {"NetLiquidation": "100000"}, "positions": [{"symbol": "AAPL", "position": 100}]},
            {"account_id": "DU2", "summary": {"NetLiquidation": "200000"}, "positions": []},
        ])
        data = store.get_accounts_data()
        assert len(data) == 2
        assert data[0]["account_id"] == "DU1"
        assert data[0]["summary"]["NetLiquidation"] == "100000"
        assert len(data[0]["positions"]) == 1
        assert data[0]["positions"][0]["symbol"] == "AAPL"
        assert data[1]["account_id"] == "DU2"
        store.set_accounts_data([])
        assert store.get_accounts_data() == []

    def test_thread_safety(self):
        store = Store()

        def writer():
            for i in range(100):
                store.set_underlying_quote(100.0 + i * 0.01, 100.0 + i * 0.01 + 0.1)
                store.set_positions([], i)

        def reader():
            for _ in range(100):
                _ = store.get_underlying_price()
                _ = store.get_spread_pct()
                _ = store.get_positions()
                _ = store.get_stock_position()

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
