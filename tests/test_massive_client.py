"""Tests for retired MassiveClient stub and contract-key helpers."""

from __future__ import annotations

from bifrost_worker.data.massive.vendor.client import (
    MassiveClient,
    _as_error_str,
    contract_key_from_parts,
    contract_key_from_reference_result,
)


def test_contract_key_from_parts() -> None:
    assert contract_key_from_parts("aapl", "2025-01-17", 150.0, "CALL") == (
        "AAPL|OPT|20250117|150.0|C"
    )


def test_contract_key_from_reference_result() -> None:
    key = contract_key_from_reference_result(
        "NVDA",
        {
            "expiration_date": "2025-06-20",
            "strike_price": 100.5,
            "contract_type": "put",
        },
    )
    assert key == "NVDA|OPT|20250620|100.5|P"


def test_as_error_str() -> None:
    assert _as_error_str("boom") == "boom"
    assert "x" in _as_error_str({"x": 1})


def test_massive_client_stub_refuses_http() -> None:
    client = MassiveClient(api_key="test-key")
    assert client.configured is True
    out = client.fetch_option_open_close("O:SPY251219C00600000", "2023-01-09")
    assert out.get("ok") is False
    assert "market-data plugin" in str(out.get("error", ""))
