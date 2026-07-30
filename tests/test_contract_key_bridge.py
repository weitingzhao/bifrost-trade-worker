"""Unit tests for IB ↔ Polygon option key bridge (P8 Critical C1)."""

from __future__ import annotations

from datetime import date

from bifrost_worker.data.massive.vendor.contract_key_bridge import (
    ib_contract_key_from_parts,
    identity_key,
    is_polygon_option_ticker,
    parse_ib_contract_key,
    split_contract_keys,
)


def _parse_ib_strike_right(ck: str) -> tuple[float | None, str | None]:
    parts = (ck or "").split("|")
    if len(parts) >= 5:
        try:
            return float(parts[3]), parts[4]
        except (TypeError, ValueError):
            return None, None
    return None, None


def test_is_polygon_option_ticker() -> None:
    assert is_polygon_option_ticker("O:AAPL250620C00150000")
    assert is_polygon_option_ticker("o:aapl250620c00150000")
    assert not is_polygon_option_ticker("AAPL|OPT|20250620|150.0|C")
    assert not is_polygon_option_ticker("")


def test_parse_ib_contract_key_yyyymmdd() -> None:
    parts = parse_ib_contract_key("AAPL|OPT|20250620|150.0|C")
    assert parts is not None
    assert parts.underlying == "AAPL"
    assert parts.expiry == date(2025, 6, 20)
    assert parts.strike == 150.0
    assert parts.option_right == "C"
    assert parts.original_key == "AAPL|OPT|20250620|150.0|C"


def test_parse_ib_contract_key_iso_and_put() -> None:
    parts = parse_ib_contract_key("msft|OPT|2025-07-19|400|PUT")
    assert parts is not None
    assert parts.underlying == "MSFT"
    assert parts.expiry == date(2025, 7, 19)
    assert parts.strike == 400.0
    assert parts.option_right == "P"


def test_parse_ib_rejects_polygon_and_malformed() -> None:
    assert parse_ib_contract_key("O:AAPL250620C00150000") is None
    assert parse_ib_contract_key("AAPL|STK|20250620") is None
    assert parse_ib_contract_key("AAPL|OPT|bad|150|C") is None


def test_ib_contract_key_from_parts_roundtrip() -> None:
    ck = ib_contract_key_from_parts("AAPL", date(2025, 6, 20), 150.0, "C")
    strike, right = _parse_ib_strike_right(ck)
    assert strike == 150.0
    assert right == "C"
    parts = parse_ib_contract_key(ck)
    assert parts is not None
    assert identity_key(parts.underlying, parts.expiry, parts.strike, parts.option_right) == (
        "AAPL",
        date(2025, 6, 20),
        150.0,
        "C",
    )


def test_split_contract_keys() -> None:
    poly, ib = split_contract_keys(
        [
            "AAPL|OPT|20250620|150.0|C",
            "O:AAPL250620C00150000",
            "o:msft250719p00400000",
            "junk",
            "AAPL|OPT|20250620|150.0|C",  # dedupe
        ]
    )
    assert poly == ["O:AAPL250620C00150000", "O:MSFT250719P00400000"]
    assert len(ib) == 1
    assert ib[0].underlying == "AAPL"
