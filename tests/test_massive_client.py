"""Lightweight tests for MassiveClient aggregate methods (mock _get)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from bifrost_worker.data.massive.vendor.client import MassiveClient, contract_key_from_reference_result


def _client() -> MassiveClient:
    return MassiveClient(api_key="test-key")


class TestFetchOptionOpenClose:
    def test_success(self):
        fake_response = {
            "status": "OK",
            "symbol": "O:TSLA210903C00700000",
            "from": "2023-01-09",
            "open": 25,
            "high": 26.35,
            "low": 25,
            "close": 26.35,
            "volume": 2,
            "preMarket": 25,
            "afterHours": 26.35,
        }
        with patch.object(MassiveClient, "_get", return_value=(200, fake_response)):
            result = _client().fetch_option_open_close("O:TSLA210903C00700000", "2023-01-09")
        assert result.get("close") == 26.35
        assert result.get("open") == 25
        assert "error" not in result

    def test_missing_ticker(self):
        result = _client().fetch_option_open_close("", "2023-01-09")
        assert "error" in result

    def test_missing_date(self):
        result = _client().fetch_option_open_close("O:SPY251219C00600000", "")
        assert "error" in result

    def test_http_error(self):
        with patch.object(MassiveClient, "_get", return_value=(404, {"error": "Not Found"})):
            result = _client().fetch_option_open_close("O:SPY251219C00600000", "2023-01-09")
        assert "error" in result


class TestFetchOptionPreviousDay:
    def test_success(self):
        fake_response = {
            "adjusted": True,
            "results": [
                {"T": "O:TSLA210903C00700000", "o": 115.55, "h": 117.59, "l": 114.13, "c": 115.97, "v": 131704427, "vw": 116.3058, "n": 2, "t": 1605042000000}
            ],
            "resultsCount": 1,
            "status": "OK",
        }
        with patch.object(MassiveClient, "_get", return_value=(200, fake_response)):
            result = _client().fetch_option_previous_day("O:TSLA210903C00700000")
        assert result.get("resultsCount") == 1
        assert result["results"][0]["c"] == 115.97
        assert "error" not in result

    def test_missing_ticker(self):
        result = _client().fetch_option_previous_day("")
        assert "error" in result

    def test_http_error(self):
        with patch.object(MassiveClient, "_get", return_value=(500, {"error": "Internal"})):
            result = _client().fetch_option_previous_day("O:SPY251219C00600000")
        assert "error" in result


class TestFetchStockGroupedDaily:
    def test_requests_grouped_path(self):
        paths: list[str] = []

        def capture_get(self, path, params=None):
            paths.append(path)
            return (200, {"status": "OK", "queryCount": 0, "results": []})

        with patch.object(MassiveClient, "_get", capture_get):
            out = _client().fetch_stock_grouped_daily("2024-06-03")
        assert not out.get("error")
        assert paths and "/v2/aggs/grouped/locale/us/market/stocks/2024-06-03" in paths[0]

    def test_missing_date(self):
        out = _client().fetch_stock_grouped_daily("")
        assert out.get("error")


class TestFetchStockAggs:
    def test_delegates_to_same_range_path_as_options(self):
        paths: list[str] = []

        def capture_get(self, path, params=None):
            paths.append(path)
            return (200, {"status": "OK", "results": []})

        with patch.object(MassiveClient, "_get", capture_get):
            out = _client().fetch_stock_aggs("AAPL", 1, "minute", 1_000, 2_000)
        assert not out.get("error")
        assert paths[0].startswith("/v2/aggs/ticker/AAPL/range/1/minute/1000/2000")

    def test_stock_ticker_sends_adjusted_true(self):
        captured: list[dict | None] = []

        def capture_get(self, path, params=None):
            captured.append(params)
            return (200, {"status": "OK", "results": []})

        with patch.object(MassiveClient, "_get", capture_get):
            _client().fetch_stock_aggs("AAPL", 1, "minute", 1_000, 2_000)
        assert (captured[0] or {}).get("adjusted") == "true"

    def test_index_ticker_omits_adjusted(self):
        captured: list[dict | None] = []

        def capture_get(self, path, params=None):
            captured.append(params)
            return (200, {"status": "OK", "results": [{"t": 1, "o": 1, "h": 1, "l": 1, "c": 1, "v": 0}]})

        with patch.object(MassiveClient, "_get", capture_get):
            out = _client().fetch_stock_aggs("I:SPX", 1, "day", 1_000, 2_000)
        assert not out.get("error")
        assert "adjusted" not in (captured[0] or {})

    def test_http_200_with_status_error_surfaces_error(self):
        with patch.object(
            MassiveClient,
            "_get",
            return_value=(
                200,
                {"status": "ERROR", "error": "bad range"},
            ),
        ):
            out = _client().fetch_stock_aggs("I:DJI", 1, "day", 1_000, 2_000)
        assert out.get("error")


class TestFetchStockNews:
    def test_builds_query_params(self):
        captured: list[dict | None] = []

        def capture_get(self, path, params=None):
            assert path == "/v2/reference/news"
            captured.append(params)
            return (200, {"status": "OK", "results": []})

        with patch.object(MassiveClient, "_get", capture_get):
            out = _client().fetch_stock_news(
                ticker="aapl",
                published_utc_gte="2026-04-01T00:00:00Z",
                published_utc_lte="2026-04-28T23:59:59Z",
                limit=25,
                sort="published_utc",
                order="desc",
            )
        assert not out.get("error")
        p = captured[0] or {}
        assert p.get("ticker") == "AAPL"
        assert p.get("published_utc.gte") == "2026-04-01T00:00:00Z"
        assert p.get("published_utc.lte") == "2026-04-28T23:59:59Z"
        assert p.get("limit") == 25
        assert p.get("sort") == "published_utc"
        assert p.get("order") == "desc"

    def test_requires_api_key(self):
        c = MassiveClient(api_key="")
        out = c.fetch_stock_news(ticker="AAPL")
        assert out.get("error")


class TestFetchOptionsSnapshotAllPages:
    """Chain snapshot pagination merges all pages (Option Discovery worker)."""

    @patch("bifrost_worker.data.massive.vendor.client.time.sleep", lambda *_a, **_k: None)
    @patch("bifrost_worker.data.massive.vendor.client.urlopen")
    def test_merges_second_page(self, mock_urlopen: MagicMock) -> None:
        page2 = {"results": [{"details": {"ticker": "O:SECOND"}}], "next_url": None}
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(page2).encode()
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        page1 = {
            "results": [{"details": {"ticker": "O:FIRST"}}],
            "next_url": "https://api.polygon.io/v3/snapshot/options/NVDA?cursor=x",
            "status": "OK",
        }
        with patch.object(MassiveClient, "_get", return_value=(200, page1)):
            out = _client().fetch_options_snapshot_all_pages("NVDA", expiration_date="2026-04-22")

        assert not out.get("error")
        assert out.get("pages") == 2
        assert len(out["results"]) == 2
        assert out["results"][0]["details"]["ticker"] == "O:FIRST"
        assert out["results"][1]["details"]["ticker"] == "O:SECOND"


class TestFetchUnifiedSnapshot:
    def test_omits_type_when_tickers_set(self):
        captured: list[dict | None] = []

        def capture_get(self, path, params=None):
            captured.append(dict(params or {}))
            return (200, {"status": "OK", "results": []})

        with patch.object(MassiveClient, "_get", capture_get):
            _client().fetch_unified_snapshot(
                tickers="AAPL,MSFT",
                asset_type="stocks",
                limit=250,
            )
        p = captured[0]
        assert p.get("ticker.any_of") == "AAPL,MSFT"
        assert "type" not in p
        assert p.get("limit") == 250

    def test_sends_type_when_no_tickers(self):
        captured: list[dict | None] = []

        def capture_get(self, path, params=None):
            captured.append(dict(params or {}))
            return (200, {"status": "OK", "results": []})

        with patch.object(MassiveClient, "_get", capture_get):
            _client().fetch_unified_snapshot(asset_type="stocks", limit=10)
        p = captured[0]
        assert p.get("type") == "stocks"
        assert "ticker.any_of" not in p


class TestContractKeyFromReferenceResult:
    def test_builds_key_like_upsert_path(self):
        row = {
            "expiration_date": "2026-04-18",
            "strike_price": 150.0,
            "contract_type": "call",
        }
        ck = contract_key_from_reference_result("NVDA", row)
        assert ck == "NVDA|OPT|20260418|150.0|C"

    def test_put_right(self):
        row = {
            "expiration_date": "2026-04-18",
            "strike_price": 150,
            "contract_type": "put",
        }
        assert contract_key_from_reference_result("NVDA", row) == "NVDA|OPT|20260418|150.0|P"


class TestCollectOptionContractKeysPaginated:
    def test_collects_keys_from_pages(self):
        page1 = {
            "status": "OK",
            "results": [
                {
                    "expiration_date": "2026-04-18",
                    "strike_price": 100,
                    "contract_type": "call",
                },
                {
                    "expiration_date": "2026-04-18",
                    "strike_price": 105,
                    "contract_type": "put",
                },
            ],
            "next_url": None,
        }
        with patch.object(MassiveClient, "_get", return_value=(200, page1)):
            out = _client().collect_option_contract_keys_paginated(
                "NVDA",
                expiration_date="20260418",
            )
        assert out.get("error") is None
        assert out.get("count") == 2
        assert len(out.get("keys") or []) == 2
        assert "NVDA|OPT|20260418|100.0|C" in out["keys"]
        assert "NVDA|OPT|20260418|105.0|P" in out["keys"]


class TestFetchFinancialsV1Ratios:
    def test_retries_with_tickers_when_ticker_returns_empty(self):
        calls: list[dict] = []

        def seq_get(self, path, params=None):
            calls.append(dict(params or {}))
            if (params or {}).get("ticker") == "A":
                return (200, {"status": "OK", "results": []})
            return (
                200,
                {
                    "status": "OK",
                    "results": [{"ticker": "A", "date": "2026-05-06", "price": 1.0}],
                },
            )

        with patch.object(MassiveClient, "_get", seq_get):
            out = _client().fetch_financials_v1_ratios(ticker="A", limit=50)
        assert not out.get("error")
        assert len(out.get("results") or []) == 1
        assert len(calls) == 2
        assert calls[0].get("ticker") == "A"
        assert calls[1].get("tickers") == "A"

    def test_omits_sort_by_default(self):
        captured: list[dict] = []

        def cap(self, path, params=None):
            captured.append(dict(params or {}))
            return (200, {"status": "OK", "results": []})

        with patch.object(MassiveClient, "_get", cap):
            _client().fetch_financials_v1_ratios(ticker="MSFT", limit=10)
        assert "sort" not in captured[0]

