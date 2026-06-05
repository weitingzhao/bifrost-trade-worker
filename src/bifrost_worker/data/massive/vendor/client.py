"""HTTP client for Massive / Polygon options REST API (urllib, no extra deps)."""

from __future__ import annotations

import errno
import http.client
import json
import logging
import re
import ssl
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# urllib may raise http.client.RemoteDisconnected (ConnectionError), IncompleteRead (HTTPException),
# ssl.SSLError, etc. under parallel load or truncated bodies.
_GET_TRANSIENT_RETRIES = 5
_GET_TRANSIENT_BASE_SLEEP_SEC = 0.4
_GET_TRANSIENT_EXC_TYPES = (
    URLError,
    TimeoutError,
    ConnectionError,
    ssl.SSLError,
    http.client.HTTPException,
)
_OSError_retry_errno = frozenset(
    {
        errno.ECONNRESET,
        errno.EPIPE,
        errno.ETIMEDOUT,
        errno.ENETUNREACH,
        errno.EHOSTUNREACH,
        errno.ECONNABORTED,
        errno.ENETDOWN,
        errno.ENOTCONN,
        errno.EAGAIN,
        getattr(errno, "EWOULDBLOCK", errno.EAGAIN),
    }
)


DEFAULT_REST_BASE = "https://api.polygon.io"

# Polygon/Massive: day-range `from`/`to` may be YYYY-MM-DD or ms; use NY calendar dates for `day` bars.
_ET = ZoneInfo("America/New_York")


def _ny_date_range_strings_from_ms(start_ms: int, end_ms: int) -> Tuple[str, str]:
    """Inclusive America/New_York calendar dates for a wall-clock ms interval."""
    a, b = int(start_ms), int(end_ms)
    if b < a:
        a, b = b, a
    d0 = datetime.fromtimestamp(a / 1000.0, tz=timezone.utc).astimezone(_ET).date()
    d1 = datetime.fromtimestamp(b / 1000.0, tz=timezone.utc).astimezone(_ET).date()
    return d0.isoformat(), d1.isoformat()


def _as_error_str(err: Any) -> str:
    """Polygon/Massive sometimes returns error as a string, object, or list."""
    if isinstance(err, str):
        return err
    if err is None:
        return "Unknown error"
    try:
        return json.dumps(err, default=str)
    except (TypeError, ValueError):
        return str(err)


def _polygon_body_error_message(data: Any, http_status: int) -> Optional[str]:
    """Return a message if JSON indicates logical failure (including HTTP 200 error bodies)."""
    if not isinstance(data, dict):
        return None
    ps = str(data.get("status") or "").upper()
    if ps in ("ERROR", "NOT_AUTHORIZED", "FAILED"):
        for key in ("error", "message"):
            val = data.get(key)
            if val is not None:
                return _as_error_str(val)
        return ps if ps else f"HTTP {http_status}"
    return None


def _redact_url_api_key(url: str) -> str:
    """Replace apiKey / apikey query values with *** for logs and API responses."""
    if not url:
        return url
    return re.sub(r"([?&])(apiKey|apikey)=([^&]*)", r"\1\2=***", url, flags=re.I)


def _norm_expiry(s: str) -> str:
    """Normalize expiration to YYYYMMDD or YYYYMM as stored elsewhere."""
    s = (s or "").strip()
    if len(s) >= 10 and s[4] == "-":
        return s[:4] + s[5:7] + s[8:10]
    return s


def _expiry_to_polygon_date(s: str) -> Optional[str]:
    """Convert YYYYMMDD or YYYY-MM-DD to Polygon's YYYY-MM-DD format. Returns None if invalid."""
    s = (s or "").strip()
    if not s:
        return None
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def _right_from_contract_type(ct: str) -> str:
    u = (ct or "").upper()
    if u in ("CALL", "C"):
        return "C"
    if u in ("PUT", "P"):
        return "P"
    return "C"


def contract_key_from_parts(
    symbol: str, expiry: str, strike: float, option_right: str
) -> str:
    """Match account_positions / DATABASE.md: symbol|OPT|expiry|strike|right."""
    sym = (symbol or "").strip().upper()
    exp = _norm_expiry(expiry)
    r = (option_right or "").strip().upper()
    if r in ("CALL",):
        r = "C"
    if r in ("PUT",):
        r = "P"
    # Normalize strike so query keys match rows written from Polygon strike_price (avoids float drift).
    sk = round(float(strike), 8)
    return f"{sym}|OPT|{exp}|{sk}|{r}"


def contract_key_from_reference_result(
    underlying: str, row: Dict[str, Any]
) -> Optional[str]:
    """Build ``option_contracts.contract_key`` from a Polygon ``/v3/reference/options/contracts`` result row."""
    u = (underlying or "").strip().upper()
    if not u or not isinstance(row, dict):
        return None
    exp = row.get("expiration_date") or row.get("expiration") or ""
    if not exp:
        return None
    ed = _norm_expiry(str(exp)[:10])
    if len(ed) != 8 or not ed.isdigit():
        return None
    sp = row.get("strike_price")
    if sp is None:
        return None
    try:
        strike = float(sp)
    except (TypeError, ValueError):
        return None
    ort = _right_from_contract_type(str(row.get("contract_type") or "call"))
    return contract_key_from_parts(u, ed, strike, ort)


class MassiveClient:
    """Minimal Polygon v3 options REST wrapper."""

    def __init__(self, api_key: str, rest_base: str = DEFAULT_REST_BASE) -> None:
        self._api_key = (api_key or "").strip()
        self._base = (rest_base or DEFAULT_REST_BASE).rstrip("/")
        self._ssl = ssl.create_default_context()

    @property
    def configured(self) -> bool:
        return bool(self._api_key)

    def _redacted_get_url(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        q = dict(params or {})
        q["apiKey"] = "***"
        url = f"{self._base}{path}"
        return f"{url}?{urlencode(q)}" if q else url

    def _get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        timeout_sec: float = 60.0,
    ) -> Tuple[int, Any]:
        """Return (http_status, parsed_json_or_none). Retries transient TCP / TLS / truncated-body errors."""
        q = dict(params or {})
        q["apiKey"] = self._api_key
        url = f"{self._base}{path}"
        if q:
            url = f"{url}?{urlencode(q)}"
        last_transient: Optional[BaseException] = None
        for attempt in range(_GET_TRANSIENT_RETRIES):
            req = Request(url, headers={"Accept": "application/json"}, method="GET")
            try:
                with urlopen(req, timeout=timeout_sec, context=self._ssl) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    status = getattr(resp, "status", 200) or 200
                    try:
                        return int(status), json.loads(body)
                    except json.JSONDecodeError:
                        return int(status), {"raw": body[:500]}
            except HTTPError as e:
                try:
                    body = e.read().decode("utf-8", errors="replace")
                    return e.code, json.loads(body)
                except Exception:
                    return e.code, {"error": str(e)}
            except _GET_TRANSIENT_EXC_TYPES as e:
                last_transient = e
                if attempt < _GET_TRANSIENT_RETRIES - 1:
                    time.sleep(_GET_TRANSIENT_BASE_SLEEP_SEC * (2**attempt))
            except OSError as e:
                if getattr(e, "errno", None) not in _OSError_retry_errno:
                    raise
                last_transient = e
                if attempt < _GET_TRANSIENT_RETRIES - 1:
                    time.sleep(_GET_TRANSIENT_BASE_SLEEP_SEC * (2**attempt))
        logger.warning(
            "MassiveClient _get gave up after %s tries %s: %s",
            _GET_TRANSIENT_RETRIES,
            _redact_url_api_key(url),
            last_transient,
        )
        return 0, {"error": str(last_transient) if last_transient else "connection error"}

    def _get_json_from_next_url(self, next_url: str, *, timeout_sec: float = 60.0) -> Tuple[int, Any]:
        """Follow Polygon next_url; append apiKey when missing. Returns (http_status, parsed_json)."""
        url = next_url
        if "apiKey=" not in url and "apikey=" not in url.lower():
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}apiKey={self._api_key}"
        last_transient: Optional[BaseException] = None
        for attempt in range(_GET_TRANSIENT_RETRIES):
            req = Request(url, headers={"Accept": "application/json"}, method="GET")
            try:
                with urlopen(req, timeout=timeout_sec, context=self._ssl) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    status = getattr(resp, "status", 200) or 200
                    try:
                        return int(status), json.loads(body)
                    except json.JSONDecodeError:
                        return int(status), {"raw": body[:500]}
            except HTTPError as e:
                try:
                    body = e.read().decode("utf-8", errors="replace")
                    return e.code, json.loads(body)
                except Exception:
                    return e.code, {"error": str(e)}
            except _GET_TRANSIENT_EXC_TYPES as e:
                last_transient = e
                if attempt < _GET_TRANSIENT_RETRIES - 1:
                    time.sleep(_GET_TRANSIENT_BASE_SLEEP_SEC * (2**attempt))
            except OSError as e:
                if getattr(e, "errno", None) not in _OSError_retry_errno:
                    raise
                last_transient = e
                if attempt < _GET_TRANSIENT_RETRIES - 1:
                    time.sleep(_GET_TRANSIENT_BASE_SLEEP_SEC * (2**attempt))
        logger.warning(
            "MassiveClient _get_json_from_next_url gave up after %s tries: %s",
            _GET_TRANSIENT_RETRIES,
            last_transient,
        )
        return 0, {"error": str(last_transient) if last_transient else "connection error"}

    def count_option_contracts_list_paginated(
        self,
        underlying: str,
        *,
        expiration_date: Optional[str] = None,
        max_pages: int = 20,
        limit: int = 250,
    ) -> Dict[str, Any]:
        """Paginate GET /v3/reference/options/contracts and sum len(results) per page.

        When *expiration_date* is set, only that expiry is counted (server-side filter).
        Returns ``count``, ``truncated`` (True if stopped early due to max_pages), and optional ``error``.
        """
        underlying = (underlying or "").strip().upper()
        if not underlying or not self._api_key:
            return {"count": 0, "truncated": False, "error": "symbol or api key missing"}
        path = "/v3/reference/options/contracts"
        params: Dict[str, Any] = {
            "underlying_ticker": underlying,
            "limit": min(int(limit), 250),
        }
        poly_exp = _expiry_to_polygon_date(expiration_date or "")
        if poly_exp:
            params["expiration_date"] = poly_exp
        total = 0
        next_url: Optional[str] = None
        pages = 0
        truncated = False
        while True:
            if pages >= max_pages:
                if next_url:
                    truncated = True
                break
            pages += 1
            if next_url:
                status, data = self._get_json_from_next_url(next_url)
            else:
                status, data = self._get(path, params)
            if status >= 400:
                err = data.get("error", data) if isinstance(data, dict) else str(data)
                return {"count": total, "truncated": truncated, "error": _as_error_str(err)}
            if not isinstance(data, dict):
                return {"count": total, "truncated": truncated, "error": "invalid response"}
            poly_err = _polygon_body_error_message(data, status)
            if poly_err:
                return {"count": total, "truncated": truncated, "error": poly_err}
            results = data.get("results")
            if not isinstance(results, list):
                break
            total += len(results)
            next_url = data.get("next_url") if isinstance(data, dict) else None
            if not next_url:
                break
        return {"count": total, "truncated": truncated, "error": None}

    def collect_option_contract_keys_paginated(
        self,
        underlying: str,
        *,
        expiration_date: Optional[str],
        max_pages: int = 20,
        limit: int = 250,
    ) -> Dict[str, Any]:
        """Paginate GET /v3/reference/options/contracts; return each result row count and derived ``contract_key`` list.

        Used to compare PostgreSQL ``option_contracts.contract_key`` only within the API-returned universe for that
        expiry (rows in DB that are not returned by the reference list are excluded from the comparable PG count).
        """
        underlying = (underlying or "").strip().upper()
        if not underlying or not self._api_key:
            return {"count": 0, "keys": [], "truncated": False, "error": "symbol or api key missing"}
        path = "/v3/reference/options/contracts"
        params: Dict[str, Any] = {
            "underlying_ticker": underlying,
            "limit": min(int(limit), 250),
        }
        poly_exp = _expiry_to_polygon_date(expiration_date or "")
        if poly_exp:
            params["expiration_date"] = poly_exp
        keys: List[str] = []
        total = 0
        next_url: Optional[str] = None
        pages = 0
        truncated = False
        while True:
            if pages >= max_pages:
                if next_url:
                    truncated = True
                break
            pages += 1
            if next_url:
                status, data = self._get_json_from_next_url(next_url)
            else:
                status, data = self._get(path, params)
            if status >= 400:
                err = data.get("error", data) if isinstance(data, dict) else str(data)
                return {"count": total, "keys": keys, "truncated": truncated, "error": _as_error_str(err)}
            if not isinstance(data, dict):
                return {"count": total, "keys": keys, "truncated": truncated, "error": "invalid response"}
            poly_err = _polygon_body_error_message(data, status)
            if poly_err:
                return {"count": total, "keys": keys, "truncated": truncated, "error": poly_err}
            results = data.get("results")
            if not isinstance(results, list):
                break
            for row in results:
                total += 1
                if isinstance(row, dict):
                    ck = contract_key_from_reference_result(underlying, row)
                    if ck:
                        keys.append(ck)
            next_url = data.get("next_url") if isinstance(data, dict) else None
            if not next_url:
                break
        return {"count": total, "keys": keys, "truncated": truncated, "error": None}

    def collect_option_contract_reference_rows_paginated(
        self,
        underlying: str,
        *,
        expiration_date: Optional[str],
        max_pages: int = 20,
        limit: int = 250,
    ) -> Dict[str, Any]:
        """Paginate GET /v3/reference/options/contracts; return rows with contract_key for PG/API column parity."""
        underlying = (underlying or "").strip().upper()
        if not underlying or not self._api_key:
            return {"count": 0, "rows": [], "truncated": False, "error": "symbol or api key missing"}
        path = "/v3/reference/options/contracts"
        params: Dict[str, Any] = {
            "underlying_ticker": underlying,
            "limit": min(int(limit), 250),
        }
        poly_exp = _expiry_to_polygon_date(expiration_date or "")
        if poly_exp:
            params["expiration_date"] = poly_exp
        out_rows: List[Dict[str, Any]] = []
        total = 0
        next_url: Optional[str] = None
        pages = 0
        truncated = False
        while True:
            if pages >= max_pages:
                if next_url:
                    truncated = True
                break
            pages += 1
            if next_url:
                status, data = self._get_json_from_next_url(next_url)
            else:
                status, data = self._get(path, params)
            if status >= 400:
                err = data.get("error", data) if isinstance(data, dict) else str(data)
                return {"count": total, "rows": out_rows, "truncated": truncated, "error": _as_error_str(err)}
            if not isinstance(data, dict):
                return {"count": total, "rows": out_rows, "truncated": truncated, "error": "invalid response"}
            poly_err = _polygon_body_error_message(data, status)
            if poly_err:
                return {"count": total, "rows": out_rows, "truncated": truncated, "error": poly_err}
            results = data.get("results")
            if not isinstance(results, list):
                break
            for row in results:
                total += 1
                if isinstance(row, dict):
                    ck = contract_key_from_reference_result(underlying, row)
                    if ck:
                        out_rows.append({"contract_key": ck, "result": row})
            next_url = data.get("next_url") if isinstance(data, dict) else None
            if not next_url:
                break
        return {"count": total, "rows": out_rows, "truncated": truncated, "error": None}

    def fetch_expirations_and_strikes(
        self,
        underlying: str,
        max_pages: int = 20,
        *,
        include_debug: bool = False,
        max_contract_samples: int = 200,
        expiration_date: Optional[str] = None,
        collect_contract_rows: bool = False,
    ) -> Dict[str, Any]:
        """Paginate /v3/reference/options/contracts; return expirations, strikes, tickers map.

        When *expiration_date* is set (YYYYMMDD or YYYY-MM-DD), Polygon filters
        server-side so only contracts for that single expiry are returned.

        When *collect_contract_rows* is True, each contract row is appended to
        ``contract_rows`` for PostgreSQL upserts (option_contracts).
        """
        underlying = (underlying or "").strip().upper()
        if not underlying or not self._api_key:
            out_bad: Dict[str, Any] = {"expirations": [], "strikes": [], "error": "symbol or api key missing"}
            if collect_contract_rows:
                out_bad["contract_rows"] = []
            return out_bad
        expirations: set = set()
        strikes: set = set()
        contract_rows: List[Dict[str, Any]] = []
        debug_pages: List[Dict[str, Any]] = []
        contract_samples: List[Dict[str, Any]] = []
        next_url: Optional[str] = None
        path = "/v3/reference/options/contracts"
        params: Dict[str, Any] = {"underlying_ticker": underlying, "limit": 250}
        poly_exp = _expiry_to_polygon_date(expiration_date or "")
        if poly_exp:
            params["expiration_date"] = poly_exp
        pages = 0
        while pages < max_pages:
            pages += 1
            if next_url:
                url_redacted = _redact_url_api_key(next_url)
                http_st, data = self._get_json_from_next_url(next_url)
                if include_debug:
                    debug_pages.append(
                        {
                            "page_index": pages,
                            "request": {"method": "GET", "url": url_redacted},
                            "response_status": int(http_st),
                            "response": data if isinstance(data, dict) else {"_non_object": data},
                        }
                    )
                if http_st >= 400 or http_st == 0:
                    err_body = data.get("error", data) if isinstance(data, dict) else str(data)
                    out_e: Dict[str, Any] = {
                        "expirations": sorted(expirations),
                        "strikes": sorted(strikes),
                        "error": err_body,
                    }
                    if collect_contract_rows:
                        out_e["contract_rows"] = contract_rows
                    if include_debug:
                        out_e["massive_debug"] = {"pages": debug_pages, "contract_samples": contract_samples}
                    return out_e
            else:
                url_redacted = self._redacted_get_url(path, params)
                status, data = self._get(path, params)
                if include_debug:
                    debug_pages.append(
                        {
                            "page_index": pages,
                            "request": {"method": "GET", "url": url_redacted},
                            "response_status": int(status),
                            "response": data if isinstance(data, dict) else {"_non_object": data},
                        }
                    )
                if status >= 400:
                    err_body = data.get("error", data) if isinstance(data, dict) else str(data)
                    out_err: Dict[str, Any] = {
                        "expirations": [],
                        "strikes": [],
                        "error": err_body,
                    }
                    if collect_contract_rows:
                        out_err["contract_rows"] = []
                    if include_debug:
                        out_err["massive_debug"] = {"pages": debug_pages, "contract_samples": []}
                    return out_err
            results = data.get("results") if isinstance(data, dict) else None
            if not results:
                break
            for r in results:
                if not isinstance(r, dict):
                    continue
                if include_debug and len(contract_samples) < max_contract_samples:
                    contract_samples.append(dict(r))
                ed = r.get("expiration_date") or r.get("expiration")
                if ed:
                    expirations.add(_norm_expiry(str(ed)[:10]))
                sp = r.get("strike_price")
                if sp is not None:
                    try:
                        strikes.add(float(sp))
                    except (TypeError, ValueError):
                        pass
                if collect_contract_rows and ed and sp is not None:
                    try:
                        contract_rows.append(
                            {
                                "ticker": (r.get("ticker") or "").strip(),
                                "expiration_date": _norm_expiry(str(ed)[:10]),
                                "strike_price": float(sp),
                                "contract_type": (r.get("contract_type") or "").strip(),
                            }
                        )
                    except (TypeError, ValueError):
                        pass
            next_url = data.get("next_url") if isinstance(data, dict) else None
            if not next_url:
                break
        out: Dict[str, Any] = {
            "expirations": sorted(expirations),
            "strikes": sorted(strikes),
        }
        if collect_contract_rows:
            out["contract_rows"] = contract_rows
        if include_debug:
            out["massive_debug"] = {
                "pages": debug_pages,
                "contract_samples": contract_samples,
                "contract_samples_truncated": len(contract_samples) >= max_contract_samples,
            }
        return out

    def fetch_option_contracts_list(
        self,
        underlying: str,
        *,
        expiration_date: Optional[str] = None,
        contract_type: Optional[str] = None,
        strike_price: Optional[float] = None,
        strike_price_gte: Optional[float] = None,
        strike_price_lte: Optional[float] = None,
        limit: Optional[int] = None,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /v3/reference/options/contracts — raw contract listing with filters."""
        underlying = (underlying or "").strip().upper()
        if not underlying or not self._api_key:
            return {"results": [], "error": "symbol or api key missing"}
        params: Dict[str, Any] = {
            "underlying_ticker": underlying,
            "limit": min(int(limit or 100), 250),
        }
        poly_exp = _expiry_to_polygon_date(expiration_date or "")
        if poly_exp:
            params["expiration_date"] = poly_exp
        if contract_type:
            params["contract_type"] = contract_type.lower()
        if strike_price is not None:
            params["strike_price"] = strike_price
        if strike_price_gte is not None:
            params["strike_price.gte"] = strike_price_gte
        if strike_price_lte is not None:
            params["strike_price.lte"] = strike_price_lte
        if sort:
            params["sort"] = sort
        if order:
            params["order"] = order
        status, data = self._get("/v3/reference/options/contracts", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        return data if isinstance(data, dict) else {"results": [], "error": "invalid response"}

    def fetch_option_contract_detail(self, options_ticker: str) -> Dict[str, Any]:
        """GET /v3/reference/options/contracts/{options_ticker} — single contract metadata."""
        options_ticker = (options_ticker or "").strip()
        if not options_ticker or not self._api_key:
            return {"results": {}, "error": "options_ticker or api key missing"}
        status, data = self._get(f"/v3/reference/options/contracts/{options_ticker}")
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": {}, "error": err}
        return data if isinstance(data, dict) else {"results": {}, "error": "invalid response"}

    def fetch_options_snapshot(
        self,
        underlying: str,
        *,
        strike_price: Optional[float] = None,
        strike_price_gte: Optional[float] = None,
        strike_price_lte: Optional[float] = None,
        expiration_date: Optional[str] = None,
        expiration_date_gte: Optional[str] = None,
        expiration_date_lte: Optional[str] = None,
        contract_type: Optional[str] = None,
        limit: Optional[int] = None,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /v3/snapshot/options/{underlying} with optional filters."""
        underlying = (underlying or "").strip().upper()
        if not underlying or not self._api_key:
            return {"results": [], "error": "symbol or api key missing"}
        params: Dict[str, Any] = {}
        if strike_price is not None:
            params["strike_price"] = strike_price
        if strike_price_gte is not None:
            params["strike_price.gte"] = strike_price_gte
        if strike_price_lte is not None:
            params["strike_price.lte"] = strike_price_lte
        if expiration_date:
            poly = _expiry_to_polygon_date(expiration_date)
            if poly:
                params["expiration_date"] = poly
        if expiration_date_gte:
            poly = _expiry_to_polygon_date(expiration_date_gte)
            if poly:
                params["expiration_date.gte"] = poly
        if expiration_date_lte:
            poly = _expiry_to_polygon_date(expiration_date_lte)
            if poly:
                params["expiration_date.lte"] = poly
        if contract_type:
            params["contract_type"] = contract_type.lower()
        if limit is not None:
            params["limit"] = min(int(limit), 250)
        if sort:
            params["sort"] = sort
        if order:
            params["order"] = order
        status, data = self._get(f"/v3/snapshot/options/{underlying}", params or None)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        return data if isinstance(data, dict) else {"results": [], "error": "invalid response"}

    def fetch_options_snapshot_all_pages(
        self,
        underlying: str,
        *,
        page_delay_sec: float = 0.2,
        max_pages: int = 500,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Paginate GET /v3/snapshot/options/{underlying} until next_url is empty.

        Merges all ``results`` into a single list. Stops on error or *max_pages*.
        """
        underlying = (underlying or "").strip().upper()
        if not underlying or not self._api_key:
            return {"results": [], "error": "symbol or api key missing", "pages": 0}
        merged: List[Dict[str, Any]] = []
        next_url: Optional[str] = None
        pages = 0
        path = f"/v3/snapshot/options/{underlying}"
        while pages < max_pages:
            if next_url:
                http_st, data = self._get_json_from_next_url(next_url)
                if http_st >= 400 or http_st == 0:
                    err = data.get("error", data) if isinstance(data, dict) else str(data)
                    logger.warning("fetch_options_snapshot_all_pages page error: %s", err)
                    return {"results": merged, "error": str(err), "pages": pages}
            else:
                # First page: reuse same query params as fetch_options_snapshot
                q: Dict[str, Any] = {}
                if kwargs.get("strike_price") is not None:
                    q["strike_price"] = kwargs["strike_price"]
                if kwargs.get("strike_price_gte") is not None:
                    q["strike_price.gte"] = kwargs["strike_price_gte"]
                if kwargs.get("strike_price_lte") is not None:
                    q["strike_price.lte"] = kwargs["strike_price_lte"]
                exp = kwargs.get("expiration_date")
                if exp:
                    poly = _expiry_to_polygon_date(str(exp))
                    if poly:
                        q["expiration_date"] = poly
                eg = kwargs.get("expiration_date_gte")
                if eg:
                    poly = _expiry_to_polygon_date(str(eg))
                    if poly:
                        q["expiration_date.gte"] = poly
                el = kwargs.get("expiration_date_lte")
                if el:
                    poly = _expiry_to_polygon_date(str(el))
                    if poly:
                        q["expiration_date.lte"] = poly
                if kwargs.get("contract_type"):
                    q["contract_type"] = str(kwargs["contract_type"]).lower()
                lim = kwargs.get("limit")
                if lim is not None:
                    q["limit"] = min(int(lim), 250)
                if kwargs.get("sort"):
                    q["sort"] = str(kwargs["sort"])
                if kwargs.get("order"):
                    q["order"] = str(kwargs["order"])
                status, data = self._get(path, q or None)
                if status >= 400:
                    err = data.get("error", data) if isinstance(data, dict) else str(data)
                    return {"results": merged, "error": err, "pages": pages}
            if not isinstance(data, dict):
                return {"results": merged, "error": "invalid response", "pages": pages}
            chunk = data.get("results") or []
            if isinstance(chunk, list):
                merged.extend([x for x in chunk if isinstance(x, dict)])
            pages += 1
            next_url = data.get("next_url") if isinstance(data, dict) else None
            if not next_url:
                return {"results": merged, "error": None, "pages": pages}
            time.sleep(max(0.0, float(page_delay_sec)))
        return {"results": merged, "error": None, "pages": pages, "truncated": True}

    def fetch_option_contract_snapshot(self, underlying: str, option_contract: str) -> Dict[str, Any]:
        """GET /v3/snapshot/options/{underlyingAsset}/{optionContract}."""
        underlying = (underlying or "").strip().upper()
        option_contract = (option_contract or "").strip()
        if not underlying or not option_contract or not self._api_key:
            return {"results": {}, "error": "underlying, option_contract, or api key missing"}
        status, data = self._get(f"/v3/snapshot/options/{underlying}/{option_contract}")
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": {}, "error": err}
        return data if isinstance(data, dict) else {"results": {}, "error": "invalid response"}

    def fetch_unified_snapshot(
        self,
        *,
        tickers: Optional[str] = None,
        asset_type: Optional[str] = None,
        ticker_gte: Optional[str] = None,
        ticker_lte: Optional[str] = None,
        limit: Optional[int] = None,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /v3/snapshot — cross-asset unified snapshot.

        Massive rejects ``type`` together with ``ticker.any_of`` (error:
        "Cannot specify tickers and type"). When ``tickers`` is set, ``asset_type``
        is omitted; callers rely on explicit symbols or per-result ``type`` in JSON.
        """
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {}
        if tickers:
            params["ticker.any_of"] = tickers
        elif asset_type:
            params["type"] = asset_type
        if ticker_gte:
            params["ticker.gte"] = ticker_gte
        if ticker_lte:
            params["ticker.lte"] = ticker_lte
        if limit is not None:
            params["limit"] = min(int(limit), 250)
        if sort:
            params["sort"] = sort
        if order:
            params["order"] = order
        status, data = self._get("/v3/snapshot", params or None)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        return data if isinstance(data, dict) else {"results": []}

    @staticmethod
    def _v2_range_aggs_query_params(ticker: str) -> Dict[str, Any]:
        """Query params for GET /v2/aggs/ticker/.../range/...

        Polygon **indices** (``I:SPX``) and **options** (``O:…``) omit ``adjusted``:
        index docs omit it; option contract bars can behave poorly or omit fields like ``vw``
        when ``adjusted=true`` is forced like equities.
        Stocks use ``adjusted=true`` by default.
        """
        t = (ticker or "").strip().upper()
        base: Dict[str, Any] = {"sort": "asc", "limit": 50000}
        if t.startswith("I:") or t.startswith("O:"):
            return base
        return {**base, "adjusted": "true"}

    def fetch_option_aggs(
        self,
        options_ticker: str,
        multiplier: int,
        timespan: str,
        start_ms: int,
        end_ms: int,
    ) -> Dict[str, Any]:
        """GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}.

        ``from`` / ``to`` per Polygon docs: YYYY-MM-DD or millisecond timestamp. For
        ``timespan`` ``day`` we use inclusive NY calendar dates derived from ``start_ms`` /
        ``end_ms`` (same window as ms, easier to match REST docs / console).

        Follows ``next_url`` and merges ``results`` from all pages (Polygon caps page size).
        """
        ot = (options_ticker or "").strip()
        if not ot or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        enc = quote(ot, safe="")
        ts = (timespan or "").strip().lower()
        if ts == "day":
            d_from, d_to = _ny_date_range_strings_from_ms(start_ms, end_ms)
            path = f"/v2/aggs/ticker/{enc}/range/{multiplier}/day/{d_from}/{d_to}"
        else:
            path = f"/v2/aggs/ticker/{enc}/range/{multiplier}/{timespan}/{start_ms}/{end_ms}"
        params = self._v2_range_aggs_query_params(ot)
        status, data = self._get(path, params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if not isinstance(data, dict):
            return {"results": []}
        logical = _polygon_body_error_message(data, status)
        if logical:
            return {"results": [], "error": logical}

        merged_results: List[Any] = []
        seen: Optional[Dict[str, Any]] = data
        max_pages = 200
        pages = 0
        while seen and pages < max_pages:
            pages += 1
            chunk = seen.get("results") or []
            if isinstance(chunk, list):
                merged_results.extend(chunk)
            next_url = seen.get("next_url") if isinstance(seen, dict) else None
            if not next_url:
                break
            st, seen_payload = self._get_json_from_next_url(next_url, timeout_sec=120.0)
            if st >= 400 or st == 0:
                err_note = (
                    seen_payload.get("error", seen_payload)
                    if isinstance(seen_payload, dict)
                    else str(seen_payload)
                )
                logger.warning("fetch_option_aggs next_url fetch failed: %s", err_note)
                break
            seen = seen_payload
            if not isinstance(seen, dict):
                break
            logical = _polygon_body_error_message(seen, 200)
            if logical:
                break

        out = dict(data)
        out["results"] = merged_results
        out["next_url"] = None
        out["resultsCount"] = len(merged_results)
        return out

    def fetch_option_open_close(
        self,
        options_ticker: str,
        date: str,
        *,
        adjusted: bool = True,
    ) -> Dict[str, Any]:
        """GET /v1/open-close/{optionsTicker}/{date} — daily OHLC + pre/after-hours."""
        ot = (options_ticker or "").strip()
        d = (date or "").strip()
        if not ot or not d or not self._api_key:
            return {"error": "options_ticker, date, or api key missing"}
        params: Dict[str, Any] = {}
        if not adjusted:
            params["adjusted"] = "false"
        status, data = self._get(f"/v1/open-close/{ot}/{d}", params or None)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"error": err}
        return data if isinstance(data, dict) else {"error": "invalid response"}

    def fetch_option_previous_day(
        self,
        options_ticker: str,
        *,
        adjusted: bool = True,
    ) -> Dict[str, Any]:
        """GET /v2/aggs/ticker/{optionsTicker}/prev — previous trading day OHLC."""
        ot = (options_ticker or "").strip()
        if not ot or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        params: Dict[str, Any] = {}
        if not adjusted:
            params["adjusted"] = "false"
        status, data = self._get(f"/v2/aggs/ticker/{ot}/prev", params or None)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        return data if isinstance(data, dict) else {"results": []}

    # ── Stock aggregates (same Polygon paths as options; ticker e.g. AAPL) ──

    def fetch_stock_aggs(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        start_ms: int,
        end_ms: int,
    ) -> Dict[str, Any]:
        """GET /v2/aggs/ticker/{ticker}/range/... — custom-range OHLCV for a stock symbol."""
        return self.fetch_option_aggs(ticker, multiplier, timespan, start_ms, end_ms)

    def fetch_stock_open_close(
        self,
        ticker: str,
        date: str,
        *,
        adjusted: bool = True,
    ) -> Dict[str, Any]:
        """GET /v1/open-close/{ticker}/{date} — daily OHLC + pre/after-hours for a stock."""
        return self.fetch_option_open_close(ticker, date, adjusted=adjusted)

    def fetch_stock_previous_day(
        self,
        ticker: str,
        *,
        adjusted: bool = True,
    ) -> Dict[str, Any]:
        """GET /v2/aggs/ticker/{ticker}/prev — previous trading day OHLC for a stock."""
        return self.fetch_option_previous_day(ticker, adjusted=adjusted)

    def fetch_stock_grouped_daily(
        self,
        date: str,
        *,
        adjusted: bool = True,
    ) -> Dict[str, Any]:
        """GET /v2/aggs/grouped/locale/us/market/stocks/{date} — all US stocks for one date."""
        d = (date or "").strip()
        if not d or not self._api_key:
            return {"results": [], "error": "date or api key missing"}
        enc = quote(d, safe="")
        params: Dict[str, Any] = {"adjusted": "true" if adjusted else "false"}
        status, data = self._get(
            f"/v2/aggs/grouped/locale/us/market/stocks/{enc}",
            params,
        )
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    # ── Corporate actions (Stocks REST) ──

    def fetch_dividends(
        self, ticker: str, limit: int = 1000
    ) -> Dict[str, Any]:
        """GET /stocks/v1/dividends — current Stocks REST (replaces deprecated /v3/reference/dividends)."""
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        lim = min(max(int(limit), 1), 5000)
        params: Dict[str, Any] = {
            "ticker": ticker,
            "limit": lim,
            "sort": "ex_dividend_date.desc",
        }
        status, data = self._get("/stocks/v1/dividends", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_splits(
        self, ticker: str, limit: int = 1000
    ) -> Dict[str, Any]:
        """GET /stocks/v1/splits — current Stocks REST (replaces deprecated /v3/reference/splits)."""
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        lim = min(max(int(limit), 1), 5000)
        params: Dict[str, Any] = {
            "ticker": ticker,
            "limit": lim,
            "sort": "execution_date.desc",
        }
        status, data = self._get("/stocks/v1/splits", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    # ── Stock Fundamentals (vX/reference/financials + Short Interest / Short Volume / Float) ──

    _FISCAL_PERIOD_QUARTER_MAP = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}

    def _fetch_stock_financials_vx(
        self,
        ticker: str,
        section: Optional[str] = None,
        *,
        timeframe: Optional[str] = None,
        fiscal_year: Optional[int] = None,
        fiscal_quarter: Optional[int] = None,
        period_end: Optional[str] = None,
        filing_date: Optional[str] = None,
        limit: int = 10,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Call /vX/reference/financials (starter-tier) and optionally extract a section.

        section: 'income_statement' | 'balance_sheet' | 'cash_flow_statement' | None (full)
        """
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        params: Dict[str, Any] = {"ticker": ticker, "limit": min(max(int(limit), 1), 100)}
        if timeframe:
            tf = timeframe.strip().lower()
            if tf == "trailing_twelve_months":
                tf = "ttm"
            params["timeframe"] = tf
        if period_end:
            params["period_of_report_date"] = period_end
        if filing_date:
            params["filing_date"] = filing_date
        if sort:
            params["sort"] = sort
            params["order"] = "desc" if ".desc" in sort else "asc"
        status, data = self._get("/vX/reference/financials", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        if not isinstance(data, dict):
            return {"results": []}

        results = data.get("results")
        if not isinstance(results, list):
            return data

        # Client-side fiscal_year / fiscal_quarter filtering (vX doesn't support these as params)
        if fiscal_year is not None or fiscal_quarter is not None:
            filtered = []
            for row in results:
                if fiscal_year is not None and str(row.get("fiscal_year", "")) != str(fiscal_year):
                    continue
                if fiscal_quarter is not None:
                    fp = str(row.get("fiscal_period", "")).upper()
                    rq = self._FISCAL_PERIOD_QUARTER_MAP.get(fp)
                    if rq != fiscal_quarter:
                        continue
                filtered.append(row)
            results = filtered

        if section:
            transformed = []
            for row in results:
                fins = row.get("financials", {})
                sec_data = fins.get(section, {})
                flat: Dict[str, Any] = {
                    "start_date": row.get("start_date"),
                    "end_date": row.get("end_date"),
                    "filing_date": row.get("filing_date"),
                    "timeframe": row.get("timeframe"),
                    "fiscal_period": row.get("fiscal_period"),
                    "fiscal_year": row.get("fiscal_year"),
                    "company_name": row.get("company_name"),
                    "tickers": row.get("tickers"),
                }
                for field_name, field_val in sec_data.items():
                    if isinstance(field_val, dict):
                        flat[field_name] = field_val.get("value")
                    else:
                        flat[field_name] = field_val
                transformed.append(flat)
            out = dict(data)
            out["results"] = transformed
            return out

        return data

    def fetch_stock_income_statements(self, ticker: str, **kwargs: Any) -> Dict[str, Any]:
        """GET /vX/reference/financials → income_statement section."""
        return self._fetch_stock_financials_vx(ticker, "income_statement", **kwargs)

    def fetch_stock_balance_sheets(self, ticker: str, **kwargs: Any) -> Dict[str, Any]:
        """GET /vX/reference/financials → balance_sheet section."""
        return self._fetch_stock_financials_vx(ticker, "balance_sheet", **kwargs)

    def fetch_stock_cash_flow_statements(self, ticker: str, **kwargs: Any) -> Dict[str, Any]:
        """GET /vX/reference/financials → cash_flow_statement section."""
        return self._fetch_stock_financials_vx(ticker, "cash_flow_statement", **kwargs)

    def fetch_stock_ratios(
        self,
        ticker: str,
        *,
        limit: int = 10,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compute key ratios from /vX/reference/financials data."""
        data = self._fetch_stock_financials_vx(ticker, None, limit=limit, sort=sort)
        if data.get("error"):
            return data
        results = data.get("results")
        if not isinstance(results, list):
            return {"results": [], "error": "unexpected response"}

        ratios_list: List[Dict[str, Any]] = []
        for row in results:
            fins = row.get("financials", {})
            inc = fins.get("income_statement", {})
            bs = fins.get("balance_sheet", {})

            def _val(section: Dict, key: str) -> Optional[float]:
                v = section.get(key)
                if isinstance(v, dict):
                    v = v.get("value")
                if v is None:
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            revenue = _val(inc, "revenues")
            gross = _val(inc, "gross_profit")
            op_inc = _val(inc, "operating_income_loss")
            net_inc = _val(inc, "net_income_loss")
            assets = _val(bs, "assets")
            equity = _val(bs, "equity")
            liabilities = _val(bs, "liabilities")
            cur_assets = _val(bs, "current_assets")
            cur_liab = _val(bs, "current_liabilities")
            eps = _val(inc, "basic_earnings_per_share")
            diluted_eps = _val(inc, "diluted_earnings_per_share")

            entry: Dict[str, Any] = {
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "timeframe": row.get("timeframe"),
                "fiscal_period": row.get("fiscal_period"),
                "fiscal_year": row.get("fiscal_year"),
                "company_name": row.get("company_name"),
                "basic_earnings_per_share": eps,
                "diluted_earnings_per_share": diluted_eps,
                "return_on_equity": round(net_inc / equity, 4) if net_inc is not None and equity else None,
                "return_on_assets": round(net_inc / assets, 4) if net_inc is not None and assets else None,
                "debt_to_equity": round(liabilities / equity, 4) if liabilities is not None and equity else None,
                "current_ratio": round(cur_assets / cur_liab, 4) if cur_assets is not None and cur_liab else None,
                "gross_margin": round(gross / revenue, 4) if gross is not None and revenue else None,
                "operating_margin": round(op_inc / revenue, 4) if op_inc is not None and revenue else None,
                "net_margin": round(net_inc / revenue, 4) if net_inc is not None and revenue else None,
                "revenue": revenue,
                "net_income": net_inc,
                "total_assets": assets,
                "total_equity": equity,
                "total_liabilities": liabilities,
            }
            ratios_list.append(entry)

        out = dict(data)
        out["results"] = ratios_list
        return out

    def fetch_stock_short_interest(
        self,
        ticker: str,
        *,
        settlement_date: Optional[str] = None,
        limit: int = 10,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/v1/short-interest — shares sold short, days-to-cover, avg daily volume."""
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        params: Dict[str, Any] = {"ticker": ticker, "limit": min(max(int(limit), 1), 50000)}
        if settlement_date:
            params["settlement_date"] = settlement_date
        if sort:
            params["sort"] = sort
        status, data = self._get("/stocks/v1/short-interest", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_stock_short_volume(
        self,
        ticker: str,
        *,
        date: Optional[str] = None,
        limit: int = 10,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/v1/short-volume — daily short sale volume per venue + short_volume_ratio."""
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        params: Dict[str, Any] = {"ticker": ticker, "limit": min(max(int(limit), 1), 50000)}
        if date:
            params["date"] = date
        if sort:
            params["sort"] = sort
        status, data = self._get("/stocks/v1/short-volume", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_stock_float(
        self,
        ticker: str,
        *,
        limit: int = 10,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/vX/float — free_float shares and free_float_percent (experimental vX)."""
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        params: Dict[str, Any] = {"ticker": ticker, "limit": min(max(int(limit), 1), 5000)}
        if sort:
            params["sort"] = sort
        status, data = self._get("/stocks/vX/float", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    # ── Stocks Fundamentals v1 (flat REST: /stocks/financials/v1/...) ─────────

    def _fetch_financials_v1(
        self,
        path: str,
        *,
        tickers: Optional[str] = None,
        tickers_any_of: Optional[str] = None,
        min_ticker: Optional[str] = None,
        max_ticker: Optional[str] = None,
        timeframe: Optional[str] = None,
        fiscal_year: Optional[int] = None,
        fiscal_quarter: Optional[int] = None,
        limit: int = 50000,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 50000)}
        if tickers_any_of:
            params["tickers.any_of"] = tickers_any_of.strip()
        elif tickers:
            params["tickers"] = str(tickers).strip()
        if min_ticker:
            params["min_ticker"] = str(min_ticker).strip()
        if max_ticker:
            params["max_ticker"] = str(max_ticker).strip()
        if timeframe:
            tf = str(timeframe).strip().lower()
            if tf == "trailing_twelve_months":
                tf = "ttm"
            params["timeframe"] = tf
        if fiscal_year is not None:
            params["fiscal_year"] = int(fiscal_year)
        if fiscal_quarter is not None:
            params["fiscal_quarter"] = int(fiscal_quarter)
        if sort:
            params["sort"] = sort.strip()
        status, data = self._get(path, params)
        if status == 0:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_financials_v1_income_statements(
        self,
        *,
        tickers: Optional[str] = None,
        tickers_any_of: Optional[str] = None,
        min_ticker: Optional[str] = None,
        max_ticker: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 50000,
        sort: Optional[str] = "period_end.desc",
        fiscal_year: Optional[int] = None,
        fiscal_quarter: Optional[int] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/financials/v1/income-statements — flat quarterly/annual/ttm rows."""
        return self._fetch_financials_v1(
            "/stocks/financials/v1/income-statements",
            tickers=tickers,
            tickers_any_of=tickers_any_of,
            min_ticker=min_ticker,
            max_ticker=max_ticker,
            timeframe=timeframe,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            limit=limit,
            sort=sort,
        )

    def fetch_financials_v1_balance_sheets(
        self,
        *,
        tickers: Optional[str] = None,
        tickers_any_of: Optional[str] = None,
        min_ticker: Optional[str] = None,
        max_ticker: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 50000,
        sort: Optional[str] = "period_end.desc",
        fiscal_year: Optional[int] = None,
        fiscal_quarter: Optional[int] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/financials/v1/balance-sheets."""
        return self._fetch_financials_v1(
            "/stocks/financials/v1/balance-sheets",
            tickers=tickers,
            tickers_any_of=tickers_any_of,
            min_ticker=min_ticker,
            max_ticker=max_ticker,
            timeframe=timeframe,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            limit=limit,
            sort=sort,
        )

    def fetch_financials_v1_cash_flow_statements(
        self,
        *,
        tickers: Optional[str] = None,
        tickers_any_of: Optional[str] = None,
        min_ticker: Optional[str] = None,
        max_ticker: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 50000,
        sort: Optional[str] = "period_end.desc",
        fiscal_year: Optional[int] = None,
        fiscal_quarter: Optional[int] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/financials/v1/cash-flow-statements."""
        return self._fetch_financials_v1(
            "/stocks/financials/v1/cash-flow-statements",
            tickers=tickers,
            tickers_any_of=tickers_any_of,
            min_ticker=min_ticker,
            max_ticker=max_ticker,
            timeframe=timeframe,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            limit=limit,
            sort=sort,
        )

    def fetch_financials_v1_ratios(
        self,
        *,
        ticker: Optional[str] = None,
        tickers: Optional[str] = None,
        limit: int = 50000,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/financials/v1/ratios — TTM-derived daily snapshots (``results[].date``, ``ticker``).

        Query params documented as singular ``ticker``; Polygon often accepts ``tickers`` on other fundamentals.
        Defaults **omit ``sort`** (API default is ``ticker``); invalid sorts can yield empty ``results``.
        """
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        t = str(ticker or tickers or "").strip()
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 50000)}
        if t:
            params["ticker"] = t.upper()
        if sort:
            s = sort.strip()
            if s:
                params["sort"] = s

        status, data = self._get("/stocks/financials/v1/ratios", params)
        if status == 0:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}

        empty = False
        if not isinstance(data, dict):
            empty = True
        else:
            r0 = data.get("results")
            empty = not isinstance(r0, list) or len(r0) == 0

        if empty and t:
            alt: Dict[str, Any] = {"limit": params["limit"], "tickers": t.upper()}
            if "sort" in params:
                alt["sort"] = params["sort"]
            st2, data2 = self._get("/stocks/financials/v1/ratios", alt)
            if st2 >= 400:
                return data if isinstance(data, dict) else {"results": []}
            if isinstance(data2, dict):
                logical2 = _polygon_body_error_message(data2, st2)
                if logical2:
                    return {"results": [], "error": logical2}
                r2 = data2.get("results")
                if isinstance(r2, list) and len(r2) > 0:
                    return data2

        return data if isinstance(data, dict) else {"results": []}

    def fetch_stock_news(
        self,
        *,
        ticker: Optional[str] = None,
        published_utc_gte: Optional[str] = None,
        published_utc_lte: Optional[str] = None,
        limit: int = 10,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /v2/reference/news — stock market news articles."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 1000)}
        if ticker:
            params["ticker"] = ticker.strip().upper()
        if published_utc_gte:
            params["published_utc.gte"] = published_utc_gte
        if published_utc_lte:
            params["published_utc.lte"] = published_utc_lte
        if sort:
            params["sort"] = sort
        if order:
            params["order"] = order
        status, data = self._get("/v2/reference/news", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    # ── SEC Filings & Disclosures ──────────────────────────────────────────────

    def fetch_edgar_index(
        self,
        *,
        ticker: Optional[str] = None,
        cik: Optional[str] = None,
        form_type: Optional[str] = None,
        filing_date: Optional[str] = None,
        filing_date_gt: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        filing_date_lte: Optional[str] = None,
        limit: int = 100,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/filings/vX/index — EDGAR filing index search."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 50000)}
        if ticker: params["ticker"] = ticker.strip().upper()
        if cik: params["cik"] = cik.strip()
        if form_type: params["form_type"] = form_type
        if filing_date: params["filing_date"] = filing_date
        if filing_date_gt: params["filing_date.gt"] = filing_date_gt
        if filing_date_gte: params["filing_date.gte"] = filing_date_gte
        if filing_date_lt: params["filing_date.lt"] = filing_date_lt
        if filing_date_lte: params["filing_date.lte"] = filing_date_lte
        if sort: params["sort"] = sort
        status, data = self._get("/stocks/filings/vX/index", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_10k_sections(
        self,
        *,
        ticker: Optional[str] = None,
        cik: Optional[str] = None,
        section: Optional[str] = None,
        filing_date: Optional[str] = None,
        filing_date_gt: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        filing_date_lte: Optional[str] = None,
        period_end: Optional[str] = None,
        period_end_gte: Optional[str] = None,
        period_end_lte: Optional[str] = None,
        limit: int = 10,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/filings/10-K/vX/sections — plain-text sections from annual 10-K filings."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 99)}
        if ticker: params["ticker"] = ticker.strip().upper()
        if cik: params["cik"] = cik.strip()
        if section: params["section"] = section
        if filing_date: params["filing_date"] = filing_date
        if filing_date_gt: params["filing_date.gt"] = filing_date_gt
        if filing_date_gte: params["filing_date.gte"] = filing_date_gte
        if filing_date_lt: params["filing_date.lt"] = filing_date_lt
        if filing_date_lte: params["filing_date.lte"] = filing_date_lte
        if period_end: params["period_end"] = period_end
        if period_end_gte: params["period_end.gte"] = period_end_gte
        if period_end_lte: params["period_end.lte"] = period_end_lte
        if sort: params["sort"] = sort
        status, data = self._get("/stocks/filings/10-K/vX/sections", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_8k_text(
        self,
        *,
        ticker: Optional[str] = None,
        cik: Optional[str] = None,
        form_type: Optional[str] = None,
        filing_date: Optional[str] = None,
        filing_date_gt: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        filing_date_lte: Optional[str] = None,
        limit: int = 10,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/filings/8-K/vX/text — parsed plain-text from 8-K current report Items."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 99)}
        if ticker: params["ticker"] = ticker.strip().upper()
        if cik: params["cik"] = cik.strip()
        if form_type: params["form_type"] = form_type
        if filing_date: params["filing_date"] = filing_date
        if filing_date_gt: params["filing_date.gt"] = filing_date_gt
        if filing_date_gte: params["filing_date.gte"] = filing_date_gte
        if filing_date_lt: params["filing_date.lt"] = filing_date_lt
        if filing_date_lte: params["filing_date.lte"] = filing_date_lte
        if sort: params["sort"] = sort
        status, data = self._get("/stocks/filings/8-K/vX/text", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_13f_filings(
        self,
        *,
        filer_cik: Optional[str] = None,
        filing_date: Optional[str] = None,
        filing_date_gt: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        filing_date_lte: Optional[str] = None,
        limit: int = 100,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/filings/vX/13-F — institutional holdings from Form 13-F."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 1000)}
        if filer_cik: params["filer_cik"] = filer_cik.strip()
        if filing_date: params["filing_date"] = filing_date
        if filing_date_gt: params["filing_date.gt"] = filing_date_gt
        if filing_date_gte: params["filing_date.gte"] = filing_date_gte
        if filing_date_lt: params["filing_date.lt"] = filing_date_lt
        if filing_date_lte: params["filing_date.lte"] = filing_date_lte
        if sort: params["sort"] = sort
        status, data = self._get("/stocks/filings/vX/13-F", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_risk_factors(
        self,
        *,
        ticker: Optional[str] = None,
        cik: Optional[str] = None,
        filing_date: Optional[str] = None,
        filing_date_gt: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        filing_date_lte: Optional[str] = None,
        limit: int = 100,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/filings/vX/risk-factors — standardized risk factor disclosures from SEC filings."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 49999)}
        if ticker: params["ticker"] = ticker.strip().upper()
        if cik: params["cik"] = cik.strip()
        if filing_date: params["filing_date"] = filing_date
        if filing_date_gt: params["filing_date.gt"] = filing_date_gt
        if filing_date_gte: params["filing_date.gte"] = filing_date_gte
        if filing_date_lt: params["filing_date.lt"] = filing_date_lt
        if filing_date_lte: params["filing_date.lte"] = filing_date_lte
        if sort: params["sort"] = sort
        status, data = self._get("/stocks/filings/vX/risk-factors", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_risk_categories(
        self,
        *,
        taxonomy: Optional[int] = None,
        primary_category: Optional[str] = None,
        secondary_category: Optional[str] = None,
        tertiary_category: Optional[str] = None,
        limit: int = 200,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/taxonomies/vX/risk-factors — hierarchical risk factor taxonomy."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 999)}
        if taxonomy is not None: params["taxonomy"] = taxonomy
        if primary_category: params["primary_category"] = primary_category
        if secondary_category: params["secondary_category"] = secondary_category
        if tertiary_category: params["tertiary_category"] = tertiary_category
        if sort: params["sort"] = sort
        status, data = self._get("/stocks/taxonomies/vX/risk-factors", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_form_3(
        self,
        *,
        issuer_cik: Optional[str] = None,
        owner_cik: Optional[str] = None,
        tickers: Optional[str] = None,
        form_type: Optional[str] = None,
        filing_date: Optional[str] = None,
        filing_date_gt: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        filing_date_lte: Optional[str] = None,
        limit: int = 100,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/filings/vX/form-3 — initial insider ownership statements (Form 3)."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 10000)}
        if issuer_cik: params["issuer_cik"] = issuer_cik.strip()
        if owner_cik: params["owner_cik"] = owner_cik.strip()
        if tickers: params["tickers"] = tickers.strip().upper()
        if form_type: params["form_type"] = form_type
        if filing_date: params["filing_date"] = filing_date
        if filing_date_gt: params["filing_date.gt"] = filing_date_gt
        if filing_date_gte: params["filing_date.gte"] = filing_date_gte
        if filing_date_lt: params["filing_date.lt"] = filing_date_lt
        if filing_date_lte: params["filing_date.lte"] = filing_date_lte
        if sort: params["sort"] = sort
        status, data = self._get("/stocks/filings/vX/form-3", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_form_4(
        self,
        *,
        issuer_cik: Optional[str] = None,
        owner_cik: Optional[str] = None,
        tickers: Optional[str] = None,
        form_type: Optional[str] = None,
        transaction_code: Optional[str] = None,
        filing_date: Optional[str] = None,
        filing_date_gt: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        filing_date_lte: Optional[str] = None,
        limit: int = 100,
        sort: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /stocks/filings/vX/form-4 — insider ownership changes (Form 4)."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": min(max(int(limit), 1), 10000)}
        if issuer_cik: params["issuer_cik"] = issuer_cik.strip()
        if owner_cik: params["owner_cik"] = owner_cik.strip()
        if tickers: params["tickers"] = tickers.strip().upper()
        if form_type: params["form_type"] = form_type
        if transaction_code: params["transaction_code"] = transaction_code
        if filing_date: params["filing_date"] = filing_date
        if filing_date_gt: params["filing_date.gt"] = filing_date_gt
        if filing_date_gte: params["filing_date.gte"] = filing_date_gte
        if filing_date_lt: params["filing_date.lt"] = filing_date_lt
        if filing_date_lte: params["filing_date.lte"] = filing_date_lte
        if sort: params["sort"] = sort
        status, data = self._get("/stocks/filings/vX/form-4", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_ipos_for_ticker(self, ticker: str, limit: int = 100) -> Dict[str, Any]:
        """GET /v3/reference/ipos?ticker=… — IPO reference (per-ticker filter)."""
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": [], "error": "ticker or api key missing"}
        lim = min(max(int(limit), 1), 1000)
        status, data = self._get(
            "/v3/reference/ipos",
            {"ticker": ticker, "limit": lim},
        )
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_ticker_events(self, ticker: str) -> Dict[str, Any]:
        """GET /v3/reference/tickers/{ticker}/events — ticker lifecycle (e.g. symbol changes)."""
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": {}, "error": "ticker or api key missing"}
        enc = quote(ticker, safe="")
        status, data = self._get(f"/v3/reference/tickers/{enc}/events", {})
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": {}, "error": err}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": {}, "error": logical}
        return data if isinstance(data, dict) else {"results": {}}

    # ── Tickers reference (Stocks REST, read-only) ──

    def fetch_reference_tickers(
        self,
        *,
        ticker: Optional[str] = None,
        instrument_type: Optional[str] = None,
        market: Optional[str] = None,
        exchange: Optional[str] = None,
        search: Optional[str] = None,
        active: Optional[bool] = None,
        date: Optional[str] = None,
        limit: int = 100,
        sort: str = "ticker",
        order: str = "asc",
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /v3/reference/tickers — paginated ticker universe (Polygon Stocks reference)."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        lim = min(max(int(limit), 1), 1000)
        params: Dict[str, Any] = {"limit": lim, "sort": sort, "order": order}
        if ticker:
            params["ticker"] = ticker.strip()
        if instrument_type:
            params["type"] = instrument_type.strip()
        if market:
            params["market"] = market.strip()
        if exchange:
            params["exchange"] = exchange.strip()
        if search:
            params["search"] = search.strip()
        if active is not None:
            params["active"] = "true" if active else "false"
        if date:
            params["date"] = date.strip()
        if cursor:
            params["cursor"] = cursor.strip()
        status, data = self._get("/v3/reference/tickers", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": _as_error_str(err)}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_ticker_detail(self, ticker: str, *, date: Optional[str] = None) -> Dict[str, Any]:
        """GET /v3/reference/tickers/{ticker} — single ticker metadata."""
        sym = (ticker or "").strip()
        if not sym or not self._api_key:
            return {"error": "ticker or api key missing"}
        enc = quote(sym, safe="")
        params: Dict[str, Any] = {}
        if date:
            params["date"] = date.strip()
        status, data = self._get(f"/v3/reference/tickers/{enc}", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"error": _as_error_str(err)}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"error": logical}
        return data if isinstance(data, dict) else {"error": "invalid response"}

    def fetch_ticker_types(
        self,
        *,
        asset_class: Optional[str] = None,
        locale: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /v3/reference/tickers/types — instrument type codes."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {}
        if asset_class:
            params["asset_class"] = asset_class.strip()
        if locale:
            params["locale"] = locale.strip()
        status, data = self._get("/v3/reference/tickers/types", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": _as_error_str(err)}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"results": [], "error": logical}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_related_companies(self, ticker: str) -> Dict[str, Any]:
        """GET /v1/related-companies/{ticker} — peer / related tickers."""
        sym = (ticker or "").strip().upper()
        if not sym or not self._api_key:
            return {"error": "ticker or api key missing"}
        enc = quote(sym, safe="")
        status, data = self._get(f"/v1/related-companies/{enc}")
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"error": _as_error_str(err)}
        if isinstance(data, dict):
            logical = _polygon_body_error_message(data, status)
            if logical:
                return {"error": logical}
        return data if isinstance(data, dict) else {"error": "invalid response"}

    # ── Market Ops (cross-asset reference, read-only) ──

    def fetch_market_conditions(
        self,
        *,
        asset_class: Optional[str] = None,
        data_type: Optional[str] = None,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        """GET /v3/reference/conditions — trade/quote condition codes."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {"limit": limit}
        if asset_class:
            params["asset_class"] = asset_class
        if data_type:
            params["data_type"] = data_type
        status, data = self._get("/v3/reference/conditions", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_market_exchanges(self, *, asset_class: Optional[str] = None, locale: Optional[str] = None) -> Dict[str, Any]:
        """GET /v3/reference/exchanges — list of exchanges."""
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        params: Dict[str, Any] = {}
        if asset_class:
            params["asset_class"] = asset_class
        if locale:
            params["locale"] = locale
        status, data = self._get("/v3/reference/exchanges", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_market_holidays(self) -> Dict[str, Any]:
        """GET /v1/marketstatus/upcoming — upcoming market holidays.

        Polygon returns a bare JSON array (e.g. ``[{"exchange": "NYSE", ...}]``),
        not a ``{"results": [...]}`` envelope. Normalize to the envelope shape so
        callers can keep using ``data["results"]`` consistently.
        """
        if not self._api_key:
            return {"results": [], "error": "api key missing"}
        status, data = self._get("/v1/marketstatus/upcoming")
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        if isinstance(data, list):
            return {"results": data}
        if isinstance(data, dict):
            if "results" in data:
                return data
            # Some tiers wrap under different keys; accept any list value.
            for v in data.values():
                if isinstance(v, list):
                    return {"results": v}
        return {"results": []}

    def fetch_market_status(self) -> Dict[str, Any]:
        """GET /v1/marketstatus/now — current trading status."""
        if not self._api_key:
            return {"error": "api key missing"}
        status, data = self._get("/v1/marketstatus/now")
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"error": err}
        return data if isinstance(data, dict) else {"error": "invalid response"}

    # ── Technical Indicators (cross-asset, read-only) ──

    def _fetch_indicator(
        self,
        indicator: str,
        ticker: str,
        *,
        timespan: str = "day",
        window: int = 14,
        series_type: str = "close",
        adjusted: bool = True,
        order: str = "desc",
        limit: int = 100,
        expand_underlying: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Generic helper for GET /v1/indicators/{indicator}/{ticker}."""
        ticker = (ticker or "").strip().upper()
        if not ticker or not self._api_key:
            return {"results": {}, "error": "ticker or api key missing"}
        params: Dict[str, Any] = {
            "timespan": timespan,
            "window": window,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
            "order": order,
            "limit": limit,
            "expand_underlying": str(expand_underlying).lower(),
        }
        if extra:
            params.update(extra)
        status, data = self._get(f"/v1/indicators/{indicator}/{ticker}", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": {}, "error": err}
        return data if isinstance(data, dict) else {"results": {}}

    def fetch_indicator_sma(self, ticker: str, **kwargs: Any) -> Dict[str, Any]:
        """GET /v1/indicators/sma/{ticker} — Simple Moving Average."""
        return self._fetch_indicator("sma", ticker, **kwargs)

    def fetch_indicator_ema(self, ticker: str, **kwargs: Any) -> Dict[str, Any]:
        """GET /v1/indicators/ema/{ticker} — Exponential Moving Average."""
        return self._fetch_indicator("ema", ticker, **kwargs)

    def fetch_indicator_rsi(self, ticker: str, **kwargs: Any) -> Dict[str, Any]:
        """GET /v1/indicators/rsi/{ticker} — Relative Strength Index."""
        return self._fetch_indicator("rsi", ticker, **kwargs)

    def fetch_indicator_macd(
        self,
        ticker: str,
        *,
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """GET /v1/indicators/macd/{ticker} — MACD."""
        extra = {
            "short_window": short_window,
            "long_window": long_window,
            "signal_window": signal_window,
        }
        return self._fetch_indicator(
            "macd", ticker, extra=extra, **kwargs,
        )

    # ── Trades & Quotes (Options REST) ──

    def fetch_last_trade(self, options_ticker: str) -> Dict[str, Any]:
        """GET /v2/last/trade/{optionsTicker} — most recent trade for a contract."""
        ot = (options_ticker or "").strip()
        if not ot or not self._api_key:
            return {"results": {}, "error": "options_ticker or api key missing"}
        status, data = self._get(f"/v2/last/trade/{ot}")
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": {}, "error": err}
        return data if isinstance(data, dict) else {"results": {}}

    def fetch_option_quotes(
        self,
        options_ticker: str,
        *,
        timestamp_gte: Optional[str] = None,
        timestamp_lte: Optional[str] = None,
        limit: int = 100,
        sort: str = "timestamp",
        order: str = "asc",
    ) -> Dict[str, Any]:
        """GET /v3/quotes/{optionsTicker} — historical BBO quotes for a contract."""
        ot = (options_ticker or "").strip()
        if not ot or not self._api_key:
            return {"results": [], "error": "options_ticker or api key missing"}
        params: Dict[str, Any] = {
            "limit": min(int(limit), 50000),
            "sort": sort,
            "order": order,
        }
        if timestamp_gte:
            params["timestamp.gte"] = timestamp_gte
        if timestamp_lte:
            params["timestamp.lte"] = timestamp_lte
        status, data = self._get(f"/v3/quotes/{ot}", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        return data if isinstance(data, dict) else {"results": []}

    def fetch_option_trades(
        self,
        options_ticker: str,
        *,
        timestamp_gte: Optional[str] = None,
        timestamp_lte: Optional[str] = None,
        limit: int = 100,
        sort: str = "timestamp",
        order: str = "asc",
    ) -> Dict[str, Any]:
        """GET /v3/trades/{optionsTicker} — tick-level trade data for a contract."""
        ot = (options_ticker or "").strip()
        if not ot or not self._api_key:
            return {"results": [], "error": "options_ticker or api key missing"}
        params: Dict[str, Any] = {
            "limit": min(int(limit), 50000),
            "sort": sort,
            "order": order,
        }
        if timestamp_gte:
            params["timestamp.gte"] = timestamp_gte
        if timestamp_lte:
            params["timestamp.lte"] = timestamp_lte
        status, data = self._get(f"/v3/trades/{ot}", params)
        if status >= 400:
            err = data.get("error", data) if isinstance(data, dict) else str(data)
            return {"results": [], "error": err}
        return data if isinstance(data, dict) else {"results": []}

    def sleep_backoff(self, attempt: int) -> None:
        time.sleep(min(2.0 ** attempt, 30.0))
