"""Celery bars backfill via IB Operator (Redis RPC) when ``ib_operator.use_for_celery_bars`` is true."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from bifrost_core.ib_operator.client import IbOperatorClient
from bifrost_core.ib_operator.config import effective_ib_operator_settings

logger = logging.getLogger(__name__)


class IbOperatorBarsAdapter:
    """Duck-compatible with MarketIbClient for run_one_backfill: ensure_connected + fetch_bars_range."""

    def __init__(self, gw: IbOperatorClient, *, backfill_timeout_sec: float) -> None:
        self._gw = gw
        self._backfill_timeout_sec = float(backfill_timeout_sec)
        self._last_ping_ok = False
        self.client_id = 0
        self.name = "CeleryBarsViaIbOperator"

    @classmethod
    def from_merged_config(cls, config: Dict[str, Any], gw: IbOperatorClient) -> IbOperatorBarsAdapter:
        s = effective_ib_operator_settings(config)
        to = float(s.get("bars_backfill_request_timeout_sec") or 7200)
        return cls(gw, backfill_timeout_sec=to)

    @property
    def connected(self) -> bool:
        return self._last_ping_ok

    async def disconnect(self) -> None:
        self._last_ping_ok = False

    async def ensure_connected(self) -> None:
        r = await self._gw.request_async("ping", {}, timeout_sec=30.0, caller="celery_bars")
        if not r.get("ok"):
            self._last_ping_ok = False
            err = r.get("error") or "ping_failed"
            raise RuntimeError(f"IB Operator unreachable: {err}")
        self._last_ping_ok = True
        data = r.get("data")
        host = data.get("host") if isinstance(data, dict) else None
        if isinstance(host, dict):
            try:
                self.client_id = int(host.get("client_id") or 0)
            except (TypeError, ValueError):
                self.client_id = 0

    async def fetch_bars_range(
        self,
        symbol: str,
        period: str,
        *,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
        interval_sec: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {"symbol": (symbol or "").strip(), "period": (period or "1 D").strip()}
        if start_ts is not None:
            payload["start_ts"] = float(start_ts)
        if end_ts is not None:
            payload["end_ts"] = float(end_ts)
        if interval_sec is not None and interval_sec > 0:
            payload["interval_sec"] = float(interval_sec)
        r = await self._gw.request_async(
            "fetch_bars_range",
            payload,
            timeout_sec=self._backfill_timeout_sec,
            caller="celery_bars",
        )
        if not r.get("ok"):
            err = r.get("error") or "fetch_bars_range_failed"
            raise RuntimeError(err)
        data = r.get("data") or {}
        bars = data.get("bars") if isinstance(data, dict) else None
        if not isinstance(bars, list):
            return []
        return bars
