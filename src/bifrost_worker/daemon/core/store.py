"""In-memory runtime store: positions, spot, last_hedge_time, daily_hedge_count, daily_pnl."""

import logging
import threading
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


class Store:
    """Thread-safe runtime state updated by connector callbacks and daemon."""

    def __init__(self):
        self._lock = threading.Lock()

        self._positions: List[Any] = []
        self._stock_position = 0  # net shares of underlying

        self._underlying_bid: Optional[float] = None
        self._underlying_ask: Optional[float] = None
        self._underlying_price: Optional[float] = None

        self._last_hedge_time: Optional[float] = None
        self._last_hedge_price: Optional[float] = None

        self._daily_hedge_count = 0
        self._daily_hedge_date: Optional[str] = None  # YYYY-MM-DD
        self._daily_pnl_usd = 0.0

        # R-A1: account summary (account id + key values from IB); trading uses first account
        self._account_id: Optional[str] = None
        self._account_summary: Optional[dict] = None  # e.g. {"NetLiquidation": "123", "TotalCashValue": "456"}
        # R-A1 multi-account: list of { account_id, summary: {}, positions: [] } for monitoring
        self._accounts_data: List[dict] = []

    def set_positions(self, positions: List[Any], stock_position: int = 0) -> None:
        with self._lock:
            self._positions = list(positions)
            self._stock_position = stock_position

    def get_positions(self) -> List[Any]:
        with self._lock:
            return list(self._positions)

    def get_stock_position(self) -> int:
        with self._lock:
            return self._stock_position

    def set_underlying_quote(self, bid: Optional[float], ask: Optional[float]) -> None:
        with self._lock:
            self._underlying_bid = bid
            self._underlying_ask = ask
            if bid is not None and ask is not None:
                self._underlying_price = (bid + ask) / 2.0

    def set_underlying_price(self, price: Optional[float]) -> None:
        with self._lock:
            self._underlying_price = price

    def get_underlying_price(self) -> Optional[float]:
        with self._lock:
            return self._underlying_price

    def get_bid(self) -> Optional[float]:
        with self._lock:
            return self._underlying_bid

    def get_ask(self) -> Optional[float]:
        with self._lock:
            return self._underlying_ask

    def get_spread_pct(self) -> Optional[float]:
        """Bid-ask spread as pct of mid. None if no quote."""
        with self._lock:
            if (
                self._underlying_bid is None
                or self._underlying_ask is None
                or self._underlying_bid <= 0
            ):
                return None
            mid = (self._underlying_bid + self._underlying_ask) / 2.0
            if mid <= 0:
                return None
            return 100.0 * (self._underlying_ask - self._underlying_bid) / mid

    def set_last_hedge_time(self, t: Optional[float]) -> None:
        with self._lock:
            self._last_hedge_time = t

    def get_last_hedge_time(self) -> Optional[float]:
        with self._lock:
            return self._last_hedge_time

    def set_last_hedge_price(self, price: Optional[float]) -> None:
        with self._lock:
            self._last_hedge_price = price

    def get_last_hedge_price(self) -> Optional[float]:
        with self._lock:
            return self._last_hedge_price

    def set_daily_hedge_count(self, n: int, as_of_date: Optional[str] = None) -> None:
        with self._lock:
            self._daily_hedge_count = n
            self._daily_hedge_date = as_of_date

    def get_daily_hedge_count(self) -> int:
        with self._lock:
            return self._daily_hedge_count

    def inc_daily_hedge_count(self) -> int:
        """Increment and return new count. Caller should reset on new day if needed."""
        with self._lock:
            self._daily_hedge_count += 1
            return self._daily_hedge_count

    def set_daily_pnl(self, pnl: float) -> None:
        with self._lock:
            self._daily_pnl_usd = pnl

    def get_daily_pnl(self) -> float:
        with self._lock:
            return self._daily_pnl_usd

    def add_fill_pnl(self, pnl_delta: float) -> None:
        with self._lock:
            self._daily_pnl_usd += pnl_delta

    def set_account_summary(self, account_id: Optional[str], summary: Optional[dict]) -> None:
        """R-A1: store account id and key values (NetLiquidation, TotalCashValue, BuyingPower, etc.)."""
        with self._lock:
            self._account_id = account_id
            self._account_summary = dict(summary) if summary else None

    def get_account_id(self) -> Optional[str]:
        """R-A1: current account id from last account summary."""
        with self._lock:
            return self._account_id

    def get_account_summary(self) -> Optional[dict]:
        """R-A1: last account summary dict (tag -> value)."""
        with self._lock:
            return dict(self._account_summary) if self._account_summary else None

    def set_accounts_data(self, accounts: List[dict]) -> None:
        """R-A1 multi-account: set list of { account_id, summary: {}, positions: [] } for monitoring."""
        with self._lock:
            self._accounts_data = [dict(a) for a in accounts]

    def get_accounts_data(self) -> List[dict]:
        """R-A1 multi-account: get list of account dicts (account_id, summary, positions) for monitoring."""
        with self._lock:
            return [dict(a) for a in self._accounts_data]
