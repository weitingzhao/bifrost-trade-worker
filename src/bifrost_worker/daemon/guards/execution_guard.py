"""Hedge Execution FSM guard: order-level risk gate before sending a hedge.

Used when transitioning toward SEND in HedgeFSM (fsm/hedge_fsm.py).
apply_hedge_gates() in strategy/hedge_gate.py calls allow_hedge() to decide whether
the order may be sent. Stateful: cooldown, daily count, circuit breaker, earnings blackout.
For Trading FSM transition guards (pure predicates), see trading_guard.py in this package.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

logger = logging.getLogger(__name__)


class ExecutionGuard:
    """Order-send gate for the Hedge Execution FSM: cooldown, max daily hedges, position/earnings/circuit breaker, RTH, spread, min price move."""

    def __init__(
        self,
        cooldown_sec: int = 60,
        max_daily_hedge_count: int = 50,
        max_position_shares: int = 2000,
        max_daily_loss_usd: float = 5000.0,
        max_net_delta_shares: Optional[float] = None,
        max_spread_pct: Optional[float] = None,
        min_price_move_pct: float = 0.0,
        earnings_dates: Optional[List[str]] = None,
        blackout_days_before: int = 3,
        blackout_days_after: int = 1,
        trading_hours_only: bool = True,
    ):
        self.cooldown_sec = cooldown_sec
        self.max_daily_hedge_count = max_daily_hedge_count
        self.max_position_shares = max_position_shares
        self.max_daily_loss_usd = max_daily_loss_usd
        self.max_net_delta_shares = max_net_delta_shares
        self.max_spread_pct = max_spread_pct
        self.min_price_move_pct = min_price_move_pct
        self.earnings_dates = [d for d in (earnings_dates or []) if d]
        self.blackout_days_before = blackout_days_before
        self.blackout_days_after = blackout_days_after
        self.trading_hours_only = trading_hours_only
        self._last_hedge_time: Optional[float] = None
        self._daily_hedge_count = 0
        self._daily_hedge_date: Optional[date] = None
        self._circuit_breaker = False

    def set_last_hedge_time(self, t: Optional[float] = None) -> None:
        """Set last hedge timestamp (e.g. time.time())."""
        self._last_hedge_time = t

    def set_daily_hedge_count(self, n: int, as_of_date: Optional[date] = None) -> None:
        """Set daily hedge count and optionally the date it applies to."""
        self._daily_hedge_count = n
        self._daily_hedge_date = as_of_date or date.today()

    def set_circuit_breaker(self, tripped: bool) -> None:
        """Set or clear max daily loss circuit breaker."""
        self._circuit_breaker = tripped

    def set_daily_pnl(self, pnl_usd: float) -> None:
        """Trip circuit breaker if daily P&L <= -max_daily_loss_usd."""
        if pnl_usd <= -self.max_daily_loss_usd:
            self._circuit_breaker = True
            logger.warning("Circuit breaker: daily P&L %.2f <= -%.2f", pnl_usd, self.max_daily_loss_usd)

    def _reset_daily_if_new_day(self) -> None:
        today = date.today()
        if self._daily_hedge_date is not None and self._daily_hedge_date != today:
            self._daily_hedge_count = 0
            self._daily_hedge_date = today

    def _in_earnings_blackout(self) -> bool:
        today = date.today()
        for d_str in self.earnings_dates:
            try:
                ed = datetime.strptime(d_str.strip(), "%Y-%m-%d").date()
            except ValueError:
                continue
            start = ed - timedelta(days=self.blackout_days_before)
            end = ed + timedelta(days=self.blackout_days_after)
            if start <= today <= end:
                return True
        return False

    @staticmethod
    def is_rth_et() -> bool:
        """True if current time is US RTH (9:30-16:00 ET). Naive: no DST."""
        try:
            from zoneinfo import ZoneInfo
            et = datetime.now(ZoneInfo("America/New_York"))
        except (ImportError, OSError):
            et = datetime.now(timezone.utc)
        return (et.hour, et.minute) >= (9, 30) and (et.hour, et.minute) < (16, 0)

    def allow_hedge(
        self,
        now_ts: float,
        current_stock_position: int,
        hedge_side: str,
        hedge_quantity: int,
        portfolio_delta: Optional[float] = None,  # pylint: disable=unused-argument
        spot: Optional[float] = None,
        last_hedge_price: Optional[float] = None,
        spread_pct: Optional[float] = None,
        force_hedge: bool = False,
    ) -> tuple[bool, str]:
        """
        Returns (allowed: bool, reason: str).
        Gates: circuit breaker, RTH, earnings blackout, cooldown (skipped if force_hedge),
        max daily count, max position, spread, min price move.
        """
        self._reset_daily_if_new_day()

        if self._circuit_breaker:
            return False, "circuit_breaker"

        if self.trading_hours_only and not self.is_rth_et():
            return False, "outside_rth"

        if self._in_earnings_blackout():
            return False, "earnings_blackout"

        if not force_hedge and self._last_hedge_time is not None and (now_ts - self._last_hedge_time) < self.cooldown_sec:
            return False, "cooldown"

        if self._daily_hedge_count >= self.max_daily_hedge_count:
            return False, "max_daily_hedge_count"

        after_position = current_stock_position + (hedge_quantity if hedge_side == "BUY" else -hedge_quantity)
        if abs(after_position) > self.max_position_shares:
            return False, "max_position"

        if self.max_spread_pct is not None and spread_pct is not None:
            if spread_pct > self.max_spread_pct:
                return False, "spread_too_wide"

        if self.min_price_move_pct > 0 and spot is not None and last_hedge_price is not None and last_hedge_price > 0:
            move_pct = 100.0 * abs(spot - last_hedge_price) / last_hedge_price
            if move_pct < self.min_price_move_pct:
                return False, "min_price_move"

        return True, "ok"

    def record_hedge_sent(self) -> None:
        """Call after sending a hedge order (optimistic update)."""
        self._reset_daily_if_new_day()
        self._daily_hedge_count += 1
        import time
        self._last_hedge_time = time.time()

    def update_config(
        self,
        cooldown_sec: Optional[int] = None,
        max_daily_hedge_count: Optional[int] = None,
        max_position_shares: Optional[int] = None,
        max_daily_loss_usd: Optional[float] = None,
        max_net_delta_shares: Optional[float] = None,
        max_spread_pct: Optional[float] = None,
        min_price_move_pct: Optional[float] = None,
        earnings_dates: Optional[List[str]] = None,
        blackout_days_before: Optional[int] = None,
        blackout_days_after: Optional[int] = None,
        trading_hours_only: Optional[bool] = None,
    ) -> None:
        """Update config from hot-reload (only non-None values)."""
        if cooldown_sec is not None:
            self.cooldown_sec = cooldown_sec
        if max_daily_hedge_count is not None:
            self.max_daily_hedge_count = max_daily_hedge_count
        if max_position_shares is not None:
            self.max_position_shares = max_position_shares
        if max_daily_loss_usd is not None:
            self.max_daily_loss_usd = max_daily_loss_usd
        if max_net_delta_shares is not None:
            self.max_net_delta_shares = max_net_delta_shares
        if max_spread_pct is not None:
            self.max_spread_pct = max_spread_pct
        if min_price_move_pct is not None:
            self.min_price_move_pct = min_price_move_pct
        if earnings_dates is not None:
            self.earnings_dates = [d for d in earnings_dates if d]
        if blackout_days_before is not None:
            self.blackout_days_before = blackout_days_before
        if blackout_days_after is not None:
            self.blackout_days_after = blackout_days_after
        if trading_hours_only is not None:
            self.trading_hours_only = trading_hours_only
