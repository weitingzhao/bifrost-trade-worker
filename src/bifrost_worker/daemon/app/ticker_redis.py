"""Ticker callback and Redis quotes; threadsafe hedge trigger. Used by GsTrading."""

import asyncio
import logging
import math
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


def on_ticker(app: Any, ticker: Any) -> None:
    """Called on each ticker update from IB (may be from IB thread). Delegates to on_ticker_for_symbol."""
    on_ticker_for_symbol(app, app.symbol, ticker)


def on_ticker_for_symbol(app: Any, symbol: str, ticker: Any) -> None:
    """Called on each ticker update from IB for a symbol (may be from IB thread).
    For strategy symbol: update store, write Redis, trigger hedge eval.
    For other symbols (Watchlist STK): write Redis only."""
    try:
        if symbol == app.symbol:
            app._market_data.touch_ts()
            bid = getattr(ticker, "bid", None)
            ask = getattr(ticker, "ask", None)
            if bid is not None and ask is not None:
                try:
                    b, a = float(bid), float(ask)
                    if math.isfinite(b) and math.isfinite(a):
                        app.store.set_underlying_quote(b, a)
                except (TypeError, ValueError):
                    pass
            else:
                last = getattr(ticker, "last", None)
                if last is not None:
                    try:
                        L = float(last)
                        if math.isfinite(L):
                            app.store.set_underlying_price(L)
                    except (TypeError, ValueError):
                        pass
        # Live quotes to Redis are written by IB Ingestor; daemon does not write quote keys.
        # Hedge evaluation is driven only by heartbeat (control_heartbeat), not by every ticker update.
    except Exception as e:
        logger.debug("ticker callback error for %s: %s", symbol, e)


def quote_payload_from_ticker(symbol: str, ticker: Any) -> Optional[dict]:
    """Build quote dict for Redis from ticker (symbol, bid, ask, last, ts). Used for non-strategy symbols.
    Returns None if price is NaN, inf, or empty — such events are discarded and not written to Redis."""
    bid = getattr(ticker, "bid", None)
    ask = getattr(ticker, "ask", None)
    last = getattr(ticker, "last", None)
    try:
        bid = float(bid) if bid is not None else None
        ask = float(ask) if ask is not None else None
        last = float(last) if last is not None else None
    except (TypeError, ValueError):
        bid = ask = last = None
    if bid is not None and not math.isfinite(bid):
        bid = None
    if ask is not None and not math.isfinite(ask):
        ask = None
    if last is not None and not math.isfinite(last):
        last = None
    if last is None and (bid is None or ask is None):
        return None
    if last is None and bid is not None and ask is not None:
        last = (float(bid) + float(ask)) / 2.0
    if last is None or not math.isfinite(last):
        return None
    return {
        "symbol": symbol,
        "bid": bid,
        "ask": ask,
        "last": last,
        "ts": time.time(),
    }


def quote_payload(app: Any) -> Optional[dict]:
    """Build quote dict for Redis from store (symbol, bid, ask, last, ts). Returns None if price is NaN/inf/empty."""
    if not app.symbol:
        return None
    bid = app.store.get_bid()
    ask = app.store.get_ask()
    last = app.store.get_underlying_price()
    if last is None and bid is not None and ask is not None:
        try:
            last = (float(bid) + float(ask)) / 2.0
        except (TypeError, ValueError):
            last = None
    if last is None:
        return None
    try:
        last_f = float(last)
        if not math.isfinite(last_f):
            return None
    except (TypeError, ValueError):
        return None
    bid_f = float(bid) if bid is not None else None
    ask_f = float(ask) if ask is not None else None
    if bid_f is not None and not math.isfinite(bid_f):
        bid_f = None
    if ask_f is not None and not math.isfinite(ask_f):
        ask_f = None
    return {
        "symbol": app.symbol,
        "bid": bid_f,
        "ask": ask_f,
        "last": last_f,
        "ts": time.time(),
    }


def eval_hedge_threadsafe(app: Any) -> None:
    """Threadsafe: schedule _on_tick to be run safely from any thread using call_soon_threadsafe."""
    if app._fsm_daemon.is_running() and app._loop and app._loop.is_running():
        app._loop.call_soon_threadsafe(
            lambda: asyncio.ensure_future(app._eval_hedge_sync(), loop=app._loop)
        )


async def eval_hedge_sync(app: Any) -> None:
    """Run FSM-driven tick once (under lock)."""
    async with app._hedge_lock:
        await app._eval_hedge()
