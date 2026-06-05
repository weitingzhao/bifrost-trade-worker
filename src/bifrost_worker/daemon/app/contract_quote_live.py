"""R-M6 contract_quote_live from Redis quotes (IB Ingestor). No in-process IB subscriptions in daemon."""

import logging
import math
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def _clear_control_message(app: Any) -> None:
    if app._status_sink and hasattr(app._status_sink, "write_daemon_control_message"):
        app._status_sink.write_daemon_control_message(None)


async def release_ticker_subscriptions(app: Any) -> None:
    """Clear status reporting; IB Ingestor owns market subscriptions."""
    _clear_control_message(app)
    if app._status_sink and hasattr(app._status_sink, "write_daemon_subscribed_tickers"):
        app._status_sink.write_daemon_subscribed_tickers([])


def _parse_contract_key(contract_key: str) -> dict:
    """Parse contract_key (symbol|sec_type|expiry|strike|right) into meta dict. Returns {} if invalid."""
    if not contract_key or "|" not in contract_key:
        return {}
    parts = contract_key.split("|")
    if len(parts) < 5:
        return {}
    try:
        strike = float(parts[3]) if parts[3] else None
    except (TypeError, ValueError):
        strike = None
    return {
        "symbol": (parts[0] or "").strip(),
        "sec_type": (parts[1] or "OPT").strip().upper() or "OPT",
        "expiry": (parts[2] or "").strip(),
        "strike": strike,
        "option_right": (parts[4] or "C").strip().upper() or "C",
    }


def on_ticker_for_contract_key(app: Any, contract_key: str, ticker: Any) -> None:
    """On OPT ticker update: build row and write to contract_quote_live. Does not write Redis."""
    if not app._status_sink or not hasattr(app._status_sink, "write_contract_quote_live"):
        return
    meta = _parse_contract_key(contract_key)
    if not meta:
        return
    bid = getattr(ticker, "bid", None)
    ask = getattr(ticker, "ask", None)
    last = getattr(ticker, "last", None)
    try:
        bid_f = float(bid) if bid is not None else None
        ask_f = float(ask) if ask is not None else None
        last_f = float(last) if last is not None else None
    except (TypeError, ValueError):
        bid_f = ask_f = last_f = None
    if bid_f is not None and not math.isfinite(bid_f):
        bid_f = None
    if ask_f is not None and not math.isfinite(ask_f):
        ask_f = None
    if last_f is not None and not math.isfinite(last_f):
        last_f = None
    if bid_f is None and ask_f is None and last_f is None:
        return
    mid = (bid_f + ask_f) / 2.0 if bid_f is not None and ask_f is not None else last_f
    if mid is not None and not math.isfinite(mid):
        mid = last_f
    row = {
        "contract_key": contract_key,
        "symbol": meta["symbol"],
        "sec_type": meta["sec_type"],
        "expiry": meta["expiry"],
        "strike": meta["strike"],
        "option_right": meta["option_right"],
        "bid": bid_f,
        "ask": ask_f,
        "last": last_f,
        "mid": mid,
    }
    try:
        app._status_sink.write_contract_quote_live([row])
    except Exception as e:
        logger.debug("on_ticker_for_contract_key write_contract_quote_live %s: %s", contract_key, e)


async def init_ticker_subscriptions(app: Any) -> None:
    """No-op: STK/OPT market data subscriptions are owned by IB Ingestor."""
    _clear_control_message(app)


async def refresh_ticker_subscriptions(app: Any) -> None:
    """No-op; heartbeat reports subscribed symbols from Redis reader to status sink."""
    return


def get_position_stk_instruments(app: Any) -> dict:
    """从 accounts_data 聚合持仓中的 STK 标的，返回 contract_key -> meta（symbol, sec_type, expiry, strike, option_right, exchange, currency）。"""
    instruments: dict = {}
    accounts = app.store.get_accounts_data()
    if not accounts:
        return instruments
    for acc in accounts:
        positions = acc.get("positions") or []
        if not isinstance(positions, list):
            continue
        for p in positions:
            if not isinstance(p, dict):
                continue
            sym = (p.get("symbol") or "").strip()
            if not sym:
                continue
            sec = (p.get("secType") or p.get("sec_type") or "").strip()
            sec_u = sec.upper()
            if sec_u != "STK":
                continue
            ex = (p.get("exchange") or "").strip() or "SMART"
            curr = (p.get("currency") or "").strip() or "USD"
            contract_key = f"{sym}|{sec_u}|||"
            if contract_key in instruments:
                continue
            instruments[contract_key] = {
                "symbol": sym,
                "sec_type": sec_u,
                "expiry": None,
                "strike": None,
                "option_right": None,
                "exchange": ex,
                "currency": curr,
            }
    return instruments


async def refresh_position_prices(app: Any) -> None:
    """R-M6: update contract_quote_live from Redis quotes for position STK symbols."""
    sync_contract_quote_live_from_redis(app)


def sync_contract_quote_live_from_redis(app: Any) -> None:
    """R-M6: refresh contract_quote_live from Redis STK quotes (IB Ingestor tick keys via reader.get_quotes)."""
    if not app._status_sink or not hasattr(
        app._status_sink, "write_contract_quote_live"
    ):
        return
    rq_read = getattr(app, "_redis_quotes_reader", None)
    if not rq_read or not rq_read.available:
        return
    instruments = get_position_stk_instruments(app)
    if not instruments:
        return
    symbols = [m["symbol"] for m in instruments.values()]
    quotes = rq_read.get_quotes(symbols)
    if not quotes:
        return
    symbol_to_ck = {m["symbol"]: ck for ck, m in instruments.items()}
    rows = []
    for q in quotes:
        sym = q.get("symbol")
        ck = symbol_to_ck.get(sym) if sym else None
        if not ck:
            continue
        meta = instruments[ck]
        bid = q.get("bid")
        ask = q.get("ask")
        last = q.get("last")
        try:
            bid_f = float(bid) if bid is not None else None
            ask_f = float(ask) if ask is not None else None
            last_f = float(last) if last is not None else None
        except (TypeError, ValueError):
            continue
        if bid_f is not None and not math.isfinite(bid_f):
            bid_f = None
        if ask_f is not None and not math.isfinite(ask_f):
            ask_f = None
        if last_f is not None and not math.isfinite(last_f):
            last_f = None
        if last_f is None and (bid_f is None or ask_f is None):
            continue
        mid = (
            (bid_f + ask_f) / 2.0
            if bid_f is not None and ask_f is not None
            else last_f
        )
        if mid is None:
            mid = last_f
        if mid is not None and not math.isfinite(mid):
            continue
        rows.append(
            {
                "contract_key": ck,
                "symbol": meta["symbol"],
                "sec_type": meta["sec_type"],
                "expiry": meta["expiry"],
                "strike": meta["strike"],
                "option_right": meta["option_right"],
                "last": last_f,
                "bid": bid_f,
                "ask": ask_f,
                "mid": mid,
            }
        )
    if rows:
        app._status_sink.write_contract_quote_live(rows)
        logger.debug(
            "[R-M6] sync_contract_quote_live_from_redis: %s rows from Redis",
            len(rows),
        )
