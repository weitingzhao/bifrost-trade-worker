"""Ticker-subscription control commands.

IB Ingestor owns market data. These handlers only clear or report daemon status.
They do not read or write brokerage quotes.
"""

from typing import Any


def _clear_control_message(app: Any) -> None:
    if app._status_sink and hasattr(app._status_sink, "write_daemon_control_message"):
        app._status_sink.write_daemon_control_message(None)


async def release_ticker_subscriptions(app: Any) -> None:
    """Clear status reporting; IB Ingestor owns market subscriptions."""
    _clear_control_message(app)
    if app._status_sink and hasattr(app._status_sink, "write_daemon_subscribed_tickers"):
        app._status_sink.write_daemon_subscribed_tickers([])


async def init_ticker_subscriptions(app: Any) -> None:
    """No-op: STK/OPT market data subscriptions are owned by IB Ingestor."""
    _clear_control_message(app)


async def refresh_ticker_subscriptions(app: Any) -> None:
    """No-op; heartbeat reports subscribed symbols from Redis reader to status sink."""
    return
