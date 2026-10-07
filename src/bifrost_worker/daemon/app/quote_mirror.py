"""Observe-only STK quote mirror (``daemon.quote_mirror``).

Default off, so a running daemon keeps not writing ``contract_quote_live``.
Independent of ``mock_hedging``, which stays the hedge guard. This module
does not place, modify, or hedge orders.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_TRUE_STRINGS = frozenset({"1", "true", "yes", "on"})


def quote_mirror_from_config(config: dict | None) -> bool:
    """Read ``daemon.quote_mirror``. Missing or unrecognized values stay off."""
    if not isinstance(config, dict):
        return False
    daemon_cfg = config.get("daemon")
    if not isinstance(daemon_cfg, dict):
        return False
    raw = daemon_cfg.get("quote_mirror", False)
    if isinstance(raw, str):
        return raw.strip().lower() in _TRUE_STRINGS
    return raw is True


def quote_mirror_enabled(app: Any) -> bool:
    return getattr(app, "quote_mirror", False) is True


async def observe_quote_mirror(app: Any) -> None:
    """Refresh ``contract_quote_live`` from Redis when the flag is on.

    Does not call hedge or order placement.
    """
    if not quote_mirror_enabled(app):
        return
    if not getattr(app, "_contract_quote_live_initialized", False):
        try:
            await app._refresh_position_prices()
            app._contract_quote_live_initialized = True
        except Exception as e:
            logger.warning("R-M6 initial refresh_position_prices: %s", e)
    try:
        reader = getattr(app, "_redis_quotes_reader", None)
        if reader is not None and reader.available:
            app._sync_contract_quote_live_from_redis()
        else:
            await app._refresh_position_prices()
    except Exception as e:
        logger.warning("R-M6 contract_quote_live sync failed: %s", e)
