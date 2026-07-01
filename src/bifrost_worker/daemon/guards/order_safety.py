"""Hard safety latch: block all IB order RPC from the trading daemon (P5B observe / cutover)."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def hard_block_ib_orders() -> bool:
    """True when env BIFROST_DAEMON_HARD_NO_ORDERS is set (K8s prod observe mode)."""
    return os.environ.get("BIFROST_DAEMON_HARD_NO_ORDERS", "").strip().lower() in _TRUTHY


def apply_hard_order_block(app: Any) -> None:
    """Force paper/mock paths and drop IB Operator client — cannot be overridden by DB gates."""
    if not hard_block_ib_orders():
        app._hard_block_ib_orders = False
        return
    app._hard_block_ib_orders = True
    app.paper_trade = True
    app.mock_hedging = True
    app._operator_client = None
    logger.warning(
        "BIFROST_DAEMON_HARD_NO_ORDERS: IB Operator disabled; "
        "paper_trade=true mock_hedging=true (no live orders)"
    )
