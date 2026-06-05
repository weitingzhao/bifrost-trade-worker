"""IB connection errors for bars backfill (no bifrost-socket dependency at import time)."""

from __future__ import annotations


class IBConnectionDroppedError(ConnectionError):
    """Raised when the IB socket drops during bars fetch (same semantics as bifrost_socket)."""
