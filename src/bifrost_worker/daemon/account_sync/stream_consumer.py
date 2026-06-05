"""Redis Stream consumer for Account Sync Daemon (XREADGROUP + ACK)."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from bifrost_core.core.realtime.ib_account_keys import IB_ACCOUNT_STREAM_KEY
from bifrost_worker.daemon.account_sync.redis_keys import (
    ACCOUNT_SYNC_CONSUMER_GROUP,
    ACCOUNT_SYNC_CONSUMER_NAME,
)

logger = logging.getLogger(__name__)


class AccountStreamConsumer:
    """Wraps XREADGROUP against ib:account:stream:v1."""

    def __init__(self, r: Any) -> None:
        self._r = r
        self._group = ACCOUNT_SYNC_CONSUMER_GROUP
        self._consumer = ACCOUNT_SYNC_CONSUMER_NAME
        self._stream = IB_ACCOUNT_STREAM_KEY

    def ensure_group(self) -> None:
        """Create consumer group if it does not exist (idempotent)."""
        try:
            self._r.xgroup_create(self._stream, self._group, id="$", mkstream=True)
            logger.info("Created consumer group %s on %s", self._group, self._stream)
        except Exception as e:
            if "BUSYGROUP" in str(e):
                logger.debug("Consumer group %s already exists", self._group)
            else:
                raise

    def read(self, count: int = 10, block_ms: int = 5000) -> List[Dict[str, Any]]:
        """XREADGROUP; returns list of parsed payloads (newest first after merge).

        Each returned dict has the full snapshot JSON plus ``_stream_id``.
        """
        try:
            resp = self._r.xreadgroup(
                self._group,
                self._consumer,
                {self._stream: ">"},
                count=count,
                block=block_ms,
            )
        except Exception as e:
            logger.warning("xreadgroup failed: %s", e)
            return []

        if not resp:
            return []

        entries: List[Dict[str, Any]] = []
        ids_to_ack: List[str] = []
        for _stream_name, messages in resp:
            for msg_id, fields in messages:
                mid = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
                raw = fields.get(b"payload") or fields.get("payload")
                if raw is None:
                    ids_to_ack.append(mid)
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON in stream entry %s", mid)
                    ids_to_ack.append(mid)
                    continue
                data["_stream_id"] = mid
                entries.append(data)
                ids_to_ack.append(mid)

        if ids_to_ack:
            try:
                self._r.xack(self._stream, self._group, *ids_to_ack)
            except Exception as e:
                logger.warning("xack failed: %s", e)

        return entries

    def pending_count(self) -> int:
        """Return number of pending (unacknowledged) messages for lag monitoring."""
        try:
            info = self._r.xpending(self._stream, self._group)
            return int(info.get("pending", 0) if isinstance(info, dict) else (info[0] if info else 0))
        except Exception:
            return 0

    @staticmethod
    def merge_latest(entries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Given multiple stream entries, return the one with highest version."""
        if not entries:
            return None
        best = entries[0]
        best_v = int(best.get("version") or 0)
        for e in entries[1:]:
            v = int(e.get("version") or 0)
            if v > best_v:
                best = e
                best_v = v
        return best
