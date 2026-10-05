"""Push-driven raw_broker.account / raw_broker.positions sync (2026-10-05).

The IB Gateway plugin rewrites ``ib:account:snapshot:v1`` on redis-ib every couple of seconds and
announces each version on ``ib:account:notify``. The daemon listens to that channel and writes an
account to the Golden Source tables only when its summary or positions changed since the last
write. The hourly accounts refresh in the heartbeat stays as the failover and force-writes.

Before this, heartbeats re-upserted the cached list every few seconds with ``updated_at = now()``
while the list itself was re-read only hourly: an account whose TWS dropped at 11:00 ET kept
"updating" until 11:48, and the 16:20 ET position snapshot held a 15:46 read.

Only accounts present in a fresh plugin snapshot are written, so an account whose TWS is gone keeps
its last ``updated_at``. Read-only towards redis-ib; no order path (D10).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

#: A plugin snapshot older than this is not written: the key has no TTL, so a stopped gateway
#: leaves its last snapshot behind.
SNAPSHOT_MAX_AGE_SEC = 120.0
#: Coalesce bursts of notifications into at most one read per this many seconds.
MIN_SYNC_INTERVAL_SEC = 1.0
#: Back-off before re-subscribing after a redis-ib error.
RESUBSCRIBE_BACKOFF_SEC = 5.0


def _fingerprint(account: Dict[str, Any]) -> str:
    body = {"summary": account.get("summary"), "positions": account.get("positions")}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(raw.encode()).hexdigest()


def fresh_accounts(data: Optional[Dict[str, Any]], now: Optional[float] = None) -> Optional[List[dict]]:
    """The snapshot's accounts, or None when the snapshot is missing or older than the limit."""
    if not data:
        return None
    try:
        updated_at = float(data.get("updated_at") or 0)
    except (TypeError, ValueError):
        updated_at = 0.0
    age = (now if now is not None else time.time()) - updated_at
    if age > SNAPSHOT_MAX_AGE_SEC:
        logger.warning("[account_push] plugin snapshot is %.0fs old; not written", age)
        return None
    return [a for a in (data.get("accounts_snapshot") or []) if isinstance(a, dict)]


def read_plugin_snapshot(config: dict) -> Optional[Dict[str, Any]]:
    import redis

    from bifrost_core.core.realtime.ib_account_keys import IB_ACCOUNT_SNAPSHOT_KEY
    from bifrost_core.core.redis_url import effective_ib_redis_dict, format_redis_url

    r = redis.from_url(format_redis_url(effective_ib_redis_dict(config, default_db=0)), decode_responses=True)
    try:
        raw = r.get(IB_ACCOUNT_SNAPSHOT_KEY)
    finally:
        try:
            r.close()
        except Exception:
            pass
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("[account_push] snapshot JSON invalid: %s", e)
        return None


class AccountTablesWriter:
    """Writes changed accounts to raw_broker over its own Golden Source connection."""

    def __init__(self, config: dict) -> None:
        self._config = config
        self._conn: Any = None
        self._written: Dict[str, str] = {}

    def changed(self, accounts: List[dict]) -> List[dict]:
        out = []
        for a in accounts:
            aid = str(a.get("account_id") or a.get("account") or "").strip()
            if aid and self._written.get(aid) != _fingerprint(a):
                out.append(a)
        return out

    def _ensure_conn(self) -> Any:
        if self._conn is not None and not self._conn.closed:
            return self._conn
        from bifrost_core.monitor.reader.write_support import open_conn

        try:
            self._conn = open_conn(self._config, golden=True)
            with self._conn.cursor() as cur:
                cur.execute("SET lock_timeout = '5s'")
                cur.execute("SET idle_in_transaction_session_timeout = '60s'")
            self._conn.commit()
        except Exception as e:
            logger.warning("[account_push] golden_source connect failed: %s", e)
            self._conn = None
        return self._conn

    def write(self, accounts: List[dict], *, force: bool = False) -> int:
        """Write the changed accounts (all of them with ``force``); returns how many were written."""
        from bifrost_core.core.daemon_flags import daemon_broker_writes_off
        from bifrost_core.persistence.postgres.accounts_sync import sync_accounts_snapshot_to_tables

        if daemon_broker_writes_off():
            return 0
        todo = list(accounts) if force else self.changed(accounts)
        if not todo:
            return 0
        conn = self._ensure_conn()
        if conn is None:
            return 0
        try:
            sync_accounts_snapshot_to_tables(conn, todo)
            conn.commit()
        except Exception as e:
            logger.warning("[account_push] raw_broker write failed: %s", e)
            try:
                conn.rollback()
            except Exception:
                self._conn = None
            return 0
        for a in todo:
            aid = str(a.get("account_id") or a.get("account") or "").strip()
            if aid:
                self._written[aid] = _fingerprint(a)
        return len(todo)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


async def sync_once(app: Any, *, force: bool = False) -> int:
    """Read the plugin snapshot and write what changed (everything with ``force``)."""
    writer: Optional[AccountTablesWriter] = getattr(app, "_account_tables_writer", None)
    if writer is None:
        return 0
    try:
        data = await asyncio.to_thread(read_plugin_snapshot, app.config)
    except Exception as e:
        logger.warning("[account_push] snapshot read failed: %s", e)
        return 0
    accounts = fresh_accounts(data)
    if not accounts:
        return 0
    n = await asyncio.to_thread(writer.write, accounts, force=force)
    if n:
        logger.debug("[account_push] wrote %s account(s) force=%s", n, force)
    return n


async def run_push_listener(app: Any) -> None:
    """Subscribe to ib:account:notify and sync on each version; re-subscribe on errors."""
    import redis.asyncio as aioredis

    from bifrost_core.core.realtime.ib_account_keys import IB_ACCOUNT_NOTIFY_CHANNEL
    from bifrost_core.core.redis_url import effective_ib_redis_dict, format_redis_url

    url = format_redis_url(effective_ib_redis_dict(app.config, default_db=0))
    while True:
        r = None
        try:
            r = aioredis.from_url(url, decode_responses=True)
            pubsub = r.pubsub()
            await pubsub.subscribe(IB_ACCOUNT_NOTIFY_CHANNEL)
            logger.info("[account_push] subscribed to %s", IB_ACCOUNT_NOTIFY_CHANNEL)
            await sync_once(app)
            last_sync = time.monotonic()
            pending = False
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=MIN_SYNC_INTERVAL_SEC)
                if msg is not None:
                    pending = True
                if pending and time.monotonic() - last_sync >= MIN_SYNC_INTERVAL_SEC:
                    pending = False
                    last_sync = time.monotonic()
                    await sync_once(app)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[account_push] listener error: %s; re-subscribing", e)
            await asyncio.sleep(RESUBSCRIBE_BACKOFF_SEC)
        finally:
            if r is not None:
                try:
                    await (r.aclose() if hasattr(r, "aclose") else r.close())
                except Exception:
                    pass
