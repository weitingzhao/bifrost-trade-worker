"""Engine IB edge mode: read account snapshot from Redis (IB Account Agent); no in-process IBConnector."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_ACCOUNT_SYNC_DAEMON_ENABLED = os.environ.get("ACCOUNT_SYNC_DAEMON_ENABLED", "").strip().lower() in ("1", "true", "yes")


def _redis_sync_client(cfg: dict):
    import redis

    from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url

    url = format_redis_url(effective_redis_dict(cfg, default_db=0))
    return redis.from_url(url, decode_responses=True)


def ib_edge_snapshot_ready(cfg: dict) -> bool:
    """True if Redis has a non-empty ib:account:snapshot:v1 JSON."""
    from bifrost_core.core.realtime.ib_account_keys import IB_ACCOUNT_SNAPSHOT_KEY

    r = _redis_sync_client(cfg)
    try:
        raw = r.get(IB_ACCOUNT_SNAPSHOT_KEY)
        if not raw:
            return False
        data = json.loads(raw)
        return bool(data.get("accounts_snapshot") or data.get("open_orders"))
    except Exception:
        return False
    finally:
        try:
            r.close()
        except Exception:
            pass


def _wrap_position_item(p: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize flat position dict (from position_to_dict) for portfolio helpers."""
    sym = str(p.get("symbol") or "")
    st = str(p.get("secType") or "")
    return {
        "contract": {"symbol": sym, "secType": st},
        "position": float(p.get("position") or 0),
        "avgCost": p.get("avgCost"),
        "account": p.get("account"),
    }


async def refresh_accounts_from_redis_edge(app: Any) -> None:
    """Load accounts_snapshot / open_orders / execution rows from Redis; update store and sink (no IB)."""
    from bifrost_core.portfolio.positions.portfolio import get_stock_shares
    from bifrost_core.core.realtime.ib_account_keys import IB_ACCOUNT_SNAPSHOT_KEY

    cfg = app.config
    r = _redis_sync_client(cfg)
    try:
        raw = r.get(IB_ACCOUNT_SNAPSHOT_KEY)
    finally:
        try:
            r.close()
        except Exception:
            pass
    if not raw:
        logger.warning("[ib_edge] no account snapshot in Redis yet")
        return
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("[ib_edge] snapshot JSON invalid: %s", e)
        return

    accounts_list: List[Dict[str, Any]] = list(data.get("accounts_snapshot") or [])
    app.store.set_accounts_data(accounts_list)

    host_id: Optional[str] = None
    host_summary: Optional[dict] = None
    if app._host_account_id and accounts_list:
        for a in accounts_list:
            if a.get("account_id") == app._host_account_id:
                host_id = app._host_account_id
                host_summary = a.get("summary")
                break
    if host_id is None and accounts_list:
        host_id = accounts_list[0].get("account_id")
        host_summary = accounts_list[0].get("summary")

    app.store.set_account_summary(host_id, host_summary)

    flat: List[Any] = []
    for a in accounts_list:
        for p in a.get("positions") or []:
            if isinstance(p, dict):
                flat.append(_wrap_position_item(p))
    symbol = getattr(app, "symbol", "") or ""
    stock_shares = get_stock_shares(flat, symbol) if symbol else 0
    app.store.set_positions(flat, stock_shares)
    if symbol:
        app._set_active_symbol(app._infer_active_symbol(flat))

    oo = data.get("open_orders") or []
    if not _ACCOUNT_SYNC_DAEMON_ENABLED:
        if app._status_sink and hasattr(app._status_sink, "write_open_orders"):
            try:
                app._status_sink.write_open_orders(oo)
            except Exception as e:
                logger.debug("[ib_edge] write_open_orders: %s", e)

        rows = data.get("last_execution_rows") or []
        if rows and app._status_sink and hasattr(app._status_sink, "write_account_executions"):
            try:
                app._status_sink.write_account_executions(rows)
            except Exception as e:
                logger.debug("[ib_edge] write_account_executions: %s", e)
    else:
        logger.debug("[ib_edge] ACCOUNT_SYNC_DAEMON_ENABLED — skipping PG writes (Account Sync Daemon handles persistence)")

    logger.info(
        "[ib_edge] snapshot applied accounts=%s open_orders=%s",
        len(accounts_list),
        len(oo),
    )
