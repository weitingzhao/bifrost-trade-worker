"""Diff-based persistence engine: compare incoming snapshot with cached state and only write changes."""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _position_contract_key(p: Dict[str, Any]) -> str:
    sym = (p.get("symbol") or "").strip()
    sec = (p.get("secType") or p.get("sec_type") or "").strip()
    exp = p.get("lastTradeDateOrContractMonth") or p.get("expiry") or ""
    strike_raw = p.get("strike")
    try:
        strike_f = float(strike_raw) if strike_raw is not None else None
    except (TypeError, ValueError):
        strike_f = None
    if strike_f is not None and not math.isfinite(strike_f):
        strike_f = None
    rt = (p.get("right") or "").strip()
    if sec.upper() == "OPT":
        return f"{sym}|{sec}|{exp}|{strike_f}|{rt}"
    return f"{sym}|{sec}|||"


class AccountSyncDiffEngine:
    """Compares incoming snapshot with an in-memory cache; writes only changed rows to PG."""

    def __init__(self) -> None:
        self._account_cache: Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]] = {}
        self._position_cache: Dict[Tuple[str, str], Tuple[Optional[float], Optional[float]]] = {}
        self._seen_exec_ids: set = set()

        self.accounts_synced = 0
        self.positions_synced = 0
        self.executions_synced = 0
        self.open_orders_synced = 0

    def sync_all(self, conn: Any, payload: Dict[str, Any]) -> None:
        """Run all sync steps for one snapshot payload."""
        accounts_list = payload.get("accounts_snapshot") or []
        exec_rows = payload.get("last_execution_rows") or []
        open_orders = payload.get("open_orders") or []

        self._sync_accounts(conn, accounts_list)
        self._sync_positions(conn, accounts_list)
        self._sync_executions(conn, exec_rows)
        self._sync_open_orders(conn, open_orders)
        conn.commit()

    def _sync_accounts(self, conn: Any, accounts_list: List[Dict[str, Any]]) -> None:
        from bifrost_core.persistence.postgres.accounts_sync import _parse_summary_floats, _json_safe
        from psycopg2.extras import Json

        count = 0
        with conn.cursor() as cur:
            for acc in accounts_list:
                if not isinstance(acc, dict):
                    continue
                account_id = str(acc.get("account_id") or acc.get("account") or "").strip()
                if not account_id:
                    continue
                summary = acc.get("summary") or {}
                if not isinstance(summary, dict):
                    summary = {}
                nl, tc, bp, extra = _parse_summary_floats(summary)
                cached = self._account_cache.get(account_id)
                if cached == (nl, tc, bp):
                    continue
                self._account_cache[account_id] = (nl, tc, bp)
                extra_json = _json_safe(extra) if extra else None
                cur.execute(
                    """
                    INSERT INTO account (account_id, updated_at, net_liquidation, total_cash, buying_power, summary_extra)
                    VALUES (%s, now(), %s, %s, %s, %s)
                    ON CONFLICT (account_id) DO UPDATE SET
                        updated_at = now(),
                        net_liquidation = EXCLUDED.net_liquidation,
                        total_cash = EXCLUDED.total_cash,
                        buying_power = EXCLUDED.buying_power,
                        summary_extra = EXCLUDED.summary_extra
                    """,
                    (account_id, nl, tc, bp, Json(extra_json) if extra_json is not None else None),
                )
                count += 1
        self.accounts_synced += count

    def _sync_positions(self, conn: Any, accounts_list: List[Dict[str, Any]]) -> None:
        count = 0
        with conn.cursor() as cur:
            for acc in accounts_list:
                if not isinstance(acc, dict):
                    continue
                account_id = str(acc.get("account_id") or acc.get("account") or "").strip()
                if not account_id:
                    continue
                positions = acc.get("positions") or []
                seen_keys: List[str] = []
                if isinstance(positions, list):
                    for p in positions:
                        if not isinstance(p, dict):
                            continue
                        sym = p.get("symbol") or ""
                        sec = p.get("secType") or p.get("sec_type") or ""
                        ex = p.get("exchange") or ""
                        curr = p.get("currency") or ""
                        pos_val = p.get("position")
                        try:
                            pos_f = float(pos_val) if pos_val is not None else None
                        except (TypeError, ValueError):
                            pos_f = None
                        if pos_f is not None and not math.isfinite(pos_f):
                            pos_f = None
                        avg = p.get("avgCost") or p.get("avg_cost")
                        try:
                            avg_f = float(avg) if avg is not None else None
                        except (TypeError, ValueError):
                            avg_f = None
                        if avg_f is not None and not math.isfinite(avg_f):
                            avg_f = None
                        exp = p.get("lastTradeDateOrContractMonth") or p.get("expiry") or ""
                        strike_raw = p.get("strike")
                        try:
                            strike_f = float(strike_raw) if strike_raw is not None else None
                        except (TypeError, ValueError):
                            strike_f = None
                        if strike_f is not None and not math.isfinite(strike_f):
                            strike_f = None
                        rt = p.get("right") or ""
                        contract_key = _position_contract_key(p)
                        cache_key = (account_id, contract_key)
                        cached = self._position_cache.get(cache_key)
                        if cached == (pos_f, avg_f):
                            seen_keys.append(contract_key)
                            continue
                        self._position_cache[cache_key] = (pos_f, avg_f)
                        cur.execute(
                            """
                            INSERT INTO account_positions (account_id, symbol, sec_type, exchange, currency, position, avg_cost, expiry, strike, option_right, contract_key, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                            ON CONFLICT (account_id, contract_key) DO UPDATE SET
                                exchange = EXCLUDED.exchange,
                                currency = EXCLUDED.currency,
                                position = EXCLUDED.position,
                                avg_cost = EXCLUDED.avg_cost,
                                expiry = EXCLUDED.expiry,
                                strike = EXCLUDED.strike,
                                option_right = EXCLUDED.option_right,
                                updated_at = now()
                            """,
                            (account_id, sym, sec, ex, curr, pos_f, avg_f, exp or None, strike_f, rt or None, contract_key),
                        )
                        seen_keys.append(contract_key)
                        count += 1

                # Remove closed positions
                if seen_keys:
                    cur.execute(
                        "DELETE FROM account_positions WHERE account_id = %s AND (contract_key IS NULL OR contract_key != ALL(%s::text[]))",
                        (account_id, seen_keys),
                    )
                else:
                    cur.execute("DELETE FROM account_positions WHERE account_id = %s", (account_id,))
                # Evict cache entries for this account that are no longer present
                stale = [k for k in self._position_cache if k[0] == account_id and k[1] not in seen_keys]
                for k in stale:
                    del self._position_cache[k]
        self.positions_synced += count

    def _sync_executions(self, conn: Any, rows: List[Dict[str, Any]]) -> None:
        """INSERT ON CONFLICT DO NOTHING for executions_raw_tws + upsert commissions."""
        if not rows:
            return
        count = 0
        with conn.cursor() as cur:
            for r in rows:
                exec_id = r.get("exec_id")
                if not exec_id:
                    continue
                if exec_id in self._seen_exec_ids:
                    continue
                self._seen_exec_ids.add(exec_id)

                exec_time = r.get("time")
                if exec_time is not None:
                    try:
                        from datetime import datetime, timezone
                        if isinstance(exec_time, (int, float)):
                            exec_dt = datetime.fromtimestamp(exec_time, tz=timezone.utc)
                        else:
                            exec_dt = exec_time
                    except Exception:
                        exec_dt = None
                else:
                    exec_dt = None

                cols = (
                    "account_id, exec_id, exec_time, symbol, sec_type, side, quantity, price, source, "
                    "expiry, strike, option_right, exchange, order_id, cum_qty, contract_key"
                )
                vals = (
                    r.get("account_id"), exec_id, exec_dt, r.get("symbol"), r.get("sec_type"),
                    r.get("side"), r.get("quantity"), r.get("price"), r.get("source"),
                    r.get("expiry"), r.get("strike"), r.get("option_right"), r.get("exchange"),
                    r.get("order_id"), r.get("cum_qty"), r.get("contract_key"),
                )
                ph = ", ".join(["%s"] * len(vals))
                cur.execute(
                    f"INSERT INTO executions_raw_tws ({cols}) VALUES ({ph}) "
                    "ON CONFLICT (exec_id) WHERE exec_id IS NOT NULL AND exec_id != '' DO NOTHING",
                    vals,
                )
                count += 1

                commission = r.get("commission")
                realized_pnl = r.get("realized_pnl")
                comm_currency = r.get("commission_currency") or r.get("currency")
                if commission is not None or realized_pnl is not None:
                    cur.execute(
                        """
                        INSERT INTO account_execution_commissions (exec_id, commission, realized_pnl, currency, updated_at)
                        VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (exec_id) DO UPDATE SET
                            commission = COALESCE(EXCLUDED.commission, account_execution_commissions.commission),
                            realized_pnl = COALESCE(EXCLUDED.realized_pnl, account_execution_commissions.realized_pnl),
                            currency = COALESCE(EXCLUDED.currency, account_execution_commissions.currency),
                            updated_at = now()
                        """,
                        (exec_id, commission, realized_pnl, comm_currency),
                    )
        self.executions_synced += count

    def _sync_open_orders(self, conn: Any, orders: List[Dict[str, Any]]) -> None:
        if not orders:
            return
        count = 0
        with conn.cursor() as cur:
            cur.execute("DELETE FROM daemon_open_orders")
            for o in orders:
                order_id = o.get("order_id") or o.get("orderId")
                if order_id is None:
                    continue
                cur.execute(
                    """
                    INSERT INTO daemon_open_orders (
                        order_id, account_id, contract_key, symbol, sec_type,
                        action, total_quantity, filled_quantity, remaining,
                        status, order_type, limit_price, aux_price, updated_ts
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                    """,
                    (
                        order_id,
                        o.get("account_id") or o.get("account"),
                        o.get("contract_key"),
                        o.get("symbol"),
                        o.get("secType") or o.get("sec_type"),
                        o.get("action") or o.get("side"),
                        o.get("totalQuantity") or o.get("total_quantity"),
                        o.get("filledQuantity") or o.get("filled"),
                        o.get("remaining"),
                        o.get("status"),
                        o.get("orderType") or o.get("order_type"),
                        o.get("lmtPrice") or o.get("limit_price"),
                        o.get("auxPrice") or o.get("aux_price"),
                    ),
                )
                count += 1
        self.open_orders_synced += count
