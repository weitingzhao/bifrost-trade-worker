"""Pool fill: per-contract GET /v3/snapshot/options/{u}/{ticker} for incomplete option_snapshots rows."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Tuple

from bifrost_worker.data.massive.snapshot_chain_ingest import (
    apply_chain_snapshot_item,
    contract_snapshot_api_response_to_chain_item,
)

logger = logging.getLogger(__name__)


def _fetch_column_fill_targets(
    cur: Any,
    sym: str,
    max_contracts: int,
) -> List[Tuple[str, str]]:
    """(massive_option_ticker, contract_key) for latest snapshot row per contract with missing Greeks/IV/OI."""
    cur.execute(
        """
        WITH ranked AS (
          SELECT contract_key, snapshot_ts, iv, delta, gamma, theta, vega, open_interest,
                 ROW_NUMBER() OVER (PARTITION BY contract_key ORDER BY snapshot_ts DESC) AS rn
          FROM option_snapshots
          WHERE source = 'massive'
            AND position('|' IN contract_key) > 0
            AND UPPER(TRIM(split_part(contract_key, '|', 1))) = %s
        )
        SELECT oc.massive_option_ticker, r.contract_key
        FROM ranked r
        INNER JOIN option_contracts oc ON oc.contract_key = r.contract_key
        WHERE r.rn = 1
          AND oc.massive_option_ticker IS NOT NULL
          AND TRIM(oc.massive_option_ticker) <> ''
          AND (
            r.iv IS NULL OR r.delta IS NULL OR r.gamma IS NULL OR r.theta IS NULL OR r.vega IS NULL
            OR r.open_interest IS NULL
          )
        ORDER BY r.contract_key
        LIMIT %s
        """,
        (sym, max(1, int(max_contracts))),
    )
    rows = cur.fetchall() or []
    out: List[Tuple[str, str]] = []
    for r in rows:
        if not r or len(r) < 2:
            continue
        out.append((str(r[0]).strip(), str(r[1]).strip()))
    return out


def run_option_snapshots_pool_contract_fill(
    conn: Any,
    client: Any,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """``mode=option_snapshots_pool_contract_fill`` — refresh snapshot rows with NULL IV/Greeks/OI via per-contract API."""
    sym = (payload.get("underlying") or payload.get("symbol") or "").strip().upper()
    if not sym:
        raise ValueError("payload.underlying (or symbol) required")

    max_contracts = int(payload.get("max_contracts") or 50)
    max_contracts = max(1, min(max_contracts, 500))
    delay_sec = float(payload.get("rest_delay_sec") or 0.2)
    delay_sec = max(0.02, min(delay_sec, 2.0))

    errors: List[str] = []
    contracts_processed = 0

    with conn.cursor() as cur:
        targets = _fetch_column_fill_targets(cur, sym, max_contracts)

    for ot, _ck in targets:
        snap = client.fetch_option_contract_snapshot(sym, ot)
        if snap.get("error"):
            errors.append(f"{ot}: {snap.get('error')}")
            time.sleep(delay_sec)
            continue
        item = contract_snapshot_api_response_to_chain_item(snap)
        if not item:
            errors.append(f"{ot}: could not normalize API response")
            time.sleep(delay_sec)
            continue
        try:
            with conn.cursor() as cur2:
                if apply_chain_snapshot_item(cur2, sym, item):
                    contracts_processed += 1
            conn.commit()
        except Exception as ex:
            conn.rollback()
            err_s = str(ex)
            errors.append(f"{ot}: {err_s}")
            logger.warning("option_snapshots_pool_contract_fill: %s", err_s)
        time.sleep(delay_sec)

    if contracts_processed > 0:
        try:
            from bifrost_worker.data.massive.tasks import _refresh_snapshots_latest

            _refresh_snapshots_latest(conn)
        except Exception as ex:
            logger.debug("option_snapshots_pool_contract_fill refresh mv: %s", ex)

    return {
        "ok": True,
        "underlying": sym,
        "mode": "option_snapshots_pool_contract_fill",
        "contracts_processed": contracts_processed,
        "targets_found": len(targets),
        "errors": errors[:40],
    }
