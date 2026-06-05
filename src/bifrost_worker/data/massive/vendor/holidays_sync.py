"""Sync market holidays into `reference_us_holidays` from two complementary sources.

1. **Known seed (NYSE / NASDAQ, 2020-2027)** — the official NYSE published calendar
   covers many years out, but Polygon's REST endpoint only returns ~12 months
   of upcoming holidays. We embed the known closed days here so the table is
   useful for historical gap detection (back to 2020) and forward planning.
2. **Massive REST `/v1/marketstatus/upcoming`** — fills in early-close timing
   (open/close timestamps) and any ad-hoc closures for the upcoming window.
   Polygon returns a bare JSON array; `MassiveClient.fetch_market_holidays`
   normalizes that to a `{"results": [...]}` envelope.

Each holiday is keyed by `(exchange, holiday_date)`. We never demote a Massive
row back to seed, but we do let Massive **update** seed rows so the more
detailed `status` / `open_time` / `close_time` from Polygon supersedes the
coarse closed-only seed.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

from bifrost_core.persistence.postgres.connection import _get_conn_params
from bifrost_worker.data.massive.vendor.client import MassiveClient
from bifrost_worker.data.massive.vendor.config import get_massive_settings

logger = logging.getLogger(__name__)


# NYSE / NASDAQ official closures, 2020-2027. Source: NYSE published holiday calendar.
# NASDAQ follows the same dates as NYSE for federal market closures.
# Format: (date, name)
_KNOWN_CLOSED: List[Tuple[date, str]] = [
    # 2020
    (date(2020, 1, 1), "New Year's Day"),
    (date(2020, 1, 20), "Martin Luther King Jr. Day"),
    (date(2020, 2, 17), "Washington's Birthday"),
    (date(2020, 4, 10), "Good Friday"),
    (date(2020, 5, 25), "Memorial Day"),
    (date(2020, 7, 3), "Independence Day (observed)"),
    (date(2020, 9, 7), "Labor Day"),
    (date(2020, 11, 26), "Thanksgiving Day"),
    (date(2020, 12, 25), "Christmas Day"),
    # 2021
    (date(2021, 1, 1), "New Year's Day"),
    (date(2021, 1, 18), "Martin Luther King Jr. Day"),
    (date(2021, 2, 15), "Washington's Birthday"),
    (date(2021, 4, 2), "Good Friday"),
    (date(2021, 5, 31), "Memorial Day"),
    (date(2021, 7, 5), "Independence Day (observed)"),
    (date(2021, 9, 6), "Labor Day"),
    (date(2021, 11, 25), "Thanksgiving Day"),
    (date(2021, 12, 24), "Christmas Day (observed)"),
    # 2022
    (date(2022, 1, 17), "Martin Luther King Jr. Day"),
    (date(2022, 2, 21), "Washington's Birthday"),
    (date(2022, 4, 15), "Good Friday"),
    (date(2022, 5, 30), "Memorial Day"),
    (date(2022, 6, 20), "Juneteenth (observed)"),
    (date(2022, 7, 4), "Independence Day"),
    (date(2022, 9, 5), "Labor Day"),
    (date(2022, 11, 24), "Thanksgiving Day"),
    (date(2022, 12, 26), "Christmas Day (observed)"),
    # 2023
    (date(2023, 1, 2), "New Year's Day (observed)"),
    (date(2023, 1, 16), "Martin Luther King Jr. Day"),
    (date(2023, 2, 20), "Washington's Birthday"),
    (date(2023, 4, 7), "Good Friday"),
    (date(2023, 5, 29), "Memorial Day"),
    (date(2023, 6, 19), "Juneteenth"),
    (date(2023, 7, 4), "Independence Day"),
    (date(2023, 9, 4), "Labor Day"),
    (date(2023, 11, 23), "Thanksgiving Day"),
    (date(2023, 12, 25), "Christmas Day"),
    # 2024
    (date(2024, 1, 1), "New Year's Day"),
    (date(2024, 1, 15), "Martin Luther King Jr. Day"),
    (date(2024, 2, 19), "Washington's Birthday"),
    (date(2024, 3, 29), "Good Friday"),
    (date(2024, 5, 27), "Memorial Day"),
    (date(2024, 6, 19), "Juneteenth"),
    (date(2024, 7, 4), "Independence Day"),
    (date(2024, 9, 2), "Labor Day"),
    (date(2024, 11, 28), "Thanksgiving Day"),
    (date(2024, 12, 25), "Christmas Day"),
    # 2025
    (date(2025, 1, 1), "New Year's Day"),
    (date(2025, 1, 9), "Day of Mourning (President Carter)"),
    (date(2025, 1, 20), "Martin Luther King Jr. Day"),
    (date(2025, 2, 17), "Washington's Birthday"),
    (date(2025, 4, 18), "Good Friday"),
    (date(2025, 5, 26), "Memorial Day"),
    (date(2025, 6, 19), "Juneteenth"),
    (date(2025, 7, 4), "Independence Day"),
    (date(2025, 9, 1), "Labor Day"),
    (date(2025, 11, 27), "Thanksgiving Day"),
    (date(2025, 12, 25), "Christmas Day"),
    # 2026
    (date(2026, 1, 1), "New Year's Day"),
    (date(2026, 1, 19), "Martin Luther King Jr. Day"),
    (date(2026, 2, 16), "Washington's Birthday"),
    (date(2026, 4, 3), "Good Friday"),
    (date(2026, 5, 25), "Memorial Day"),
    (date(2026, 6, 19), "Juneteenth"),
    (date(2026, 7, 3), "Independence Day (observed)"),
    (date(2026, 9, 7), "Labor Day"),
    (date(2026, 11, 26), "Thanksgiving Day"),
    (date(2026, 12, 25), "Christmas Day"),
    # 2027
    (date(2027, 1, 1), "New Year's Day"),
    (date(2027, 1, 18), "Martin Luther King Jr. Day"),
    (date(2027, 2, 15), "Washington's Birthday"),
    (date(2027, 3, 26), "Good Friday"),
    (date(2027, 5, 31), "Memorial Day"),
    (date(2027, 6, 18), "Juneteenth (observed)"),
    (date(2027, 7, 5), "Independence Day (observed)"),
    (date(2027, 9, 6), "Labor Day"),
    (date(2027, 11, 25), "Thanksgiving Day"),
    (date(2027, 12, 24), "Christmas Day (observed)"),
]

# Exchanges that follow the NYSE/NASDAQ federal calendar.
_KNOWN_EXCHANGES: Tuple[str, ...] = ("NYSE", "NASDAQ")


def _parse_iso(ts: Any) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    s = ts.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _parse_date(d: Any) -> Optional[date]:
    if isinstance(d, date):
        return d
    if not d or not isinstance(d, str):
        return None
    try:
        return date.fromisoformat(d.strip()[:10])
    except (ValueError, TypeError):
        return None


def _normalize_status(value: Any) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    if s in {"closed", "close"}:
        return "closed"
    if s in {"early-close", "early_close", "earlyclose", "early"}:
        return "early-close"
    return s


def _seed_known_closed(cur) -> int:
    """Insert NYSE/NASDAQ closed days from the embedded calendar (idempotent).

    Returns the number of rows newly inserted (duplicates are NOOP). Existing
    rows from any source are kept as-is — this seed never overwrites richer
    Massive data.
    """
    inserted = 0
    for exch in _KNOWN_EXCHANGES:
        for d, name in _KNOWN_CLOSED:
            cur.execute(
                """
                INSERT INTO reference_us_holidays
                    (exchange, holiday_date, label, name, status, source, updated_at)
                VALUES (%s, %s, %s, %s, 'closed', 'manual_seed', now())
                ON CONFLICT (exchange, holiday_date) DO NOTHING
                RETURNING 1
                """,
                (exch, d, name, name),
            )
            if cur.fetchone() is not None:
                inserted += 1
    return inserted


def sync_market_holidays_from_massive(
    status_config: dict,
    *,
    cfg: Optional[dict] = None,
) -> Dict[str, Any]:
    """Seed known closed days, then fetch Massive `/v1/marketstatus/upcoming`.

    Returns a dict with `ok`, counts (`seeded`, `fetched`, `inserted`,
    `updated`, `skipped`, `total_in_table`), and (on failure) `error`.
    Both phases are best-effort — if Massive is unreachable the seed still
    populates historical/known dates.
    """
    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return {"ok": False, "error": "PostgreSQL not configured"}

    params = _get_conn_params(status_config)
    params["connect_timeout"] = 15

    seeded = 0
    inserted = 0
    updated = 0
    skipped = 0
    fetched = 0
    massive_error: Optional[str] = None
    api_key_present = False

    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        logger.warning("sync_market_holidays connect failed: %s", e)
        return {"ok": False, "error": str(e)}

    try:
        # Phase 1 — embedded NYSE / NASDAQ seed (no network).
        try:
            with conn.cursor() as cur:
                seeded = _seed_known_closed(cur)
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.warning("sync_market_holidays seed phase failed: %s", e)
            return {"ok": False, "error": f"seed phase: {e}"}

        # Phase 2 — Massive REST upcoming holidays.
        ms = get_massive_settings(cfg or {})
        api_key_present = bool(ms.get("api_key"))
        if not api_key_present:
            massive_error = "Massive API key not configured"
        else:
            client = MassiveClient(ms["api_key"], ms["rest_base"])
            data = client.fetch_market_holidays()
            if data.get("error"):
                massive_error = str(data["error"])
            else:
                raw = data.get("results") or []
                if not isinstance(raw, list):
                    raw = []
                fetched = len(raw)
                rows: List[Tuple[str, date, str, str, Optional[str], Optional[datetime], Optional[datetime]]] = []
                for item in raw:
                    if not isinstance(item, dict):
                        skipped += 1
                        continue
                    exch = (item.get("exchange") or "NYSE").strip().upper() or "NYSE"
                    d = _parse_date(item.get("date"))
                    if d is None:
                        skipped += 1
                        continue
                    name = (item.get("name") or "").strip() or None
                    status = _normalize_status(item.get("status"))
                    open_dt = _parse_iso(item.get("open"))
                    close_dt = _parse_iso(item.get("close"))
                    rows.append((exch, d, name or "", status or "closed", name, open_dt, close_dt))

                if rows:
                    try:
                        with conn.cursor() as cur:
                            for exch, d, label, status, name, open_dt, close_dt in rows:
                                cur.execute(
                                    """
                                    INSERT INTO reference_us_holidays
                                        (exchange, holiday_date, label, name, status,
                                         open_time, close_time, source, updated_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'massive', now())
                                    ON CONFLICT (exchange, holiday_date) DO UPDATE SET
                                        label      = COALESCE(EXCLUDED.label, reference_us_holidays.label),
                                        name       = COALESCE(EXCLUDED.name, reference_us_holidays.name),
                                        status     = EXCLUDED.status,
                                        open_time  = EXCLUDED.open_time,
                                        close_time = EXCLUDED.close_time,
                                        source     = 'massive',
                                        updated_at = now()
                                    RETURNING (xmax = 0) AS was_inserted
                                    """,
                                    (exch, d, label, name, status, open_dt, close_dt),
                                )
                                row = cur.fetchone()
                                if row and row[0]:
                                    inserted += 1
                                else:
                                    updated += 1
                        conn.commit()
                    except Exception as e:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        logger.warning("sync_market_holidays massive upsert failed: %s", e)
                        massive_error = str(e)

        # Phase 3 — observed totals for UI feedback.
        total_in_table = 0
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*)::bigint FROM reference_us_holidays")
                row = cur.fetchone()
                total_in_table = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            pass
    finally:
        conn.close()

    return {
        "ok": True,
        "seeded": seeded,
        "fetched": fetched,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "total_in_table": total_in_table,
        "synced_at": datetime.now(timezone.utc).isoformat(),
        **({"massive_error": massive_error} if massive_error else {}),
    }
