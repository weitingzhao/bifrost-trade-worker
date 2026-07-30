"""Massive job queue (job_massive_backfill) and option bars read helpers."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import date as date_type
from datetime import datetime
from datetime import time as time_of_day
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import ProgrammingError
from psycopg2.extras import RealDictCursor

from bifrost_core.persistence.postgres.connection import _get_conn_params
from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED

logger = logging.getLogger(__name__)

_MINUTE_PERIOD_TO_DB = {
    "1 min": "1 minute",
    "1 minute": "1 minute",
    "5 mins": "5 minute",
    "5 min": "5 minute",
    "5 minutes": "5 minute",
    "5 minute": "5 minute",
    "1 hour": "1 hour",
}


def _minute_period_db(period: str) -> str:
    per = (period or "").strip()
    return _MINUTE_PERIOD_TO_DB.get(per, per)


def _norm_expiry_date(expiry: str) -> Optional[date_type]:
    """Normalize expiry to date. Accepts YYYY-MM-DD or YYYYMMDD."""
    e = (expiry or "").strip()
    if not e:
        return None
    if len(e) >= 10 and e[4] == "-":
        try:
            return date_type.fromisoformat(e[:10])
        except ValueError:
            return None
    digits = "".join(c for c in e if c.isdigit())
    if len(digits) >= 8:
        try:
            return date_type(int(digits[0:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None


def canonical_payload_hash(kind: str, payload: Optional[Dict[str, Any]] = None) -> str:
    """Deterministic SHA-256 of kind + payload for job deduplication."""
    canonical = (kind or "").strip() + ":" + json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def insert_job_massive_backfill(
    status_config: dict,
    kind: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[int], bool]:
    """Insert pending job_massive_backfill with dedup.

    Returns (job_id, deduplicated).  If an identical pending/running job exists,
    returns that job's id with deduplicated=True instead of inserting a new row.

    P8: when ``MASSIVE_QUEUES_DISABLED``, refuses insert (no orphan pending rows).
    """
    if MASSIVE_QUEUES_DISABLED:
        return None, False
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None, False
    kind_clean = (kind or "").strip()
    ph = canonical_payload_hash(kind_clean, payload)
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_massive_backfill_id FROM job_massive_backfill
                    WHERE kind = %s AND payload_hash = %s AND status IN ('pending', 'running')
                    LIMIT 1
                    """,
                    (kind_clean, ph),
                )
                existing = cur.fetchone()
                if existing:
                    conn.rollback()
                    return int(existing[0]), True

                cur.execute(
                    """
                    INSERT INTO job_massive_backfill (kind, payload, payload_hash, status, created_at, updated_at)
                    VALUES (%s, %s, %s, 'pending', now(), now())
                    RETURNING job_massive_backfill_id
                    """,
                    (kind_clean, json.dumps(payload or {}), ph),
                )
                row = cur.fetchone()
            conn.commit()
            return (int(row[0]) if row else None), False
        finally:
            conn.close()
    except Exception as e:
        logger.warning("insert_job_massive_backfill failed: %s", e)
        return None, False


def get_watchlist_optionable_stk_symbols(status_config: dict) -> List[str]:
    """Distinct STK symbols on watchlist with optionable=true (for Massive EOD scope)."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT symbol FROM watchlist
                    WHERE sec_type = 'STK' AND optionable = true AND symbol IS NOT NULL AND trim(symbol) <> ''
                    ORDER BY symbol
                    """
                )
                return [str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]]
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_watchlist_optionable_stk_symbols failed: %s", e)
        return []


def update_job_massive_backfill_celery_task_id(
    status_config: dict, job_id: int, celery_task_id: str
) -> bool:
    """Bind broker task id for a pending row with **empty** ``celery_task_id`` (legacy path).

    New dispatch should use :func:`reserve_massive_dispatch_token` + ``apply_async`` +
    :func:`finalize_massive_dispatch_celery_id` so concurrent dispatchers cannot double-pick the same row.
    """
    if MASSIVE_QUEUES_DISABLED:
        return False
    return finalize_massive_dispatch_celery_id(
        status_config, job_id, None, celery_task_id
    )


def clear_massive_dispatch_token(
    status_config: dict, job_id: int, dispatch_token: str
) -> bool:
    """Clear a ``dispatch:…`` placeholder if still present (producer rollback)."""
    if MASSIVE_QUEUES_DISABLED:
        return False
    if not dispatch_token.startswith("dispatch:"):
        return False
    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return False
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return False
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE job_massive_backfill
                    SET celery_task_id = NULL, updated_at = now()
                    WHERE job_massive_backfill_id = %s
                      AND status = 'pending'
                      AND celery_task_id = %s
                    """,
                    (jid, dispatch_token),
                )
                n = cur.rowcount
            conn.commit()
            return n > 0
        finally:
            conn.close()
    except Exception as e:
        logger.warning("clear_massive_dispatch_token failed: %s", e)
        return False


def reserve_massive_dispatch_token(
    status_config: dict, job_id: int,
) -> Optional[Dict[str, Any]]:
    """Set ``celery_task_id`` to ``dispatch:<uuid>`` for one pending row with empty broker id."""
    if MASSIVE_QUEUES_DISABLED:
        return None
    import uuid

    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return None
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return None
    token = f"dispatch:{uuid.uuid4()}"
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE job_massive_backfill
                    SET celery_task_id = %s, updated_at = now()
                    WHERE job_massive_backfill_id = %s
                      AND status = 'pending'
                      AND (celery_task_id IS NULL OR trim(celery_task_id::text) = '')
                    RETURNING job_massive_backfill_id, kind, payload, status, result, celery_task_id, created_at, updated_at
                    """,
                    (token, jid),
                )
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("reserve_massive_dispatch_token failed: %s", e)
        return None


def finalize_massive_dispatch_celery_id(
    status_config: dict,
    job_id: int,
    dispatch_token: Optional[str],
    celery_task_id: str,
) -> bool:
    """After ``apply_async``, replace ``dispatch:…`` (or empty pending slot) with the real Celery task id."""
    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return False
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return False
    rid = (celery_task_id or "").strip()
    if not rid:
        return False
    dt = (dispatch_token or "").strip()
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                if dt.startswith("dispatch:"):
                    cur.execute(
                        """
                        UPDATE job_massive_backfill
                        SET celery_task_id = %s, updated_at = now()
                        WHERE job_massive_backfill_id = %s
                          AND status = 'pending'
                          AND celery_task_id = %s
                        """,
                        (rid, jid, dt),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE job_massive_backfill
                        SET celery_task_id = %s, updated_at = now()
                        WHERE job_massive_backfill_id = %s
                          AND status = 'pending'
                          AND (celery_task_id IS NULL OR trim(celery_task_id::text) = '')
                        """,
                        (rid, jid),
                    )
                ok = cur.rowcount > 0
            conn.commit()
            return ok
        finally:
            conn.close()
    except Exception as e:
        logger.warning("finalize_massive_dispatch_celery_id failed: %s", e)
        return False


def reserve_next_pending_massive_job_for_queue_slice(
    status_config: dict,
    qcond: str,
    qparams: List[Any],
) -> Optional[Dict[str, Any]]:
    """Pick next pending row for a broker slice under lock and set ``dispatch:`` token (single winner)."""
    if MASSIVE_QUEUES_DISABLED:
        return None
    import uuid

    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return None
    cq = (qcond or "").strip()
    if not cq:
        return None
    token = f"dispatch:{uuid.uuid4()}"
    sql = f"""
        UPDATE job_massive_backfill AS j
        SET celery_task_id = %s,
            updated_at = now()
        FROM (
            SELECT job_massive_backfill_id
            FROM job_massive_backfill
            WHERE status = 'pending'
              AND (
                celery_task_id IS NULL
                OR trim(celery_task_id::text) = ''
                OR (
                  trim(celery_task_id::text) LIKE 'dispatch:%%'
                  AND updated_at < (now() - interval '15 minutes')
                )
              )
              AND ({cq})
            ORDER BY job_massive_backfill_id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        ) AS t
        WHERE j.job_massive_backfill_id = t.job_massive_backfill_id
        RETURNING j.job_massive_backfill_id, j.kind, j.payload, j.status, j.result, j.celery_task_id, j.created_at, j.updated_at
    """
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple([token] + list(qparams)))
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("reserve_next_pending_massive_job_for_queue_slice failed: %s", e)
        return None


def release_massive_job_to_pending_for_redispatch(status_config: dict, job_id: int) -> bool:
    """Set row back to ``pending``, clear broker id and result (transient DB / worker issues).

    Used when a worker hit a transient DB issue so a pull worker can claim the row again.
    """
    if MASSIVE_QUEUES_DISABLED:
        return False
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return False
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return False
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE job_massive_backfill
                    SET status = 'pending', celery_task_id = NULL, result = NULL, updated_at = now()
                    WHERE job_massive_backfill_id = %s
                      AND status IN ('pending', 'running')
                    """,
                    (jid,),
                )
                n = cur.rowcount
            conn.commit()
            return bool(n and n > 0)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("release_massive_job_to_pending_for_redispatch failed: %s", e)
        return False


def claim_next_massive_job_for_queue_slice(
    status_config: dict,
    celery_queue: str,
    claim_token: str,
) -> Optional[int]:
    """Atomically claim the oldest pending row for a broker queue slice (``FOR UPDATE SKIP LOCKED``).

    Sets ``status`` to ``running`` and ``celery_task_id`` to ``claim_token`` (caller should use a
    ``dbpull:`` prefix). Returns ``job_massive_backfill_id`` or ``None`` if no row matched.
    """
    if MASSIVE_QUEUES_DISABLED:
        return None
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    cq = (celery_queue or "").strip()
    qcond, qparams = _massive_celery_queue_condition(cq)
    if not qcond:
        return None
    tok = (claim_token or "").strip()[:512]
    if not tok:
        return None
    sql = f"""
        WITH c AS (
            SELECT job_massive_backfill_id
            FROM job_massive_backfill
            WHERE status = 'pending'
              AND (celery_task_id IS NULL OR trim(celery_task_id) = '')
              AND ({qcond})
            ORDER BY job_massive_backfill_id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE job_massive_backfill j
        SET status = 'running', celery_task_id = %s, updated_at = now()
        FROM c
        WHERE j.job_massive_backfill_id = c.job_massive_backfill_id
        RETURNING j.job_massive_backfill_id
    """
    params_exec = tuple(qparams) + (tok,)
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(sql, params_exec)
                row = cur.fetchone()
            conn.commit()
            if row and row[0] is not None:
                return int(row[0])
            return None
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
    except Exception as e:
        logger.warning("claim_next_massive_job_for_queue_slice failed: %s", e)
        return None


def get_job_massive_backfill(status_config: dict, job_id: Any) -> Optional[Dict[str, Any]]:
    if MASSIVE_QUEUES_DISABLED:
        return None
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT job_massive_backfill_id, kind, payload, status, result, celery_task_id, created_at, updated_at
                    FROM job_massive_backfill
                    WHERE job_massive_backfill_id = %s
                    """,
                    (jid,),
                )
                row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_job_massive_backfill failed: %s", e)
        return None


def get_and_claim_massive_backfill_for_run(
    status_config: dict,
    job_id: Any,
    celery_task_id: Optional[str],
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Load one row under ``FOR UPDATE`` and claim ``pending`` -> ``running`` for this Celery task id only.

    Duplicate broker deliveries / rapid retries can run multiple ``run_massive_job`` tasks for one row.
    Uncoordinated concurrent execution previously raced on ``update_job... running`` and could mark rows
    ``failed`` under worker saturation.
    """
    if MASSIVE_QUEUES_DISABLED:
        return None, "not_found"
    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return None, "not_found"
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return None, "not_found"
    rid = (celery_task_id or "").strip()
    conn: Optional[Any] = None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        conn.autocommit = False
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT job_massive_backfill_id, kind, payload, status, result, celery_task_id, created_at, updated_at
                FROM job_massive_backfill
                WHERE job_massive_backfill_id = %s
                FOR UPDATE
                """,
                (jid,),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None, "not_found"

            st = str(row["status"] or "").strip().lower()
            ct = str(row["celery_task_id"] or "").strip()

            if st == "done":
                conn.commit()
                return dict(row), "skip_done"
            if st == "failed":
                conn.commit()
                return dict(row), "skip_failed"
            if st == "running":
                if rid and ct == rid:
                    conn.commit()
                    return dict(row), "continue_owner"
                conn.commit()
                return dict(row), "skip_duplicate"
            if st == "pending":
                is_dispatch_tok = ct.startswith("dispatch:")
                if rid and ct and ct != rid and not is_dispatch_tok:
                    conn.commit()
                    return dict(row), "skip_duplicate"
                if rid:
                    cur.execute(
                        """
                        UPDATE job_massive_backfill
                        SET status = %s,
                            updated_at = now(),
                            celery_task_id = %s
                        WHERE job_massive_backfill_id = %s
                        RETURNING job_massive_backfill_id, kind, payload, status, result, celery_task_id, created_at, updated_at
                        """,
                        ("running", rid, jid),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE job_massive_backfill
                        SET status = %s, updated_at = now()
                        WHERE job_massive_backfill_id = %s
                        RETURNING job_massive_backfill_id, kind, payload, status, result, celery_task_id, created_at, updated_at
                        """,
                        ("running", jid),
                    )
                row2 = cur.fetchone()
                conn.commit()
                return dict(row2) if row2 else dict(row), "claimed"

            conn.rollback()
            return dict(row), "skip_duplicate"
    except Exception as e:
        logger.warning("get_and_claim_massive_backfill_for_run failed: %s", e)
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        return None, "not_found"
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def list_job_massive_backfill(
    status_config: dict,
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None,
    kind_filter: Optional[str] = None,
    celery_queue: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Latest Massive sync jobs, newest first."""
    if MASSIVE_QUEUES_DISABLED:
        return []
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    lim = max(1, min(int(limit), 100))
    off = max(0, int(offset))
    conditions: List[str] = []
    params_list: List[Any] = []
    if status_filter and str(status_filter).strip():
        conditions.append("status = %s")
        params_list.append(str(status_filter).strip())
    if kind_filter and str(kind_filter).strip():
        conditions.append("kind = %s")
        params_list.append(str(kind_filter).strip().lower())
    cq = (celery_queue or "").strip()
    if cq:
        qcond, qparams = _massive_celery_queue_condition(cq)
        if qcond:
            conditions.append(f"({qcond})")
            params_list.extend(qparams)
    where_sql = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""
        SELECT job_massive_backfill_id, kind, payload, status, result, celery_task_id, created_at, updated_at
        FROM job_massive_backfill
        {where_sql}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    params_list.extend([lim, off])
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params_list))
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.warning("list_job_massive_backfill failed: %s", e)
        return []


_VALID_MASSIVE_JOB_STATUS = frozenset({"pending", "running", "done", "failed"})


def _massive_celery_queue_condition(celery_queue: str) -> Tuple[Optional[str], List[Any]]:
    """SQL predicate for rows routed to ``celery_queue`` (see ``celery_queue_for_massive_job``)."""
    from bifrost_worker.data.massive.celery_queues import MASSIVE_STOCKS_QUEUE_KINDS

    cq = (celery_queue or "").strip()
    if not cq:
        return None, []
    # Keep SQL-side queue grouping aligned with the real Celery routing logic so
    # stock OHLC / corporate-action jobs are not misclassified as options jobs.
    stock = sorted(MASSIVE_STOCKS_QUEUE_KINDS)
    ph = ",".join(["%s"] * len(stock))
    pri_low = "lower(coalesce(payload->>'priority','')) <> 'high'"
    pri_high = "lower(coalesce(payload->>'priority','')) = 'high'"
    not_stock = f"(kind NOT IN ({ph}))"
    is_stock = f"(kind IN ({ph}))"
    sp = list(stock)
    if cq == "options_massive":
        return f"{not_stock} AND {pri_low}", sp
    if cq == "options_massive_high":
        return f"{not_stock} AND {pri_high}", sp
    if cq == "stocks_massive":
        return f"{is_stock} AND {pri_low}", sp
    if cq == "stocks_massive_high":
        return f"{is_stock} AND {pri_high}", sp
    logger.warning("unknown massive celery_queue filter %r — no SQL filter applied", cq)
    return None, []


def delete_job_massive_backfill(status_config: dict, job_id: Any) -> bool:
    """Delete one job_massive_backfill row by id. Returns True if deleted or not found."""
    if MASSIVE_QUEUES_DISABLED:
        return False
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return False
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return False
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM job_massive_backfill WHERE job_massive_backfill_id = %s",
                    (jid,),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.warning("delete_job_massive_backfill failed: %s", e)
        return False


def delete_all_job_massive_backfill(
    status_config: dict,
    status_filter: Optional[str] = None,
    celery_queue: Optional[str] = None,
) -> int:
    """Delete Massive jobs, optionally scoped by status and/or Celery queue routing."""
    if MASSIVE_QUEUES_DISABLED:
        return 0
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return 0
    sf = (status_filter or "").strip().lower()
    cq = (celery_queue or "").strip()
    qcond, qparams = _massive_celery_queue_condition(cq) if cq else (None, [])
    qsql = f" AND ({qcond})" if qcond else ""
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                if sf in _VALID_MASSIVE_JOB_STATUS:
                    cur.execute(
                        f"DELETE FROM job_massive_backfill WHERE status = %s{qsql}",
                        (sf, *qparams),
                    )
                else:
                    if qcond:
                        cur.execute(
                            f"DELETE FROM job_massive_backfill WHERE ({qcond})",
                            tuple(qparams),
                        )
                    else:
                        cur.execute("DELETE FROM job_massive_backfill")
                deleted = cur.rowcount
            conn.commit()
            return int(deleted)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("delete_all_job_massive_backfill failed: %s", e)
        return 0


def trim_job_massive_backfill(
    status_config: dict, keep: int = 200, celery_queue: Optional[str] = None,
) -> int:
    """Keep the newest ``keep`` rows (globally or within one Celery queue slice); delete older."""
    if MASSIVE_QUEUES_DISABLED:
        return 0
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return 0
    k = max(1, min(int(keep), 50_000))
    cq = (celery_queue or "").strip()
    qcond, qparams = _massive_celery_queue_condition(cq) if cq else (None, [])
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                if qcond:
                    cur.execute(
                        f"""
                        WITH ranked AS (
                            SELECT job_massive_backfill_id,
                                   ROW_NUMBER() OVER (ORDER BY job_massive_backfill_id DESC) AS rn
                            FROM job_massive_backfill
                            WHERE ({qcond})
                        )
                        DELETE FROM job_massive_backfill
                        WHERE job_massive_backfill_id IN (
                            SELECT job_massive_backfill_id FROM ranked WHERE rn > %s
                        )
                        """,
                        tuple(qparams) + (k,),
                    )
                else:
                    cur.execute(
                        """
                        WITH kept AS (
                            SELECT job_massive_backfill_id FROM job_massive_backfill
                            ORDER BY job_massive_backfill_id DESC
                            LIMIT %s
                        )
                        DELETE FROM job_massive_backfill
                        WHERE job_massive_backfill_id NOT IN (SELECT job_massive_backfill_id FROM kept)
                        """,
                        (k,),
                    )
                deleted = cur.rowcount
            conn.commit()
            return int(deleted)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("trim_job_massive_backfill failed: %s", e)
        return 0


def count_job_massive_backfill_by_status(
    status_config: dict,
    celery_queue: Optional[str] = None,
) -> Dict[str, int]:
    """Return counts per status, optionally scoped to one broker queue slice."""
    if MASSIVE_QUEUES_DISABLED:
        return {}
    labels = ("pending", "running", "done", "failed")
    out: Dict[str, int] = {s: 0 for s in labels}
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return out
    cq = (celery_queue or "").strip()
    qcond: Optional[str]
    qparams: List[Any]
    if cq:
        qcond, qparams = _massive_celery_queue_condition(cq)
        if not qcond:
            return out
        where_sql = f" WHERE ({qcond})"
    else:
        where_sql = ""
        qparams = []
    sql = f"SELECT status, COUNT(*)::bigint FROM job_massive_backfill{where_sql} GROUP BY status"
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(qparams))
                for row in cur.fetchall() or []:
                    st = str(row[0] or "").strip().lower()
                    if st in out:
                        out[st] = int(row[1])
            return out
        finally:
            conn.close()
    except Exception as e:
        logger.warning("count_job_massive_backfill_by_status failed: %s", e)
        return out


def reset_failed_job_massive_backfill_batch(
    status_config: dict,
    celery_queue: Optional[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """Set failed rows to pending (cleared result) for re-enqueue; returns updated rows (oldest failed first)."""
    if MASSIVE_QUEUES_DISABLED:
        return []
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    lim = max(1, min(int(limit), 2000))
    cq = (celery_queue or "").strip()
    if cq:
        qcond, qparams = _massive_celery_queue_condition(cq)
        if not qcond:
            return []
        where_failed = f"status = 'failed' AND ({qcond})"
        params_sel = tuple(qparams) + (lim,)
    else:
        where_failed = "status = 'failed'"
        params_sel = (lim,)
    sql = f"""
        WITH sel AS (
            SELECT job_massive_backfill_id FROM job_massive_backfill
            WHERE {where_failed}
            ORDER BY job_massive_backfill_id ASC
            LIMIT %s
        )
        UPDATE job_massive_backfill j
        SET status = 'pending', result = NULL, updated_at = now(), celery_task_id = NULL
        FROM sel
        WHERE j.job_massive_backfill_id = sel.job_massive_backfill_id
        RETURNING j.job_massive_backfill_id, j.kind, j.payload, j.status, j.result,
                  j.celery_task_id, j.created_at, j.updated_at
    """
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params_sel)
                rows = cur.fetchall()
            conn.commit()
            return [dict(r) for r in rows] if rows else []
        finally:
            conn.close()
    except Exception as e:
        logger.warning("reset_failed_job_massive_backfill_batch failed: %s", e)
        return []


def reset_failed_job_massive_backfill_one(status_config: dict, job_id: Any) -> Optional[Dict[str, Any]]:
    """If the row is ``failed``, set ``pending`` and clear ``result`` / ``celery_task_id``. Returns row for re-enqueue."""
    if MASSIVE_QUEUES_DISABLED:
        return None
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return None
    sql = """
        UPDATE job_massive_backfill
        SET status = 'pending', result = NULL, updated_at = now(), celery_task_id = NULL
        WHERE job_massive_backfill_id = %s AND status = 'failed'
        RETURNING job_massive_backfill_id, kind, payload, status, result,
                  celery_task_id, created_at, updated_at
    """
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, (jid,))
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("reset_failed_job_massive_backfill_one failed: %s", e)
        return None


def _publish_massive_job_redis(job_id: int, status: str, result: Optional[Dict[str, Any]] = None) -> None:
    """Optional: notify subscribers (e.g. future WS) when a job reaches a terminal state."""
    try:
        import redis

        from bifrost_worker.celery.celery_app import broker_url

        r = redis.from_url(broker_url, socket_connect_timeout=2.0)
        r.publish(
            f"massive:job:{job_id}",
            json.dumps({"job_id": job_id, "status": status, "result": result}),
        )
    except Exception:
        pass


def update_job_massive_backfill_result(
    status_config: dict,
    job_id: int,
    status: str,
    result: Optional[Dict[str, Any]] = None,
) -> bool:
    if MASSIVE_QUEUES_DISABLED:
        return False
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return False
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE job_massive_backfill
                    SET status = %s, result = %s, updated_at = now()
                    WHERE job_massive_backfill_id = %s
                    """,
                    (status, json.dumps(result) if result is not None else None, job_id),
                )
            conn.commit()
            if status in ("done", "failed"):
                _publish_massive_job_redis(job_id, status, result)
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.warning("update_job_massive_backfill_result failed: %s", e)
        return False


def insert_job_sepa_phase4(
    status_config: dict,
    job_id: str,
    request_payload: Optional[Dict[str, Any]] = None,
    *,
    version: str = "sepa_phase4_v1",
) -> Optional[int]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    jid = (job_id or "").strip()
    if not jid:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_sepa_phase4
                        (job_id, status, progress, request, summary, errors, created_at, updated_at, version)
                    VALUES (%s, 'queued', %s::jsonb, %s::jsonb, '{}'::jsonb, '[]'::jsonb, now(), now(), %s)
                    RETURNING job_sepa_phase4_id
                    """,
                    (
                        jid,
                        json.dumps({"current": 0, "total": len((request_payload or {}).get("symbols") or []), "stage": "queued", "pct": 0.0}),
                        json.dumps(request_payload or {}),
                        version,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row[0]) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("insert_job_sepa_phase4 failed: %s", e)
        return None


def get_job_sepa_phase4(
    status_config: dict,
    job_id: str,
) -> Optional[Dict[str, Any]]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    jid = (job_id or "").strip()
    if not jid:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT job_id, status, progress, request, summary, errors,
                           created_at, updated_at, started_at, finished_at, version
                    FROM job_sepa_phase4
                    WHERE job_id = %s
                    LIMIT 1
                    """,
                    (jid,),
                )
                row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_job_sepa_phase4 failed: %s", e)
        return None


def get_job_sepa_phase4_result(
    status_config: dict,
    job_id: str,
    *,
    offset: int = 0,
    limit: int = 200,
) -> Optional[Dict[str, Any]]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    jid = (job_id or "").strip()
    if not jid:
        return None
    st = max(0, int(offset))
    lim = max(1, min(int(limit), 1000))
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT job_id, status, summary, result, version
                    FROM job_sepa_phase4
                    WHERE job_id = %s
                    LIMIT 1
                    """,
                    (jid,),
                )
                row = cur.fetchone()
            if not row:
                return None
            result = row.get("result") or {}
            rows = result.get("rows") or []
            if not isinstance(rows, list):
                rows = []
            ed = st + lim
            return {
                "job_id": row.get("job_id"),
                "status": row.get("status"),
                "summary": row.get("summary") or {},
                "rows": rows[st:ed],
                "total_rows": len(rows),
                "offset": st,
                "limit": lim,
                "version": row.get("version") or "sepa_phase4_v1",
            }
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_job_sepa_phase4_result failed: %s", e)
        return None


def update_job_sepa_phase4(
    status_config: dict,
    job_id: str,
    **fields: Any,
) -> bool:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return False
    jid = (job_id or "").strip()
    if not jid:
        return False
    allowed = {
        "status",
        "progress",
        "request",
        "summary",
        "result",
        "errors",
        "started_at",
        "finished_at",
        "version",
    }
    set_parts: List[str] = []
    params_list: List[Any] = []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k in {"progress", "request", "summary", "result", "errors"}:
            set_parts.append(f"{k} = %s::jsonb")
            params_list.append(json.dumps(v) if v is not None else ("[]" if k == "errors" else "{}"))
        elif k in {"started_at", "finished_at"} and isinstance(v, str):
            set_parts.append(f"{k} = %s::timestamptz")
            params_list.append(v)
        else:
            set_parts.append(f"{k} = %s")
            params_list.append(v)
    if not set_parts:
        return True
    set_parts.append("updated_at = now()")
    sql = f"UPDATE job_sepa_phase4 SET {', '.join(set_parts)} WHERE job_id = %s"
    params_list.append(jid)
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params_list))
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.warning("update_job_sepa_phase4 failed: %s", e)
        return False


def list_job_sepa_phase4(
    status_config: dict,
    *,
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None,
    created_from: Optional[str] = None,
    created_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    base_sql = """
        SELECT job_id, status, progress, request, summary, errors,
               created_at, updated_at, started_at, finished_at, version
        FROM job_sepa_phase4
    """
    conditions: List[str] = []
    params_list: List[Any] = []
    sf = (status_filter or "").strip()
    if sf:
        conditions.append("status = %s")
        params_list.append(sf)
    cf = (created_from or "").strip()
    if cf:
        conditions.append("created_at >= %s::timestamptz")
        params_list.append(cf)
    ct = (created_to or "").strip()
    if ct:
        conditions.append("created_at <= %s::timestamptz")
        params_list.append(ct)
    where_sql = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"{base_sql}{where_sql}"
    sql += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params_list.extend([lim, off])
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params_list))
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.warning("list_job_sepa_phase4 failed: %s", e)
        return []


def delete_job_sepa_phase4(status_config: dict, job_id: str) -> bool:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return False
    jid = (job_id or "").strip()
    if not jid:
        return False
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM job_sepa_phase4 WHERE job_id = %s", (jid,))
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.warning("delete_job_sepa_phase4 failed: %s", e)
        return False


def _expiry_to_date_param(expiry: str) -> Optional[str]:
    """Normalize expiry string to ISO date (YYYY-MM-DD) for market.* date columns."""
    e = _norm_expiry_db(str(expiry or "").strip())
    if len(e) == 8 and e.isdigit():
        return f"{e[:4]}-{e[4:6]}-{e[6:8]}"
    raw = (expiry or "").strip()
    if len(raw) >= 10 and raw[4] == "-":
        return raw[:10]
    return None


def get_option_open_interest_daily(
    status_config: dict,
    symbol: str,
    expiry: Optional[str] = None,
    limit: int = 100,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Latest OI rows for symbol from market.option_open_interest (optional expiry / trade_date range)."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    _OI_SELECT = """
        SELECT option_ticker AS contract_key,
               underlying AS symbol,
               expiry::text AS expiry,
               strike,
               option_right,
               trade_date,
               open_interest,
               'polygon' AS source
        FROM market.option_open_interest
    """
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                lim = max(1, min(500, limit))
                exp_d = _expiry_to_date_param(expiry) if expiry else None
                if expiry and exp_d is None:
                    return []
                if expiry and date_from and date_to:
                    cur.execute(
                        _OI_SELECT
                        + """
                        WHERE underlying = %s AND expiry = %s::date
                          AND trade_date >= %s::date AND trade_date <= %s::date
                        ORDER BY trade_date DESC
                        LIMIT %s
                        """,
                        (sym, exp_d, date_from[:10], date_to[:10], lim),
                    )
                elif expiry:
                    cur.execute(
                        _OI_SELECT
                        + """
                        WHERE underlying = %s AND expiry = %s::date
                        ORDER BY trade_date DESC
                        LIMIT %s
                        """,
                        (sym, exp_d, lim),
                    )
                elif date_from and date_to:
                    cur.execute(
                        _OI_SELECT
                        + """
                        WHERE underlying = %s
                          AND trade_date >= %s::date AND trade_date <= %s::date
                        ORDER BY trade_date DESC
                        LIMIT %s
                        """,
                        (sym, date_from[:10], date_to[:10], lim),
                    )
                else:
                    cur.execute(
                        _OI_SELECT
                        + """
                        WHERE underlying = %s
                        ORDER BY trade_date DESC
                        LIMIT %s
                        """,
                        (sym, lim),
                    )
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_option_open_interest_daily failed: %s", e)
        return []


def get_option_trades(
    status_config: dict,
    symbol: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT contract_key, trade_ts, price, size, exchange, massive_trade_id
                    FROM option_trades
                    WHERE symbol = %s
                    ORDER BY trade_ts DESC
                    LIMIT %s
                    """,
                    (sym, max(1, min(500, limit))),
                )
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_option_trades failed: %s", e)
        return []


_SNAPSHOT_LATEST_SELECT = """
    s.snapshot_ts,
    s.iv, s.delta, s.gamma, s.theta, s.vega, s.open_interest,
    s.underlying AS underlying_ticker,
    s.day_open, s.day_high, s.day_low, s.day_close,
    s.day_previous_close,
    NULL::double precision AS day_change,
    s.day_change_percent,
    s.day_volume, s.day_vwap,
    NULL::timestamptz AS day_last_updated,
    NULL::date AS day_last_updated_day,
    'massive' AS source,
    s.fetched_at AS created_at,
    oc.option_ticker AS _option_ticker,
    oc.underlying AS _underlying,
    oc.expiry AS _expiry,
    oc.strike AS _strike,
    oc.option_right AS _option_right
"""

_IB_CONTRACT_JOIN = """
    FROM unnest(%s::text[], %s::date[], %s::text[], %s::float8[], %s::text[])
      AS req(underlying, expiry, option_right, strike, ib_key)
    JOIN market.option_contract oc
      ON oc.underlying = req.underlying
     AND oc.expiry = req.expiry
     AND oc.option_right = req.option_right
     AND abs(oc.strike - req.strike) < 1e-4
"""


def _map_snapshot_row_to_ib_key(
    row: Dict[str, Any],
    *,
    ib_by_identity: Dict[Tuple[str, date_type, float, str], str],
    poly_requested: set,
) -> Optional[Dict[str, Any]]:
    """Attach IB ``contract_key`` for callers; drop internal bridge columns."""
    from bifrost_worker.data.massive.vendor.contract_key_bridge import (
        ib_contract_key_from_parts,
        identity_key,
    )

    out = dict(row)
    req_ib_key = out.pop("_req_ib_key", None)
    underlying = out.pop("_underlying", None) or out.get("underlying_ticker")
    expiry = out.pop("_expiry", None)
    strike = out.pop("_strike", None)
    option_right = out.pop("_option_right", None)
    option_ticker = out.pop("_option_ticker", None)

    ck: Optional[str] = None
    if req_ib_key:
        ck = str(req_ib_key)
    elif underlying is not None and expiry is not None and strike is not None and option_right:
        try:
            exp_d = expiry if isinstance(expiry, date_type) else date_type.fromisoformat(str(expiry)[:10])
            ident = identity_key(str(underlying), exp_d, float(strike), str(option_right))
            ck = ib_by_identity.get(ident)
            if ck is None and option_ticker in poly_requested:
                ck = ib_contract_key_from_parts(
                    str(underlying), exp_d, float(strike), str(option_right)
                )
        except (TypeError, ValueError):
            ck = None
    if ck is None:
        return None
    out["contract_key"] = ck
    return out


def _fetch_option_snapshots_latest_bridged(cur: Any, keys: List[str]) -> List[Dict[str, Any]]:
    from bifrost_worker.data.massive.vendor.contract_key_bridge import (
        identity_key,
        split_contract_keys,
    )

    polygon, ib_parts = split_contract_keys(keys)
    if not polygon and not ib_parts:
        return []

    ib_by_identity = {
        identity_key(p.underlying, p.expiry, p.strike, p.option_right): p.original_key
        for p in ib_parts
    }
    poly_requested = set(polygon)

    rows: List[Dict[str, Any]] = []

    def _view_exists() -> bool:
        cur.execute(
            """
            SELECT 1 FROM information_schema.views
            WHERE table_schema = 'market' AND table_name = 'v_option_chain_latest'
            LIMIT 1
            """
        )
        return bool(cur.fetchone())

    if polygon:
        view_ok = False
        try:
            if _view_exists():
                cur.execute(
                    f"""
                    SELECT {_SNAPSHOT_LATEST_SELECT}
                    FROM market.v_option_chain_latest s
                    JOIN market.option_contract oc ON oc.option_ticker = s.option_ticker
                    WHERE s.option_ticker = ANY(%s)
                    """,
                    (polygon,),
                )
                view_ok = True
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
            view_ok = False

        if not view_ok:
            cur.execute(
                f"""
                SELECT DISTINCT ON (s.option_ticker)
                    {_SNAPSHOT_LATEST_SELECT}
                FROM market.option_snapshot s
                JOIN market.option_contract oc ON oc.option_ticker = s.option_ticker
                WHERE s.option_ticker = ANY(%s)
                ORDER BY s.option_ticker, s.snapshot_ts DESC
                """,
                (polygon,),
            )
        rows.extend(dict(r) for r in cur.fetchall())

    if ib_parts:
        underlyings = [p.underlying for p in ib_parts]
        expiries = [p.expiry for p in ib_parts]
        strikes = [p.strike for p in ib_parts]
        rights = [p.option_right for p in ib_parts]
        ib_keys = [p.original_key for p in ib_parts]
        ib_params = (underlyings, expiries, rights, strikes, ib_keys)
        select_ib = _SNAPSHOT_LATEST_SELECT + ",\n    req.ib_key AS _req_ib_key"
        view_ok = False
        try:
            if _view_exists():
                cur.execute(
                    f"""
                    SELECT {select_ib}
                    {_IB_CONTRACT_JOIN}
                    JOIN market.v_option_chain_latest s ON s.option_ticker = oc.option_ticker
                    """,
                    ib_params,
                )
                view_ok = True
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
            view_ok = False

        if not view_ok:
            cur.execute(
                f"""
                SELECT DISTINCT ON (oc.option_ticker)
                    {select_ib}
                {_IB_CONTRACT_JOIN}
                JOIN market.option_snapshot s ON s.option_ticker = oc.option_ticker
                ORDER BY oc.option_ticker, s.snapshot_ts DESC
                """,
                ib_params,
            )
        rows.extend(dict(r) for r in cur.fetchall())

    out: List[Dict[str, Any]] = []
    seen_ck: set = set()
    for row in rows:
        mapped = _map_snapshot_row_to_ib_key(
            row, ib_by_identity=ib_by_identity, poly_requested=poly_requested
        )
        if mapped is None:
            continue
        ck = mapped["contract_key"]
        if ck in seen_ck:
            continue
        seen_ck.add(ck)
        out.append(mapped)
    return out


def get_option_snapshots_latest(
    status_config: dict,
    contract_keys: List[str],
    source: str = "massive",
) -> List[Dict[str, Any]]:
    """Latest snapshot per contract_key from ``market.v_option_chain_latest``.

    Accepts IB keys (``SYM|OPT|…``) and Polygon tickers (``O:…``). Always returns
    IB-shaped ``contract_key`` so Discovery/Screener ``parse_contract_key`` works.
    ``source`` is accepted for API compatibility but ignored.
    """
    _ = source  # unused — market.* has no source column
    if not contract_keys or not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return []
    keys = [k for k in contract_keys if k and str(k).strip()][:120]
    if not keys:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                return _fetch_option_snapshots_latest_bridged(cur, keys)
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_option_snapshots_latest failed: %s", e)
        return []


def _fetch_option_snapshots_eod_bridged(
    cur: Any,
    keys: List[str],
    since_ts: datetime,
) -> List[Dict[str, Any]]:
    from bifrost_worker.data.massive.vendor.contract_key_bridge import (
        ib_contract_key_from_parts,
        identity_key,
        split_contract_keys,
    )

    polygon, ib_parts = split_contract_keys(keys)
    if not polygon and not ib_parts:
        return []

    ib_by_identity = {
        identity_key(p.underlying, p.expiry, p.strike, p.option_right): p.original_key
        for p in ib_parts
    }
    poly_requested = set(polygon)
    out: List[Dict[str, Any]] = []

    def _append_mapped(raw_rows: List[Any]) -> None:
        for row in raw_rows:
            d = dict(row)
            req_ib_key = d.pop("_req_ib_key", None)
            underlying = d.pop("_underlying", None)
            expiry = d.pop("_expiry", None)
            strike = d.pop("_strike", None)
            option_right = d.pop("_option_right", None)
            option_ticker = d.pop("_option_ticker", None)
            ck: Optional[str] = None
            if req_ib_key:
                ck = str(req_ib_key)
            elif underlying is not None and expiry is not None and strike is not None and option_right:
                try:
                    exp_d = (
                        expiry
                        if isinstance(expiry, date_type)
                        else date_type.fromisoformat(str(expiry)[:10])
                    )
                    ident = identity_key(str(underlying), exp_d, float(strike), str(option_right))
                    ck = ib_by_identity.get(ident)
                    if ck is None and option_ticker in poly_requested:
                        ck = ib_contract_key_from_parts(
                            str(underlying), exp_d, float(strike), str(option_right)
                        )
                except (TypeError, ValueError):
                    ck = None
            if ck is None:
                continue
            d["contract_key"] = ck
            out.append(d)

    if polygon:
        cur.execute(
            """
            SELECT DISTINCT ON (
              (DATE(timezone('America/New_York', v.snapshot_ts))),
              oc.option_ticker
            )
              DATE(timezone('America/New_York', v.snapshot_ts)) AS snap_day,
              v.iv,
              v.underlying_price,
              v.snapshot_ts,
              oc.option_ticker AS _option_ticker,
              oc.underlying AS _underlying,
              oc.expiry AS _expiry,
              oc.strike AS _strike,
              oc.option_right AS _option_right
            FROM market.v_option_snapshot_with_stock v
            JOIN market.option_contract oc ON oc.option_ticker = v.option_ticker
            WHERE v.option_ticker = ANY(%s)
              AND v.snapshot_ts >= %s
            ORDER BY
              DATE(timezone('America/New_York', v.snapshot_ts)),
              oc.option_ticker,
              v.snapshot_ts DESC
            """,
            (polygon, since_ts),
        )
        _append_mapped(cur.fetchall())

    if ib_parts:
        underlyings = [p.underlying for p in ib_parts]
        expiries = [p.expiry for p in ib_parts]
        strikes = [p.strike for p in ib_parts]
        rights = [p.option_right for p in ib_parts]
        ib_keys = [p.original_key for p in ib_parts]
        cur.execute(
            f"""
            SELECT DISTINCT ON (
              (DATE(timezone('America/New_York', v.snapshot_ts))),
              oc.option_ticker
            )
              DATE(timezone('America/New_York', v.snapshot_ts)) AS snap_day,
              v.iv,
              v.underlying_price,
              v.snapshot_ts,
              oc.option_ticker AS _option_ticker,
              oc.underlying AS _underlying,
              oc.expiry AS _expiry,
              oc.strike AS _strike,
              oc.option_right AS _option_right,
              req.ib_key AS _req_ib_key
            {_IB_CONTRACT_JOIN}
            JOIN market.v_option_snapshot_with_stock v ON v.option_ticker = oc.option_ticker
            WHERE v.snapshot_ts >= %s
            ORDER BY
              DATE(timezone('America/New_York', v.snapshot_ts)),
              oc.option_ticker,
              v.snapshot_ts DESC
            """,
            (underlyings, expiries, rights, strikes, ib_keys, since_ts),
        )
        _append_mapped(cur.fetchall())

    return out


def get_option_snapshots_eod_per_day(
    status_config: dict,
    contract_keys: List[str],
    source: str = "massive",
    since_ts: Optional[datetime] = None,
    chunk_size: int = 100,
) -> List[Dict[str, Any]]:
    """Latest snapshot per calendar day (America/New_York) per contract_key.

    Reads ``market.v_option_snapshot_with_stock``. Accepts IB and Polygon keys;
    returns IB-shaped ``contract_key``. ``source`` kept for API compat.
    """
    _ = source  # unused — market.* has no source column
    if not contract_keys or not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return []
    keys = [k for k in contract_keys if k and str(k).strip()]
    if not keys:
        return []
    if since_ts is None:
        since_ts = datetime(1970, 1, 1)
    chunk_size = max(10, min(120, int(chunk_size)))

    out: List[Dict[str, Any]] = []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for i in range(0, len(keys), chunk_size):
                    batch = keys[i : i + chunk_size]
                    out.extend(_fetch_option_snapshots_eod_bridged(cur, batch, since_ts))
            return out
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_option_snapshots_eod_per_day failed: %s", e)
        return []

def get_report_option_atm_iv_daily(
    status_config: dict,
    symbol: str,
    expirations: List[str],
    source: str,
    since_date: date_type,
) -> List[Dict[str, Any]]:
    """Daily ATM IV rollup for IV volatility cone fast path (report_option_atm_iv_daily)."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    if not sym or not expirations:
        return []
    exp_clean = [str(e).strip() for e in expirations if e and len(str(e).strip()) == 8 and str(e).strip().isdigit()]
    if not exp_clean:
        return []
    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT symbol, expiry, trade_date, atm_iv, iv_call, iv_put, strike, underlying_price, source
                    FROM report_option_atm_iv_daily
                    WHERE symbol = %s AND source = %s
                      AND expiry = ANY(%s)
                      AND trade_date >= %s
                    ORDER BY expiry ASC, trade_date ASC
                    """,
                    (sym, src, exp_clean, since_date),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_report_option_atm_iv_daily failed: %s", e)
        return []


def get_corporate_actions(
    status_config: dict,
    symbol: str,
    action_type: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Corporate actions from massive_corporate_action, newest ex_date first."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                conditions = ["symbol = %s"]
                args: list = [sym]
                if action_type and action_type.strip():
                    conditions.append("action_type = %s")
                    args.append(action_type.strip().lower())
                where = " AND ".join(conditions)
                args.append(max(1, min(500, limit)))
                cur.execute(
                    f"""
                    SELECT symbol, action_type, ex_date, record_date, payment_date,
                           ratio_from, ratio_to, amount, description, source, created_at
                    FROM massive_corporate_action
                    WHERE {where}
                    ORDER BY ex_date DESC
                    LIMIT %s
                    """,
                    tuple(args),
                )
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_corporate_actions failed: %s", e)
        return []


def _norm_expiry_db(expiry: str) -> str:
    e = (expiry or "").strip()
    if len(e) >= 10 and e[4] == "-":
        return e[:4] + e[5:7] + e[8:10]
    return e


def get_option_bars(
    status_config: dict,
    symbol: str,
    expiry: str,
    strike: float,
    option_right: str,
    period: str,
    source: str = "massive",
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """OHLC for one option contract from market.option_daily (1 D) or market.option_minute.

    ``source`` is accepted for API compatibility but ignored (single Polygon vendor).
    Response includes ``source='massive'`` for downstream callers.
    """
    _ = source  # unused — market.* has no source column
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    per = (period or "1 min").strip()
    sym = (symbol or "").strip().upper()
    exp = _norm_expiry_date(expiry)
    r = (option_right or "").strip().upper()
    if r in ("CALL",):
        r = "C"
    if r in ("PUT",):
        r = "P"
    if not sym or exp is None:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if per.upper() == "1 D":
                    cur.execute(
                        """
                        SELECT extract(epoch from bar_date) AS time,
                               open, high, low, close, volume, vwap,
                               'massive' AS source
                        FROM market.option_daily
                        WHERE underlying = %s AND expiry = %s
                          AND strike = %s AND option_right = %s
                        ORDER BY bar_date DESC NULLS LAST
                        LIMIT %s
                        """,
                        (sym, exp, float(strike), r, max(1, min(500, limit))),
                    )
                else:
                    cur.execute(
                        """
                        SELECT extract(epoch from bar_time) AS time,
                               open, high, low, close, volume, vwap,
                               'massive' AS source
                        FROM market.option_minute
                        WHERE underlying = %s AND expiry = %s
                          AND strike = %s AND option_right = %s
                          AND period = %s
                        ORDER BY bar_time DESC NULLS LAST
                        LIMIT %s
                        """,
                        (
                            sym,
                            exp,
                            float(strike),
                            r,
                            _minute_period_db(per),
                            max(1, min(500, limit)),
                        ),
                    )
                rows = cur.fetchall()
            return [dict(x) for x in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_option_bars failed: %s", e)
        return []


def count_pending_massive_jobs(status_config: dict) -> int:
    """Count job_massive_backfill rows with status pending or running."""
    if MASSIVE_QUEUES_DISABLED:
        return 0
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return 0
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)::int FROM job_massive_backfill
                    WHERE status IN ('pending', 'running')
                    """
                )
                row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()
    except Exception as e:
        logger.debug("count_pending_massive_jobs failed: %s", e)
        return 0


def get_report_max_pain_rows(
    status_config: dict,
    *,
    symbol: Optional[str] = None,
    expiry: Optional[str] = None,
    trade_date_gte: Optional[str] = None,
    trade_date_lte: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Query report_option_max_pain_daily (source=massive)."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    lim = max(1, min(int(limit), 500))
    sym = (symbol or "").strip().upper() or None
    exp = (expiry or "").strip() or None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                conds = ["source = 'massive'"]
                args: List[Any] = []
                if sym:
                    conds.append("symbol = %s")
                    args.append(sym)
                if exp:
                    conds.append("expiry = %s")
                    args.append(exp)
                if trade_date_gte:
                    conds.append("trade_date >= %s")
                    args.append(trade_date_gte)
                if trade_date_lte:
                    conds.append("trade_date <= %s")
                    args.append(trade_date_lte)
                where = " AND ".join(conds)
                args.append(lim)
                cur.execute(
                    f"""
                    SELECT report_option_max_pain_daily_id, symbol, expiry, trade_date,
                           max_pain_strike, underlying_close, total_oi, computation_detail, source, created_at
                    FROM report_option_max_pain_daily
                    WHERE {where}
                    ORDER BY trade_date DESC, symbol, expiry
                    LIMIT %s
                    """,
                    tuple(args),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_report_max_pain_rows failed: %s", e)
        return []


def get_report_max_pain_latest_batch(
    status_config: dict,
    *,
    symbol: Optional[str] = None,
    limit: int = 80,
) -> tuple[List[Dict[str, Any]], Optional[str]]:
    """Rows for the latest trade_date present in report_option_max_pain_daily; returns (rows, trade_date_iso)."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return [], None
    lim = max(1, min(int(limit), 500))
    sym = (symbol or "").strip().upper() or None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT MAX(trade_date) FROM report_option_max_pain_daily WHERE source = 'massive'"
                )
                r0 = cur.fetchone()
                max_d = r0[0] if r0 else None
                if max_d is None:
                    return [], None
                td = max_d.isoformat() if hasattr(max_d, "isoformat") else str(max_d)
                if sym:
                    cur.execute(
                        """
                        SELECT report_option_max_pain_daily_id, symbol, expiry, trade_date,
                               max_pain_strike, underlying_close, total_oi, computation_detail, source, created_at
                        FROM report_option_max_pain_daily
                        WHERE source = 'massive' AND trade_date = %s AND symbol = %s
                        ORDER BY symbol, expiry
                        LIMIT %s
                        """,
                        (max_d, sym, lim),
                    )
                else:
                    cur.execute(
                        """
                        SELECT report_option_max_pain_daily_id, symbol, expiry, trade_date,
                               max_pain_strike, underlying_close, total_oi, computation_detail, source, created_at
                        FROM report_option_max_pain_daily
                        WHERE source = 'massive' AND trade_date = %s
                        ORDER BY symbol, expiry
                        LIMIT %s
                        """,
                        (max_d, lim),
                    )
                return [dict(r) for r in cur.fetchall()], td
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_report_max_pain_latest_batch failed: %s", e)
        return [], None


def get_massive_daily_checklist_data(
    status_config: dict,
    symbols: List[str],
    trade_date: str,
) -> Dict[str, Any]:
    """Per-symbol daily dimension status for UI checklist (PG + optional Redis WS).

    *trade_date* is the US session calendar date (YYYY-MM-DD) to evaluate against.
    """
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {"trade_date": trade_date, "symbols": {}, "error": "postgres not configured"}
    syms = [s.strip().upper() for s in symbols if s and str(s).strip()][:80]
    if not syms:
        return {"trade_date": trade_date, "symbols": {}}

    out_symbols: Dict[str, Any] = {}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                for sym in syms:
                    # Chain snapshot on trade_date in America/New_York (market.*)
                    cur.execute(
                        """
                        SELECT COUNT(*)::int, MAX(snapshot_ts)
                        FROM market.option_snapshot
                        WHERE UPPER(TRIM(underlying)) = %s
                          AND (snapshot_ts AT TIME ZONE 'America/New_York')::date = %s::date
                        """,
                        (sym, trade_date),
                    )
                    snap_row = cur.fetchone()
                    snap_cnt = int(snap_row[0]) if snap_row else 0
                    snap_max = snap_row[1]
                    if snap_cnt > 0:
                        daily_snapshot = {
                            "status": "complete",
                            "rows": snap_cnt,
                            "last_ts": snap_max.isoformat() if snap_max else None,
                        }
                    else:
                        daily_snapshot = {"status": "missing", "rows": 0}

                    cur.execute(
                        """
                        SELECT COUNT(*)::int, MAX(trade_date)
                        FROM market.option_open_interest
                        WHERE UPPER(TRIM(underlying)) = %s AND trade_date = %s::date
                        """,
                        (sym, trade_date),
                    )
                    oi_row = cur.fetchone()
                    oi_cnt = int(oi_row[0]) if oi_row else 0
                    if oi_cnt > 0:
                        daily_oi = {"status": "complete", "rows": oi_cnt, "trade_date": trade_date}
                    else:
                        cur.execute(
                            """
                            SELECT MAX(trade_date) FROM market.option_open_interest
                            WHERE UPPER(TRIM(underlying)) = %s
                            """,
                            (sym,),
                        )
                        ld = cur.fetchone()[0]
                        daily_oi = {
                            "status": "missing",
                            "last_trade_date": ld.isoformat() if ld is not None and hasattr(ld, "isoformat") else None,
                        }

                    cur.execute(
                        """
                        SELECT COUNT(*)::int
                        FROM report_option_max_pain_daily
                        WHERE symbol = %s AND source = 'massive' AND trade_date = %s::date
                        """,
                        (sym, trade_date),
                    )
                    mp_cnt = int(cur.fetchone()[0])
                    if mp_cnt > 0:
                        daily_mp = {"status": "complete", "rows": mp_cnt, "trade_date": trade_date}
                    else:
                        daily_mp = {"status": "missing"}

                    cur.execute(
                        """
                        SELECT MAX(created_at) FROM massive_corporate_action
                        WHERE symbol = %s AND source = 'massive'
                        """,
                        (sym,),
                    )
                    mx = cur.fetchone()[0]
                    if mx is not None:
                        from datetime import datetime, timezone

                        now = datetime.now(timezone.utc)
                        if getattr(mx, "tzinfo", None) is None:
                            mx_aware = mx.replace(tzinfo=timezone.utc)
                        else:
                            mx_aware = mx.astimezone(timezone.utc)
                        age_sec = (now - mx_aware).total_seconds()
                        if age_sec <= 7 * 86400:
                            daily_corp = {
                                "status": "complete",
                                "last_sync": mx.isoformat() if hasattr(mx, "isoformat") else str(mx),
                            }
                        else:
                            daily_corp = {
                                "status": "partial",
                                "last_sync": mx.isoformat() if hasattr(mx, "isoformat") else str(mx),
                            }
                    else:
                        daily_corp = {"status": "missing"}

                    out_symbols[sym] = {
                        "daily-snapshot": daily_snapshot,
                        "daily-oi": daily_oi,
                        "daily-max-pain": daily_mp,
                        "daily-corporate": daily_corp,
                    }
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_massive_daily_checklist_data failed: %s", e)
        return {"trade_date": trade_date, "symbols": out_symbols, "error": str(e)}

    # WS status: global (same for all symbols)
    ws_block: Dict[str, Any] = {"status": "missing", "connected": False}
    try:
        from bifrost_core.monitor.redis_url import redis_url_from_config

        rurl = redis_url_from_config(status_config)
        if rurl:
            import redis

            from bifrost_core.core.redis_health_keys import hgetall_massive_ws_status

            r = redis.from_url(rurl, decode_responses=True)
            h = hgetall_massive_ws_status(r)
            if h:
                connected = h.get("connected") == "1"
                lm = h.get("last_msg_ts")
                age_s: Optional[float] = None
                if lm is not None:
                    try:
                        import time as _time

                        age_s = max(0.0, _time.time() - float(lm))
                    except (TypeError, ValueError):
                        age_s = None
                if connected and age_s is not None and age_s < 120:
                    ws_block = {"status": "complete", "connected": True, "last_msg_age_s": age_s}
                elif connected:
                    ws_block = {"status": "degraded", "connected": True, "last_msg_age_s": age_s}
                else:
                    ws_block = {"status": "degraded", "connected": False, "last_msg_age_s": age_s}
    except Exception:
        pass

    for sym in out_symbols:
        out_symbols[sym]["daily-ws-alive"] = dict(ws_block)

    return {"trade_date": trade_date, "symbols": out_symbols}


def get_latest_massive_job_by_kind(
    status_config: dict, kind: str
) -> Optional[Dict[str, Any]]:
    """Latest job row for a given kind (newest first)."""
    if MASSIVE_QUEUES_DISABLED:
        return None
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    k = (kind or "").strip().lower()
    if not k:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT job_massive_backfill_id, kind, payload, status, result, celery_task_id, created_at, updated_at
                    FROM job_massive_backfill
                    WHERE kind = %s
                    ORDER BY job_massive_backfill_id DESC
                    LIMIT 1
                    """,
                    (k,),
                )
                row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_latest_massive_job_by_kind failed: %s", e)
        return None


def _stock_close_on_date(cur: Any, symbol: str, trade_date: date_type) -> Optional[float]:
    """Latest stock_daily close on calendar day (if any)."""
    try:
        cur.execute(
            """
            SELECT close FROM market.stock_daily
            WHERE symbol = %s AND bar_date = %s
            LIMIT 1
            """,
            (symbol, trade_date),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return None


def get_stock_day_series_for_sepa(
    status_config: dict,
    symbols: List[str],
    *,
    lookback_days: int = 400,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read market.stock_daily rows for SEPA phase-1 technical screening.

    Returns an ascending bar series per symbol with keys:
    ``symbol, bar_time, open, high, low, close, volume, source``.
    ``source`` is accepted for API compatibility but ignored.
    """
    _ = source  # unused — market.* has no source column
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    lb = max(260, min(int(lookback_days), 3000))
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                      UPPER(TRIM(symbol)) AS symbol,
                      bar_date AS bar_time,
                      open,
                      high,
                      low,
                      close,
                      volume,
                      'massive' AS source
                    FROM market.stock_daily
                    WHERE UPPER(TRIM(symbol)) = ANY(%s)
                      AND bar_date >= (CURRENT_DATE - (%s || ' days')::interval)::date
                    ORDER BY UPPER(TRIM(symbol)), bar_date ASC
                    """,
                    (syms, lb),
                )
                rows = cur.fetchall() or []
            for row in rows:
                sym = str((row or {}).get("symbol") or "").strip().upper()
                if not sym:
                    continue
                if sym not in out:
                    out[sym] = []
                out[sym].append(dict(row))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_stock_day_series_for_sepa failed: %s", e)
        return out
    return out


def get_stock_day_close_series_for_crs(
    status_config: dict,
    symbols: List[str],
    *,
    lookback_days: int = 420,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read market.stock_daily close series for CRS calculation."""
    _ = source  # unused — market.* has no source column
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    lb = max(260, min(int(lookback_days), 3000))
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                      UPPER(TRIM(symbol)) AS symbol,
                      bar_date AS bar_time,
                      close
                    FROM market.stock_daily
                    WHERE UPPER(TRIM(symbol)) = ANY(%s)
                      AND bar_date >= (CURRENT_DATE - (%s || ' days')::interval)::date
                      AND close IS NOT NULL
                    ORDER BY UPPER(TRIM(symbol)), bar_date ASC
                    """,
                    (syms, lb),
                )
                rows = cur.fetchall() or []
            for row in rows:
                sym = str((row or {}).get("symbol") or "").strip().upper()
                if not sym:
                    continue
                out.setdefault(sym, []).append(dict(row))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_stock_day_close_series_for_crs failed: %s", e)
        return out
    return out


def _recent_corporate_action_flag(cur: Any, symbol: str) -> bool:
    try:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM massive_corporate_action
              WHERE symbol = %s AND source = 'massive'
                AND created_at >= (now() AT TIME ZONE 'utc') - interval '30 days'
            )
            """,
            (symbol,),
        )
        row = cur.fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _load_oi_rows_from_chain_snapshots(
    cur: Any,
    symbol: str,
    exp_norm: str,
) -> Tuple[List[Dict[str, Any]], Optional[date_type], Optional[float]]:
    """When EOD ``market.option_open_interest`` is empty, use latest snapshot OI per contract.

    Option Discovery syncs chain quotes into ``market.option_snapshot`` (often with open_interest)
    even when the watchlist EOD OI job has not populated ``market.option_open_interest``.

    Returns (raw_rows for strike_map_for_expiry, max snapshot calendar date, representative underlying price).
    """
    from bifrost_core.monitor.reader.max_pain_math import normalize_expiry_for_oi

    sym = (symbol or "").strip().upper()
    exp_date = _norm_expiry_date(exp_norm)
    if not sym or not exp_norm or exp_date is None:
        return [], None, None

    best_rows: List[Dict[str, Any]] = []
    best_td: Optional[date_type] = None
    best_uc: Optional[float] = None

    try:
        cur.execute(
            """
            SELECT DISTINCT ON (oc.option_ticker)
                oc.expiry, oc.strike, oc.option_right, os.open_interest,
                os.snapshot_ts, os.underlying_price
            FROM market.option_contract oc
            INNER JOIN market.v_option_snapshot_with_stock os
              ON os.option_ticker = oc.option_ticker
            WHERE oc.underlying = %s AND oc.expiry = %s
              AND os.open_interest IS NOT NULL AND os.open_interest > 0
            ORDER BY oc.option_ticker, os.snapshot_ts DESC
            """,
            (sym, exp_date),
        )
        rows = cur.fetchall() or []
    except Exception as ex:
        logger.debug("_load_oi_rows_from_chain_snapshots: %s", ex)
        try:
            cur.connection.rollback()
        except Exception:
            pass
        return [], None, None

    raw: List[Dict[str, Any]] = []
    max_ts: Optional[datetime] = None
    uc_vals: List[float] = []
    for row in rows:
        exp_v, strike, ort, oi, snap_ts, und = row[0], row[1], row[2], row[3], row[4], row[5]
        exp_key = exp_v.isoformat()[:10].replace("-", "") if hasattr(exp_v, "isoformat") else str(exp_v or "")
        if normalize_expiry_for_oi(exp_key) != exp_norm:
            continue
        raw.append(
            {
                "expiry": exp_v.isoformat() if hasattr(exp_v, "isoformat") else str(exp_v),
                "strike": float(strike),
                "option_right": str(ort or "").strip().upper(),
                "open_interest": int(oi),
            }
        )
        if snap_ts is not None:
            if isinstance(snap_ts, datetime):
                ts = snap_ts
            else:
                try:
                    ts = datetime.fromisoformat(str(snap_ts).replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    ts = None
            if ts is not None:
                if max_ts is None or ts > max_ts:
                    max_ts = ts
        if und is not None:
            try:
                ufv = float(und)
                if ufv > 0:
                    uc_vals.append(ufv)
            except (TypeError, ValueError):
                pass

    best_rows = raw
    if max_ts is not None:
        best_td = max_ts.date()
    if uc_vals:
        best_uc = sum(uc_vals) / len(uc_vals)

    return best_rows, best_td, best_uc


def compute_max_pain_live_from_db(
    status_config: dict,
    *,
    symbol: str,
    expiry: str,
    trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Real-time Max Pain from market.option_open_interest, with chain-snapshot OI fallback."""
    from bifrost_core.monitor.reader.max_pain_math import (
        compute_max_pain_curve,
        normalize_expiry_for_oi,
        strike_map_for_expiry,
    )

    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym = (symbol or "").strip().upper()
    exp = (expiry or "").strip()
    if not sym or not exp:
        return {"ok": False, "error": "symbol and expiry are required"}
    exp_norm = normalize_expiry_for_oi(exp)
    exp_d = _expiry_to_date_param(exp_norm)
    if exp_d is None:
        return {"ok": False, "error": "invalid expiry"}
    explicit_trade_date = bool(trade_date and str(trade_date).strip())
    oi_basis = "eod_open_interest_daily"
    snapshot_uc: Optional[float] = None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                td_use: Optional[date_type] = None
                raw_rows: List[Dict[str, Any]] = []
                if explicit_trade_date:
                    raw_td = str(trade_date).strip()[:10]
                    td_use = date_type.fromisoformat(raw_td)
                    cur.execute(
                        """
                        SELECT expiry, strike, option_right, open_interest
                        FROM market.option_open_interest
                        WHERE underlying = %s AND expiry = %s::date AND trade_date = %s
                        """,
                        (sym, exp_d, td_use),
                    )
                    raw_rows = [
                        {"expiry": row[0], "strike": row[1], "option_right": row[2], "open_interest": row[3]}
                        for row in cur.fetchall()
                    ]
                else:
                    cur.execute(
                        """
                        SELECT MAX(trade_date) FROM market.option_open_interest
                        WHERE underlying = %s AND expiry = %s::date
                        """,
                        (sym, exp_d),
                    )
                    r0 = cur.fetchone()
                    if r0 and r0[0] is not None:
                        d0 = r0[0]
                        td_use = d0 if isinstance(d0, date_type) else date_type.fromisoformat(str(d0)[:10])
                    if td_use is not None:
                        cur.execute(
                            """
                            SELECT expiry, strike, option_right, open_interest
                            FROM market.option_open_interest
                            WHERE underlying = %s AND expiry = %s::date AND trade_date = %s
                            """,
                            (sym, exp_d, td_use),
                        )
                        raw_rows = [
                            {"expiry": row[0], "strike": row[1], "option_right": row[2], "open_interest": row[3]}
                            for row in cur.fetchall()
                        ]

                skmap = strike_map_for_expiry(raw_rows, exp)

                if not explicit_trade_date and (td_use is None or not skmap):
                    snap_rows, snap_td, snap_uc = _load_oi_rows_from_chain_snapshots(cur, sym, exp_norm)
                    if snap_rows:
                        raw_rows = snap_rows
                        td_use = snap_td if snap_td is not None else date_type.today()
                        oi_basis = "chain_snapshot"
                        snapshot_uc = snap_uc
                        skmap = strike_map_for_expiry(raw_rows, exp)
                    else:
                        snapshot_uc = None

                if td_use is None or not skmap:
                    return {
                        "ok": False,
                        "error": (
                            "No open interest for this symbol/expiry. "
                            "Run EOD OI sync or load chain snapshots (quotes) so PostgreSQL has OI."
                        ),
                        "symbol": sym,
                        "expiry": exp_norm,
                    }

                mp_strike, min_pain, points, total_oi = compute_max_pain_curve(skmap)
                underlying_close = _stock_close_on_date(cur, sym, td_use)
                if underlying_close is None and snapshot_uc is not None and snapshot_uc > 0:
                    underlying_close = snapshot_uc
                corp_flag = _recent_corporate_action_flag(cur, sym)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("compute_max_pain_live_from_db failed: %s", e)
        return {"ok": False, "error": str(e)}

    uc = underlying_close
    dist_pct: Optional[float] = None
    if uc is not None and uc > 0:
        dist_pct = abs(float(mp_strike) - float(uc)) / float(uc)

    out: Dict[str, Any] = {
        "ok": True,
        "symbol": sym,
        "expiry": exp_norm,
        "trade_date": td_use.isoformat(),
        "max_pain_strike": mp_strike,
        "min_pain_value": min_pain,
        "total_oi": total_oi,
        "underlying_close": uc,
        "distance_to_max_pain_pct": dist_pct,
        "pain_by_strike": points,
        "recent_corporate_action": corp_flag,
        "oi_basis": oi_basis,
    }
    return out


def compute_max_pain_history_from_db(
    status_config: dict,
    *,
    symbol: str,
    expiry: str,
    lookback_days: int = 90,
) -> Dict[str, Any]:
    """Time series of max pain per trade_date (recomputed from OI; no report table)."""
    from bifrost_core.monitor.reader.max_pain_math import (
        compute_max_pain_curve,
        normalize_expiry_for_oi,
        strike_map_for_expiry,
    )

    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {"ok": False, "error": "PostgreSQL not configured", "series": []}
    sym = (symbol or "").strip().upper()
    exp = (expiry or "").strip()
    if not sym or not exp:
        return {"ok": False, "error": "symbol and expiry are required", "series": []}
    exp_norm = normalize_expiry_for_oi(exp)
    lb = max(7, min(int(lookback_days), 365))
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                exp_d = _expiry_to_date_param(exp_norm)
                if exp_d is None:
                    return {"ok": False, "error": "invalid expiry", "series": []}
                cur.execute(
                    """
                    WITH latest AS (
                      SELECT MAX(trade_date) AS max_td FROM market.option_open_interest
                      WHERE underlying = %s AND expiry = %s::date
                    )
                    SELECT o.trade_date, o.expiry, o.strike, o.option_right, o.open_interest
                    FROM market.option_open_interest o, latest
                    WHERE o.underlying = %s AND o.expiry = %s::date
                      AND latest.max_td IS NOT NULL
                      AND o.trade_date >= (latest.max_td - %s::integer)
                      AND o.trade_date <= latest.max_td
                    ORDER BY o.trade_date, o.strike
                    """,
                    (sym, exp_d, sym, exp_d, lb),
                )
                all_rows = cur.fetchall()
                cur.execute(
                    """
                    WITH latest AS (
                      SELECT MAX(trade_date) AS max_td FROM market.option_open_interest
                      WHERE underlying = %s AND expiry = %s::date
                    )
                    SELECT trade_date, close FROM (
                      SELECT DISTINCT ON (o.bar_date)
                        o.bar_date AS trade_date, o.close
                      FROM market.stock_daily o, latest
                      WHERE o.symbol = %s AND latest.max_td IS NOT NULL
                        AND o.bar_date >= (latest.max_td - %s::integer)
                        AND o.bar_date <= latest.max_td
                      ORDER BY o.bar_date ASC
                    ) x
                    ORDER BY trade_date
                    """,
                    (sym, exp_d, sym, lb),
                )
                stock_rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("compute_max_pain_history_from_db failed: %s", e)
        return {"ok": False, "error": str(e), "series": []}

    close_by_day: Dict[str, float] = {}
    for r in stock_rows:
        d0 = r[0]
        if d0 is None:
            continue
        d = d0.isoformat()[:10] if hasattr(d0, "isoformat") else str(d0)[:10]
        if r[1] is not None:
            close_by_day[d] = float(r[1])

    by_td: Dict[str, List[Dict[str, Any]]] = {}
    for row in all_rows:
        td0 = row[0]
        if td0 is None:
            continue
        td_s = td0.isoformat()[:10] if hasattr(td0, "isoformat") else str(td0)[:10]
        by_td.setdefault(td_s, []).append(
            {
                "expiry": row[1],
                "strike": row[2],
                "option_right": row[3],
                "open_interest": row[4],
            }
        )

    series: List[Dict[str, Any]] = []
    for td_s in sorted(by_td.keys()):
        raw_rows = by_td[td_s]
        skmap = strike_map_for_expiry(raw_rows, exp)
        if not skmap:
            continue
        mp_strike, _min_p, _pts, tot_oi = compute_max_pain_curve(skmap)
        series.append(
            {
                "trade_date": td_s,
                "max_pain_strike": mp_strike,
                "total_oi": tot_oi,
                "underlying_close": close_by_day.get(td_s),
            }
        )

    return {"ok": True, "symbol": sym, "expiry": exp_norm, "series": series}


def _right_from_ref_contract_type(ct: str) -> str:
    u = (ct or "").upper()
    if u in ("CALL", "C"):
        return "C"
    if u in ("PUT", "P"):
        return "P"
    return "C"


def is_us_equity_regular_session_et(now: Optional[datetime] = None) -> bool:
    """Weekday 09:30–16:00 America/New_York (no holiday calendar)."""
    et = ZoneInfo("America/New_York")
    dt = now or datetime.now(et)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=et)
    else:
        dt = dt.astimezone(et)
    if dt.weekday() >= 5:
        return False
    t = dt.time()
    return time_of_day(9, 30) <= t < time_of_day(16, 0)


def get_option_expirations_from_contracts_db(status_config: dict, symbol: str) -> List[str]:
    """Distinct expirations (YYYYMMDD) from market.option_contract for an underlying."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT expiry FROM market.option_contract
                    WHERE underlying = %s
                    ORDER BY expiry
                    """,
                    (sym,),
                )
                out: List[str] = []
                for r in cur.fetchall():
                    if not r or r[0] is None:
                        continue
                    v = r[0]
                    if hasattr(v, "strftime"):
                        out.append(v.strftime("%Y%m%d"))
                    else:
                        s = str(v).strip()
                        if len(s) >= 10 and s[4] == "-":
                            out.append(s[:4] + s[5:7] + s[8:10])
                        else:
                            out.append(s)
                return out
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_option_expirations_from_contracts_db failed: %s", e)
        return []


def get_strikes_for_expiry_from_contracts_db(
    status_config: dict, symbol: str, expiration: str
) -> List[float]:
    """Distinct strikes for symbol + expiry from market.option_contract."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    exp = _norm_expiry_date((expiration or "").strip())
    if not sym or exp is None:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT strike FROM market.option_contract
                    WHERE underlying = %s AND expiry = %s
                    ORDER BY strike
                    """,
                    (sym, exp),
                )
                out: List[float] = []
                for r in cur.fetchall():
                    if r and r[0] is not None:
                        try:
                            out.append(float(r[0]))
                        except (TypeError, ValueError):
                            pass
                return out
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_strikes_for_expiry_from_contracts_db failed: %s", e)
        return []


def get_option_expiration_cache_snapshot(
    status_config: dict, symbol: str, source: str = "massive"
) -> Optional[Tuple[List[str], Optional[datetime]]]:
    """Return (sorted expirations, max updated_at) from market.option_expiration.

    ``source`` is accepted for API compatibility but ignored (no source column).
    """
    _ = source
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT expiry, updated_at FROM market.option_expiration
                    WHERE underlying = %s
                    ORDER BY expiry
                    """,
                    (sym,),
                )
                rows = cur.fetchall()
            if not rows:
                return None
            exps: List[str] = []
            max_u: Optional[datetime] = None
            for r in rows:
                exp_raw = r[0]
                if hasattr(exp_raw, "isoformat"):
                    exps.append(exp_raw.isoformat()[:10])
                else:
                    exps.append(str(exp_raw).strip())
                u = r[1]
                if u is not None:
                    if hasattr(u, "tzinfo") and u.tzinfo is None:
                        u = u.replace(tzinfo=ZoneInfo("UTC"))
                    if max_u is None or u > max_u:
                        max_u = u
            return (exps, max_u)
        finally:
            conn.close()
    except ProgrammingError as e:
        if getattr(e, "pgcode", None) == "42P01":
            return None
        logger.debug("get_option_expiration_cache_snapshot: %s", e)
        return None
    except Exception as e:
        logger.debug("get_option_expiration_cache_snapshot failed: %s", e)
        return None


def replace_option_expiration_cache(
    status_config: dict,
    symbol: str,
    expirations: List[str],
    source: str = "massive",
) -> None:
    """Replace full expiration list for a symbol in market.option_expiration.

    ``source`` is accepted for API compatibility but ignored (no source column).
    """
    _ = source
    sym = (symbol or "").strip().upper()
    if not sym or not status_config:
        return
    if not expirations:
        return
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM market.option_expiration WHERE underlying = %s",
                    (sym,),
                )
                for raw in expirations:
                    exp_d = _expiry_to_date_param(str(raw))
                    if not exp_d:
                        continue
                    cur.execute(
                        """
                        INSERT INTO market.option_expiration (underlying, expiry, updated_at)
                        VALUES (%s, %s::date, now())
                        ON CONFLICT (underlying, expiry) DO UPDATE SET updated_at = now()
                        """,
                        (sym, exp_d),
                    )
            conn.commit()
        finally:
            conn.close()
    except ProgrammingError as e:
        if getattr(e, "pgcode", None) == "42P01":
            return
        logger.warning("replace_option_expiration_cache failed: %s", e)
    except Exception as e:
        logger.warning("replace_option_expiration_cache failed: %s", e)


def upsert_option_contracts_from_reference_rows(
    status_config: dict,
    underlying: str,
    contract_rows: List[Dict[str, Any]],
) -> int:
    """Upsert option_contracts from Polygon reference contract rows."""
    from bifrost_worker.data.massive.vendor.client import contract_key_from_parts

    underlying = (underlying or "").strip().upper()
    if not contract_rows or not underlying:
        return 0
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return 0
    n = 0
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                for row in contract_rows:
                    exp = row.get("expiration_date") or row.get("expiration") or ""
                    if not exp:
                        continue
                    ed = _norm_expiry_db(str(exp)[:10])
                    if len(ed) != 8 or not ed.isdigit():
                        continue
                    sp = row.get("strike_price")
                    if sp is None:
                        continue
                    try:
                        strike = float(sp)
                    except (TypeError, ValueError):
                        continue
                    ort = _right_from_ref_contract_type(str(row.get("contract_type") or "call"))
                    ticker = (row.get("ticker") or "").strip() or None
                    ck = contract_key_from_parts(underlying, ed, strike, ort)
                    cur.execute(
                        """
                        INSERT INTO option_contracts (contract_key, symbol, expiry, strike, option_right, massive_option_ticker, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (contract_key) DO UPDATE SET
                          massive_option_ticker = COALESCE(EXCLUDED.massive_option_ticker, option_contracts.massive_option_ticker)
                        """,
                        (ck, underlying, ed, strike, ort, ticker),
                    )
                    n += 1
                # Record that the contracts sync ran for this symbol, regardless of
                # whether any new rows were inserted (all conflicts = no new created_at).
                cur.execute(
                    """
                    INSERT INTO job_ticker_reference_state (sync_kind, last_cursor, status, updated_at)
                    VALUES (%s, NULL, 'done', now())
                    ON CONFLICT (sync_kind) DO UPDATE SET
                      status = 'done',
                      updated_at = now()
                    """,
                    (f"option_contracts:{underlying}",),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("upsert_option_contracts_from_reference_rows failed: %s", e)
    return n


def refresh_expirations_from_massive_api(
    status_config: dict,
    config: dict,
    symbol: str,
    expiration_date: Optional[str] = None,
    include_debug: bool = False,
    skip_persist: bool = False,
) -> Dict[str, Any]:
    """Fetch expirations/strikes from Massive REST and persist contracts + expiration cache."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    ms = get_massive_settings(config)
    if not ms["api_key"]:
        return {"expirations": [], "strikes": [], "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    result = client.fetch_expirations_and_strikes(
        symbol,
        include_debug=include_debug,
        expiration_date=expiration_date,
        collect_contract_rows=True,
    )
    rows_upserted = 0
    if status_config and not result.get("error") and not skip_persist:
        rows = result.get("contract_rows") or []
        try:
            rows_upserted = upsert_option_contracts_from_reference_rows(status_config, symbol, rows)
            if not (expiration_date or "").strip():
                replace_option_expiration_cache(status_config, symbol, result.get("expirations") or [], source="massive")
        except Exception as e:
            logger.warning("refresh_expirations_from_massive_api persist failed: %s", e)
    result["rows_upserted"] = rows_upserted
    return result


def refresh_expirations_watchlist_batch(
    status_config: dict,
    config: dict,
    symbols: List[str],
    *,
    max_symbols: int = 24,
) -> Dict[str, Any]:
    """Refresh expiration cache + contracts for a batch of underlyings (Celery beat)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings

    ms = get_massive_settings(config)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured", "refreshed": 0}
    syms = [s.strip().upper() for s in symbols if s]
    syms = list(dict.fromkeys(syms))[: max(1, max_symbols)]
    ok = 0
    errors: List[str] = []
    gap = 0.2
    for i, sym in enumerate(syms):
        if i > 0:
            time.sleep(gap)
        try:
            r = refresh_expirations_from_massive_api(
                status_config, config, sym, expiration_date=None, include_debug=False
            )
            if r.get("error"):
                errors.append(f"{sym}: {r.get('error')}")
            else:
                ok += 1
        except Exception as e:
            errors.append(f"{sym}: {e}")
    return {"ok": True, "refreshed": ok, "errors": errors[:20], "batch_size": len(syms)}


def _ensure_sepa_fundamentals_cache_table(cur: Any) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS research_sepa_fundamentals_cache (
            symbol text NOT NULL,
            rule_version text NOT NULL,
            payload jsonb NOT NULL,
            source text DEFAULT 'massive',
            fetched_at timestamptz NOT NULL DEFAULT now(),
            expire_at timestamptz NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, rule_version)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_research_sepa_fund_cache_expire
        ON research_sepa_fundamentals_cache (expire_at)
        """
    )


def get_sepa_fundamentals_cache_snapshot(
    status_config: dict,
    symbol: str,
    *,
    rule_version: str,
) -> Optional[Dict[str, Any]]:
    sym = (symbol or "").strip().upper()
    if not sym or not status_config:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_sepa_fundamentals_cache_table(cur)
                cur.execute(
                    """
                    SELECT payload, fetched_at, expire_at, source
                    FROM research_sepa_fundamentals_cache
                    WHERE symbol = %s AND rule_version = %s AND expire_at > now()
                    LIMIT 1
                    """,
                    (sym, rule_version),
                )
                row = cur.fetchone()
            conn.commit()
            if not row:
                return None
            payload = row.get("payload")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = None
            if not isinstance(payload, dict):
                return None
            return {
                "symbol": sym,
                "payload": payload,
                "source": row.get("source"),
                "fetched_at": row.get("fetched_at"),
                "expire_at": row.get("expire_at"),
            }
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_sepa_fundamentals_cache_snapshot failed: %s", e)
        return None


def upsert_sepa_fundamentals_cache(
    status_config: dict,
    symbol: str,
    payload: Dict[str, Any],
    *,
    rule_version: str,
    source: str = "massive",
    ttl_sec: int = 21600,
) -> bool:
    sym = (symbol or "").strip().upper()
    if not sym or not status_config or not isinstance(payload, dict):
        return False
    ttl = max(60, int(ttl_sec))
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                _ensure_sepa_fundamentals_cache_table(cur)
                cur.execute(
                    """
                    INSERT INTO research_sepa_fundamentals_cache
                        (symbol, rule_version, payload, source, fetched_at, expire_at, updated_at)
                    VALUES (%s, %s, %s::jsonb, %s, now(), now() + (%s || ' seconds')::interval, now())
                    ON CONFLICT (symbol, rule_version) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        expire_at = EXCLUDED.expire_at,
                        updated_at = now()
                    """,
                    (sym, rule_version, json.dumps(payload), source, str(ttl)),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.debug("upsert_sepa_fundamentals_cache failed: %s", e)
        return False


# ── Tier 2–4 batch readers (technical_engine) ────────────────────────────────


def get_spy_close_series(
    status_config: dict,
    *,
    lookback_days: int = 420,
    source: str = "massive",
) -> List[float]:
    """Read SPY daily closes (ascending) from market.stock_daily. Shared by all symbols."""
    _ = source  # unused — market.* has no source column
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    lb = max(260, min(int(lookback_days), 3000))
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT close
                    FROM market.stock_daily
                    WHERE UPPER(TRIM(symbol)) = 'SPY'
                      AND bar_date >= (CURRENT_DATE - (%s || ' days')::interval)::date
                      AND close IS NOT NULL
                    ORDER BY bar_date ASC
                    """,
                    (lb,),
                )
                rows = cur.fetchall() or []
            return [float(r[0]) for r in rows if r[0] is not None]
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_spy_close_series failed: %s", e)
        return []


def get_short_interest_recent(
    status_config: dict,
    symbols: List[str],
    *,
    settlements: int = 6,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read recent short interest rows per symbol (settlement_date DESC).

    Reads ``market.stock_financials`` where ``report_type = 'short_interest'`` and
    unpacks jsonb ``data``. ``source`` kept for API compat.
    """
    _ = source  # unused — market.* has no source column
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                      UPPER(TRIM(symbol)) AS symbol,
                      period_date AS settlement_date,
                      COALESCE(
                        (data->>'short_interest')::bigint,
                        (data->>'short_interest_shares')::bigint,
                        (data->>'short_shares')::bigint
                      ) AS short_interest,
                      COALESCE(
                        (data->>'avg_daily_volume')::bigint,
                        (data->>'avg_daily_volume_consolidated')::bigint
                      ) AS avg_daily_volume,
                      (data->>'days_to_cover')::double precision AS days_to_cover
                    FROM (
                      SELECT *,
                        ROW_NUMBER() OVER (
                          PARTITION BY UPPER(TRIM(symbol)) ORDER BY period_date DESC
                        ) AS rn
                      FROM market.stock_financials
                      WHERE UPPER(TRIM(symbol)) = ANY(%s)
                        AND report_type = 'short_interest'
                    ) sub
                    WHERE rn <= %s
                    ORDER BY symbol, settlement_date DESC
                    """,
                    (syms, settlements),
                )
                for row in cur.fetchall() or []:
                    sym = str((row or {}).get("symbol") or "").strip().upper()
                    if sym:
                        out.setdefault(sym, []).append(dict(row))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_short_interest_recent failed: %s", e)
    return out


def get_short_volume_recent(
    status_config: dict,
    symbols: List[str],
    *,
    trade_days: int = 60,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read recent short volume rows per symbol (trade_date DESC).

    Reads ``market.stock_financials`` where ``report_type = 'short_volume'`` and
    unpacks jsonb ``data``. ``source`` kept for API compat.
    """
    _ = source  # unused — market.* has no source column
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                      UPPER(TRIM(symbol)) AS symbol,
                      period_date AS trade_date,
                      (data->>'short_volume')::bigint AS short_volume,
                      (data->>'short_volume_ratio')::double precision AS short_volume_ratio,
                      (data->>'total_volume')::bigint AS total_volume
                    FROM (
                      SELECT *,
                        ROW_NUMBER() OVER (
                          PARTITION BY UPPER(TRIM(symbol)) ORDER BY period_date DESC
                        ) AS rn
                      FROM market.stock_financials
                      WHERE UPPER(TRIM(symbol)) = ANY(%s)
                        AND report_type = 'short_volume'
                    ) sub
                    WHERE rn <= %s
                    ORDER BY symbol, trade_date DESC
                    """,
                    (syms, trade_days),
                )
                for row in cur.fetchall() or []:
                    sym = str((row or {}).get("symbol") or "").strip().upper()
                    if sym:
                        out.setdefault(sym, []).append(dict(row))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_short_volume_recent failed: %s", e)
    return out
