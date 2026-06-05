"""Canonical Redis broker queue names for Celery (single source of truth).

Used by Ops queue summary, worker profiles, and UI — same strings as ``celery -Q`` lists.

Stock aggregate jobs use kind ``feed_stocks_aggregate`` (legacy ``stock_ohlc_sync``) on ``stocks_massive`` / ``stocks_massive_high``.

Option contract OHLC / pool jobs use kind ``feed_options_aggregate`` (legacy ``aggregates``) on ``options_massive`` / ``options_massive_high``.

Options last-trade / quotes / historical trades proxy jobs use kind ``feed_options_trades_quotes`` (legacy ``trades_quotes``) on ``options_massive`` / ``options_massive_high``.

Option reference contracts (list/detail/upsert/backfill) jobs use kind ``feed_option_contracts`` (legacy ``contracts``) on ``options_massive`` / ``options_massive_high``.

Full tickers reference universe sync uses kind ``feed_stocks_tickers_reference_universe`` (legacy ``ticker_reference_universe`` / ``stock_reference_universe``) on ``stocks_massive`` / ``stocks_massive_high``.

Ticker-reference overview jobs use kind ``feed_stocks_tickers_overview`` (legacy ``ticker_reference_overview`` / ``stock_reference_overview``) on ``stocks_massive`` / ``stocks_massive_high``.

Ticker-reference related-peers jobs use kind ``feed_stocks_tickers_related`` (legacy ``ticker_reference_related`` / ``stock_reference_related``) on ``stocks_massive`` / ``stocks_massive_high``.

Ticker types dictionary jobs (GET /v3/reference/tickers/types) use kind ``feed_stocks_tickers_types`` (legacy ``ticker_reference_ticker_types`` / ``ticker_reference_instrument_types`` / ``stock_reference_instrument_types``) on ``stocks_massive`` / ``stocks_massive_high``.

IB historical bars backfill uses queue ``stocks_ib``.

Display names: **authoritative** map ``ops.celery.broker_queue_display_names`` in YAML; merged into
GET /ops/celery/capabilities ``broker_queue_labels`` and Ops queue summary ``display_name`` per row.
Optional fallback from ``ops.worker_profiles`` ``label`` when a queue key is missing from the map.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Final, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Redis LIST keys (stable for workers and broker).
BROKER_QUEUE_STOCKS_IB: Final[str] = "stocks_ib"
BROKER_QUEUE_STOCKS_MASSIVE_HIGH: Final[str] = "stocks_massive_high"
BROKER_QUEUE_STOCKS_MASSIVE: Final[str] = "stocks_massive"
BROKER_QUEUE_OPTIONS_MASSIVE_HIGH: Final[str] = "options_massive_high"
BROKER_QUEUE_OPTIONS_MASSIVE: Final[str] = "options_massive"

# Default order when ``ops.celery.canonical_queue_order`` is absent (tests, tools without merged YAML).
CANONICAL_BROKER_QUEUE_NAMES: Final[Tuple[str, ...]] = (
    BROKER_QUEUE_STOCKS_IB,
    BROKER_QUEUE_STOCKS_MASSIVE_HIGH,
    BROKER_QUEUE_STOCKS_MASSIVE,
    BROKER_QUEUE_OPTIONS_MASSIVE_HIGH,
    BROKER_QUEUE_OPTIONS_MASSIVE,
)


def load_canonical_broker_queue_names(config: Optional[dict]) -> Tuple[str, ...]:
    """Return ordered broker queue keys for default multi-queue worker and Ops summaries.

    Reads ``ops.celery.canonical_queue_order``; falls back to :data:`CANONICAL_BROKER_QUEUE_NAMES`.
    """
    if not config or not isinstance(config, dict):
        return CANONICAL_BROKER_QUEUE_NAMES
    ops = config.get("ops") or {}
    if not isinstance(ops, dict):
        return CANONICAL_BROKER_QUEUE_NAMES
    celery = ops.get("celery") or {}
    if not isinstance(celery, dict):
        return CANONICAL_BROKER_QUEUE_NAMES
    raw = celery.get("canonical_queue_order")
    if raw is None:
        return CANONICAL_BROKER_QUEUE_NAMES
    if not isinstance(raw, list):
        logger.warning("ops.celery.canonical_queue_order must be a list; using default canonical queues")
        return CANONICAL_BROKER_QUEUE_NAMES
    out: List[str] = []
    for x in raw:
        s = str(x).strip()
        if s:
            out.append(s)
    if not out:
        return CANONICAL_BROKER_QUEUE_NAMES
    return tuple(out)


def _queues_declared_in_worker_profiles(ops: dict) -> Tuple[str, ...]:
    """Union of all ``queues`` entries under ``ops.worker_profiles`` (stable order not guaranteed)."""
    raw = ops.get("worker_profiles") or {}
    if not isinstance(raw, dict):
        return ()
    seen: List[str] = []
    for _pk, ent in raw.items():
        if not isinstance(ent, dict):
            continue
        qs = ent.get("queues") or []
        if isinstance(qs, str):
            qs = [qs]
        for q in qs:
            s = str(q).strip()
            if s and s not in seen:
                seen.append(s)
    return tuple(seen)


def ops_celery_config_validation_errors(config: Optional[dict]) -> List[str]:
    """Return human-readable issues when ``canonical_queue_order`` and ``worker_profiles`` disagree."""
    errors: List[str] = []
    if not config or not isinstance(config, dict):
        return errors
    ops = config.get("ops") or {}
    if not isinstance(ops, dict):
        return errors
    celery = ops.get("celery") or {}
    if not isinstance(celery, dict):
        return errors
    order = celery.get("canonical_queue_order")
    if order is None:
        return errors
    if not isinstance(order, list):
        errors.append("ops.celery.canonical_queue_order must be a list of queue name strings")
        return errors
    profile_queues = set(_queues_declared_in_worker_profiles(ops))
    for x in order:
        qn = str(x).strip()
        if not qn:
            continue
        if profile_queues and qn not in profile_queues:
            errors.append(
                f"canonical_queue_order entry {qn!r} is not listed under ops.worker_profiles[*].queues",
            )
    return errors


def parse_broker_queue_display_names(config: Optional[dict]) -> Dict[str, str]:
    """Load ``ops.celery.broker_queue_display_names`` (broker queue key → UI label)."""
    out: Dict[str, str] = {}
    if not config or not isinstance(config, dict):
        return out
    ops = config.get("ops") or {}
    if not isinstance(ops, dict):
        return out
    celery = ops.get("celery") or {}
    if not isinstance(celery, dict):
        return out
    raw = celery.get("broker_queue_display_names")
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        ks, vs = str(k).strip(), str(v).strip()
        if ks and vs:
            out[ks] = vs
    return out


def build_broker_queue_labels(config: Optional[dict]) -> Dict[str, str]:
    """Map broker queue key → display label: ``broker_queue_display_names`` first, then worker_profiles."""
    out = parse_broker_queue_display_names(config)
    for k, v in build_broker_queue_labels_from_worker_profiles(config).items():
        if k not in out:
            out[k] = v
    return out


def build_broker_queue_labels_from_worker_profiles(config: Optional[dict]) -> Dict[str, str]:
    """Map broker queue key → profile label from ``ops.worker_profiles`` (first profile wins on conflict)."""
    out: Dict[str, str] = {}
    if not config or not isinstance(config, dict):
        return out
    ops = config.get("ops") or {}
    if not isinstance(ops, dict):
        return out
    raw = ops.get("worker_profiles") or {}
    if not isinstance(raw, dict):
        return out
    for _pk, ent in raw.items():
        if not isinstance(ent, dict):
            continue
        label = str(ent.get("label", "")).strip() or str(_pk)
        qs = ent.get("queues") or []
        if isinstance(qs, str):
            qs = [qs]
        for q in qs:
            qn = str(q).strip()
            if not qn:
                continue
            if qn in out and out[qn] != label:
                logger.warning(
                    "Duplicate broker queue %r in worker_profiles with different labels (%r vs %r); keeping first",
                    qn,
                    out[qn],
                    label,
                )
                continue
            if qn not in out:
                out[qn] = label
    return out


# Default prefork child count when YAML omits ``massive_worker_concurrency`` (Massive profiles default to ``solo``).
DEFAULT_MASSIVE_WORKER_CONCURRENCY: Final[int] = 1


def build_celery_worker_pool_argv(
    *,
    instance_profile_resolved: bool,
    profile_key: Optional[str],
    worker_profile_entry: Optional[dict],
    ops_celery: Any,
) -> List[str]:
    """Return argv fragments for ``celery worker`` pool (e.g. ``--pool=solo`` or ``--pool=prefork --concurrency=N``).

    - No ``--instance`` / unresolved profile: ``solo`` (legacy worker may mix ``stocks_ib`` with Massive queues).
    - ``stocks_ib`` profile: always ``solo`` (single IB connection per worker OS process).
    - Other profiles: ``prefork`` unless ``ops.worker_profiles.<key>.pool`` is ``solo``; concurrency from profile,
      else ``ops.celery.massive_worker_concurrency``, else :data:`DEFAULT_MASSIVE_WORKER_CONCURRENCY`.
    """
    if not instance_profile_resolved or not profile_key:
        return ["--pool=solo"]
    if profile_key == BROKER_QUEUE_STOCKS_IB:
        return ["--pool=solo"]
    entry = worker_profile_entry if isinstance(worker_profile_entry, dict) else {}
    pool = str(entry.get("pool", "") or "").strip().lower()
    if pool in ("", "prefork"):
        pass
    elif pool == "solo":
        return ["--pool=solo"]
    else:
        logger.warning(
            "ops.worker_profiles[%r].pool=%r is not supported; using prefork",
            profile_key,
            entry.get("pool"),
        )

    oc = ops_celery if isinstance(ops_celery, dict) else {}
    conc_val: Any = entry.get("concurrency")
    if conc_val is None:
        conc_val = oc.get("massive_worker_concurrency")
    if conc_val is None:
        n = DEFAULT_MASSIVE_WORKER_CONCURRENCY
    else:
        try:
            n = int(conc_val)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid massive worker concurrency %r; using %s",
                conc_val,
                DEFAULT_MASSIVE_WORKER_CONCURRENCY,
            )
            n = DEFAULT_MASSIVE_WORKER_CONCURRENCY
    n = max(1, min(n, 64))
    return ["--pool=prefork", f"--concurrency={n}"]
