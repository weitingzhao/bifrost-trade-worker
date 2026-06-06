"""Single source of truth for Celery Beat schedules (Massive-related tasks).

Used by ``src.workers.celery_app`` and ``GET /research/massive/celery-beat-schedule``.
"""

from __future__ import annotations

from typing import Any, Dict, List

# Each entry: name, Celery task path, human label, note (for Ops /capabilities), crontab kwargs (UTC).
MASSIVE_BEAT_SCHEDULE_SPEC: List[Dict[str, Any]] = [
    {
        "name": "massive-eod-pipeline",
        "task": "src.massive.tasks.beat_eod_pipeline",
        "label": "EOD pipeline (OI + Max Pain)",
        "note": "Inserts eod_pipeline job: watchlist EOD OI + report_option_max_pain for the trade date.",
        "crontab_kwargs": {"hour": 22, "minute": 0},
    },
    {
        "name": "massive-corporate-watchlist",
        "task": "src.massive.tasks.beat_corporate_watchlist",
        "label": "Corporate actions (watchlist)",
        "note": "Inserts feed_stocks_corporate_action job with all watchlist optionable STK symbols.",
        "crontab_kwargs": {"hour": 23, "minute": 0},
    },
    {
        "name": "massive-reconcile",
        "task": "src.massive.tasks.beat_reconcile",
        "label": "Reconcile (watchlist vs DB OI)",
        "note": "Inserts reconcile job: watchlist vs DB open-interest counts.",
        "crontab_kwargs": {"hour": 22, "minute": 45},
    },
    {
        "name": "massive-trim-jobs",
        "task": "src.massive.tasks.beat_trim_massive_jobs",
        "label": "Trim Massive job table",
        "note": "Inserts trim_jobs: cap job_massive_backfill history (newest 500 rows).",
        "crontab_kwargs": {"hour": 2, "minute": 15},
    },
    {
        "name": "massive-refresh-expirations",
        "task": "src.massive.tasks.beat_refresh_expirations",
        "label": "Refresh option expirations",
        "note": "Runs expiration cache + option_contracts refresh in-process; not a run_massive_job enqueue.",
        "crontab_kwargs": {"hour": "*/6", "minute": 20},
    },
    {
        "name": "massive-stock-day-eod",
        "task": "src.massive.tasks.beat_stock_day_eod",
        "label": "Stock day EOD sync (daily_smart)",
        "note": "After market close, enqueues feed_stocks_aggregate daily_smart for all watchlist STK symbols. Skips non-trading days. UTC 21:30 = 5:30pm EDT / 4:30pm EST.",
        "crontab_kwargs": {"hour": 21, "minute": 30},
    },
    {
        "name": "massive-sepa-universe-grouped-daily",
        "task": "src.massive.tasks.beat_sepa_universe_grouped_daily",
        "label": "SEPA universe daily bars (Grouped Daily, full market)",
        "note": "After market close, enqueues feed_stocks_aggregate daily_market_summary for today. One API call covers all 5,000+ US stocks simultaneously. Skips non-trading days. UTC 22:00 = 6:00pm EDT / 5:00pm EST.",
        "crontab_kwargs": {"hour": 22, "minute": 0},
    },
]


def beat_tasks_payload_for_capabilities() -> List[Dict[str, str]]:
    """Rows for GET /ops/celery/capabilities ``beat_tasks`` (task path + note)."""
    out: List[Dict[str, str]] = []
    for spec in MASSIVE_BEAT_SCHEDULE_SPEC:
        out.append(
            {
                "name": str(spec["task"]),
                "note": str(spec.get("note", "")),
            }
        )
    return out


def build_celery_beat_schedule() -> Dict[str, Any]:
    """Return ``beat_schedule`` dict for ``app.conf.update(beat_schedule=...)``."""
    from celery.schedules import crontab

    out: Dict[str, Any] = {}
    for spec in MASSIVE_BEAT_SCHEDULE_SPEC:
        name = str(spec["name"])
        kw = dict(spec["crontab_kwargs"])
        out[name] = {
            "task": str(spec["task"]),
            "schedule": crontab(**kw),
        }
    return out


def public_celery_beat_schedule_response() -> Dict[str, Any]:
    """JSON-serializable payload for Research API (no Celery runtime required)."""
    entries = []
    for spec in MASSIVE_BEAT_SCHEDULE_SPEC:
        entries.append(
            {
                "name": spec["name"],
                "task": spec["task"],
                "label": spec["label"],
                "crontab": dict(spec["crontab_kwargs"]),
            }
        )
    return {
        "ok": True,
        "timezone": "UTC",
        "entries": entries,
    }
