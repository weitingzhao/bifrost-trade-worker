"""SSOT for ``run_massive_job`` kind/mode combinations and broker queue routing (with tests).

Queue columns use :func:`celery_queue_for_massive_job` — same logic as Massive API enqueue.
Mode is for documentation/UI only; routing depends on ``kind`` only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from bifrost_worker.data.massive.celery_queues import (
    FEED_OPTION_CONTRACTS_KINDS,
    FEED_OPTIONS_AGGREGATE_KINDS,
    FEED_OPTIONS_TRADES_QUOTES_KINDS,
    FEED_STOCKS_AGGREGATE_KINDS,
    FEED_STOCKS_CORPORATE_ACTION_KINDS,
    FEED_STOCKS_FINANCIALS_KINDS,
    FEED_STOCKS_TICKERS_REFERENCE_UNIVERSE_KINDS,
    FEED_STOCKS_TICKERS_TYPES_KINDS,
    TICKER_REFERENCE_KINDS,
    celery_queue_for_massive_job,
)
from bifrost_worker.data.massive.run_massive_job_matrix_effects import effects_for_matrix_row, matrix_row_effects_to_api

# Celery task that executes ``job_massive_backfill`` rows (always this for matrix paths).
RUN_MASSIVE_JOB_CELERY_TASK_NAME = "src.massive.tasks.run_massive_job"

# Job ``kind`` values that Celery Beat inserts on a schedule (see ``beat_schedule_public`` + ``tasks.beat_*``).
# Value: Beat task name (same as ``@app.task(name=...)``).
_KIND_BEAT_SCHEDULED_TASK: Dict[str, str] = {
    "eod_pipeline": "src.massive.tasks.beat_eod_pipeline",
    "feed_stocks_corporate_action": "src.massive.tasks.beat_corporate_watchlist",
    "reconcile": "src.massive.tasks.beat_reconcile",
    "trim_jobs": "src.massive.tasks.beat_trim_massive_jobs",
}


def matrix_row_task_name_and_job_style(kind: str) -> Tuple[str, str]:
    """Return (celery_task_name, job_style) for API: ``scheduled`` | ``on_demand``."""
    k = (kind or "").strip().lower()
    beat = _KIND_BEAT_SCHEDULED_TASK.get(k)
    if beat:
        return beat, "scheduled"
    return RUN_MASSIVE_JOB_CELERY_TASK_NAME, "on_demand"


@dataclass(frozen=True)
class RunMassiveJobMatrixRow:
    kind: str
    mode: Optional[str]
    mode_source: str
    broker_queue_standard: str
    broker_queue_high: str

    def to_api_dict(self) -> Dict[str, Any]:
        task_name, job_style = matrix_row_task_name_and_job_style(self.kind)
        out: Dict[str, Any] = {
            "kind": self.kind,
            "mode": self.mode,
            "mode_source": self.mode_source,
            "broker_queue_standard": self.broker_queue_standard,
            "broker_queue_high": self.broker_queue_high,
            "task_name": task_name,
            "job_style": job_style,
        }
        out.update(matrix_row_effects_to_api(effects_for_matrix_row(self.kind, self.mode)))
        return out


def queue_for_row(kind: str, mode: Optional[str], *, priority_high: bool) -> str:
    """Broker queue for a matrix row; ``mode`` is ignored (same as Massive API routing)."""
    del mode
    return celery_queue_for_massive_job(kind, priority_high=priority_high)


def _qpair(kind: str) -> Tuple[str, str]:
    return (
        celery_queue_for_massive_job(kind, priority_high=False),
        celery_queue_for_massive_job(kind, priority_high=True),
    )


def _row(kind: str, mode: Optional[str], mode_source: str) -> RunMassiveJobMatrixRow:
    qs, qh = _qpair(kind)
    return RunMassiveJobMatrixRow(
        kind=kind,
        mode=mode,
        mode_source=mode_source,
        broker_queue_standard=qs,
        broker_queue_high=qh,
    )


def build_run_massive_job_matrix() -> Tuple[RunMassiveJobMatrixRow, ...]:
    """All documented kind/mode combinations implemented in ``run_massive_job``."""
    rows: List[RunMassiveJobMatrixRow] = []

    # feed_option_snapshots — payload.mode (legacy payload.snapshot_type still accepted in worker)
    for m in ("chain", "contract", "unified"):
        rows.append(_row("feed_option_snapshots", m, "payload.mode"))

    # feed_stocks_aggregate — payload.mode
    for m in ("custom_bars", "daily_market_summary", "daily_ticker_summary", "previous_day_bar"):
        rows.append(_row("feed_stocks_aggregate", m, "payload.mode"))

    # feed_options_aggregate — payload.mode
    for m in (
        "open_close",
        "option_day_pool_row_gap",
        "option_day_pool_column_fill",
        "prev",
        "option_min_pool_row_gap",
        "option_min_pool_column_fill",
        "option_snapshots_pool_contract_fill",
        "custom_bars",
    ):
        rows.append(_row("feed_options_aggregate", m, "payload.mode"))

    # oi
    rows.append(_row("oi", "watchlist_eod", "payload.mode"))

    for k in ("eod_pipeline", "report_option_max_pain", "reconcile", "trim_jobs"):
        rows.append(_row(k, None, "n/a"))

    rows.append(_row("feed_stocks_corporate_action", None, "n/a"))

    for k in FEED_STOCKS_FINANCIALS_KINDS:
        rows.append(_row(k, None, "n/a"))

    # feed_option_contracts — payload.mode
    for m in ("list", "detail", "reference_upsert", "nullable_column_backfill"):
        rows.append(_row("feed_option_contracts", m, "payload.mode"))

    # feed_options_trades_quotes — payload.mode
    for m in ("last_trade", "quotes", "trades"):
        rows.append(_row("feed_options_trades_quotes", m, "payload.mode"))

    # Ticker / stock reference — payload.mode where applicable (see tasks.py)
    overview_modes = ("all", "symbols", "missing", "stale")
    # Canonical feed_stocks_tickers_* kinds replace legacy ``ticker_reference_*`` / ``stock_reference_*``; skip aliases.
    _ticker_ref_matrix_skip = frozenset(
        {
            "ticker_reference_related",
            "stock_reference_related",
            "ticker_reference_overview",
            "stock_reference_overview",
        }
    )
    for k in TICKER_REFERENCE_KINDS:
        if k in _ticker_ref_matrix_skip:
            continue
        if k in (
            "feed_stocks_tickers_overview",
            "feed_stocks_tickers_related",
        ):
            for m in overview_modes:
                rows.append(_row(k, m, "payload.mode"))
        else:
            rows.append(_row(k, None, "n/a"))

    return tuple(rows)


RUN_MASSIVE_JOB_MATRIX: Tuple[RunMassiveJobMatrixRow, ...] = build_run_massive_job_matrix()

# Top-level ``if kind ==`` branches in run_massive_job (for drift tests).
# Top-level ``kind`` values accepted by ``run_massive_job`` (before ``normalize_ticker_ref_kind``).
RUN_MASSIVE_JOB_TOP_LEVEL_KINDS: frozenset[str] = frozenset(
    {
        "feed_option_snapshots",
        "oi",
        "eod_pipeline",
        "report_option_max_pain",
        "reconcile",
        "trim_jobs",
    }
    | FEED_STOCKS_CORPORATE_ACTION_KINDS
    | TICKER_REFERENCE_KINDS
    | FEED_STOCKS_AGGREGATE_KINDS
    | FEED_OPTIONS_AGGREGATE_KINDS
    | FEED_OPTIONS_TRADES_QUOTES_KINDS
    | FEED_OPTION_CONTRACTS_KINDS
    | FEED_STOCKS_TICKERS_REFERENCE_UNIVERSE_KINDS
    | FEED_STOCKS_TICKERS_TYPES_KINDS
    | FEED_STOCKS_FINANCIALS_KINDS
)
