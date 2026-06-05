#!/usr/bin/env python3
"""Copy engine worker/daemon tests into bifrost-trade-worker/tests."""

from __future__ import annotations

import re
from pathlib import Path

ENGINE = Path(__file__).resolve().parents[2] / "bifrost-trader-engine" / "tests"
DEST = Path(__file__).resolve().parents[1] / "tests"

TESTS = [
    "test_daemon_fsm_coverage.py",
    "test_gs_trading_fsm.py",
    "test_trading_fsm.py",
    "test_hedge_fsm.py",
    "test_guards.py",
    "test_risk_guard.py",
    "test_gamma_scalper.py",
    "test_hedge_gate.py",
    "test_hedge_flow.py",
    "test_state.py",
    "test_state_classifier.py",
    "test_black_scholes.py",
    "test_celery_capabilities.py",
    "test_celery_supported_tasks.py",
    "test_celery_worker_pool_argv.py",
    "test_massive_client.py",
    "test_massive_celery_queues.py",
    "test_massive_job_goal.py",
]

REPLS = [
    (r"\bfrom src\.daemon\b", "from bifrost_worker.daemon"),
    (r"\bfrom src\.workers\b", "from bifrost_worker.celery"),
    (r"\bfrom src\.massive\b", "from bifrost_worker.data.massive"),
    (r"\bfrom src\.vendor\.massive\b", "from bifrost_worker.data.massive.vendor"),
    (r"\bfrom src\.config\b", "from bifrost_core.config"),
    (r"\bfrom src\.core\b", "from bifrost_core.core"),
]


def main() -> None:
    DEST.mkdir(exist_ok=True)
    for name in TESTS:
        src = ENGINE / name
        if not src.is_file():
            continue
        text = src.read_text(encoding="utf-8")
        for pat, rep in REPLS:
            text = re.sub(pat, rep, text)
        (DEST / name).write_text(text, encoding="utf-8")
        print("ok", name)


if __name__ == "__main__":
    main()
