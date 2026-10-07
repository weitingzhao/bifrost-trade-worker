"""Daemon logging and liveness (TD-215, TD-216).

Ratchets:
- the daemon entry configures the root logger (INFO by default, a stream handler) before anything
  else runs; before worker 0.2.7 nothing did and every INFO line was dropped;
- no write failure in the worker is logged at DEBUG;
- raw_broker writes and heartbeat passes are recorded as timestamps and served on /metrics, and
  /health fails only for a leader whose heartbeat stopped.
"""

from __future__ import annotations

import ast
import logging
import sys
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bifrost_worker.daemon.app import account_push, entry, observability

_SRC = Path(__file__).resolve().parents[1] / "src" / "bifrost_worker"


@pytest.fixture
def restore_root_logger():
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield root
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in handlers:
        root.addHandler(h)
    root.setLevel(level)


def _stdout_handlers(root):
    return [
        h
        for h in root.handlers
        if isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
    ]


def test_configure_logging_defaults_to_info_on_stdout(restore_root_logger, monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    assert observability.configure_logging() == logging.INFO
    root = restore_root_logger
    assert root.level == logging.INFO
    assert len(_stdout_handlers(root)) == 1
    assert logging.getLogger("bifrost_core.portfolio.ib_edge").isEnabledFor(logging.INFO)


def test_log_level_env_and_unknown_names(restore_root_logger, monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "debug")
    assert observability.configure_logging() == logging.DEBUG
    monkeypatch.setenv("LOG_LEVEL", "chatty")
    assert observability.configure_logging() == logging.INFO
    assert len(_stdout_handlers(restore_root_logger)) == 1


def test_run_daemon_configures_logging_first(restore_root_logger, monkeypatch):
    """The entry's first act is logging setup, so even a config error is logged at INFO+."""
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    order = []
    monkeypatch.setattr(
        entry.observability, "start_http_server", lambda *a, **k: order.append("http")
    )

    def boom(path):
        order.append("config")
        raise RuntimeError("stop here")

    monkeypatch.setattr(entry, "read_config", boom)
    with pytest.raises(RuntimeError, match="stop here"):
        entry.run_daemon(None)
    assert order == ["http", "config"]
    assert restore_root_logger.level == logging.INFO
    assert _stdout_handlers(restore_root_logger)


def _debug_write_failures() -> list[str]:
    hits = []
    for path in sorted(_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for handler in ast.walk(tree):
            if not isinstance(handler, ast.ExceptHandler):
                continue
            for node in ast.walk(handler):
                if not (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "debug"
                ):
                    continue
                texts = [
                    a.value for a in node.args if isinstance(a, ast.Constant) and isinstance(a.value, str)
                ]
                if any("write" in t.lower() or "sync failed" in t.lower() for t in texts):
                    hits.append(f"{path.relative_to(_SRC.parent)}:{node.lineno}")
    return hits


def test_no_write_failure_is_logged_at_debug():
    assert _debug_write_failures() == []


# --- liveness -------------------------------------------------------------------------------


def test_standby_is_live_and_exports_no_timestamps():
    h = observability.DaemonHealth()
    assert h.live(now=1_000.0) == (True, "standby")
    text = h.render_metrics()
    assert "bifrost_daemon_leader 0.0" in text
    assert "\nbifrost_daemon_heartbeat_timestamp_seconds " not in text
    assert "\nbifrost_daemon_raw_broker_last_write_timestamp_seconds " not in text
    # BifrostAPIWithoutHttpMetrics needs the series before the first request.
    assert 'http_requests_total{handler="/health",method="GET",status="2xx"} 0.0' in text


def test_leader_liveness_follows_the_heartbeat():
    h = observability.DaemonHealth()
    limit = observability.HEARTBEAT_MAX_AGE_SEC
    h.mark_leading(now=1_000.0)
    assert h.live(now=1_000.0 + limit - 1)[0] is True
    assert h.live(now=1_000.0 + limit + 1)[0] is False  # bootstrap never reached the loop
    h.mark_heartbeat(now=1_500.0)
    assert h.live(now=1_500.0 + limit - 1)[0] is True
    ok, reason = h.live(now=1_500.0 + limit + 1)
    assert ok is False and "no heartbeat" in reason


def test_metrics_carry_timestamps_and_counters():
    h = observability.DaemonHealth()
    h.mark_leading(now=10.0)
    h.mark_heartbeat(now=20.0)
    h.mark_raw_broker_write(2, now=30.0)
    h.mark_raw_broker_failure()
    text = h.render_metrics()
    assert "bifrost_daemon_leader 1.0" in text
    assert "bifrost_daemon_heartbeat_timestamp_seconds 20.0" in text
    assert "bifrost_daemon_raw_broker_last_write_timestamp_seconds 30.0" in text
    assert "bifrost_daemon_raw_broker_accounts_written_total 2.0" in text
    assert "bifrost_daemon_raw_broker_write_failures_total 1.0" in text


def test_http_server_serves_metrics_and_health():
    h = observability.DaemonHealth()
    server = observability.start_http_server(port=0, health=h, host="127.0.0.1")
    assert server is not None
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        body = urllib.request.urlopen(base + "/metrics", timeout=5).read().decode()
        assert "bifrost_daemon_leader 0.0" in body
        assert urllib.request.urlopen(base + "/health", timeout=5).status == 200
        h.mark_leading(now=0.0)  # leading since the epoch, never a heartbeat
        with pytest.raises(urllib.error.HTTPError) as err:
            urllib.request.urlopen(base + "/health", timeout=5)
        assert err.value.code == 503
        body = urllib.request.urlopen(base + "/metrics", timeout=5).read().decode()
        assert 'http_requests_total{handler="/health",method="GET",status="2xx"} 1.0' in body
        assert 'http_requests_total{handler="/health",method="GET",status="5xx"} 1.0' in body
        assert 'handler="/metrics"' not in body
    finally:
        server.shutdown()
        server.server_close()


def test_port_zero_from_env_disables_the_server(monkeypatch):
    monkeypatch.setenv(observability.METRICS_PORT_ENV, "0")
    assert observability.start_http_server() is None


# --- raw_broker writes feed the liveness record ----------------------------------------------


def _acc(aid, nl="100"):
    return {"account_id": aid, "summary": {"NetLiquidation": nl}, "positions": []}


def test_account_writes_and_failures_are_recorded(monkeypatch):
    health = observability.DaemonHealth()
    monkeypatch.setattr(account_push._observability, "HEALTH", health)
    monkeypatch.setattr("bifrost_core.core.daemon_flags.daemon_broker_writes_off", lambda: False)
    calls = []
    monkeypatch.setattr(
        "bifrost_core.persistence.postgres.accounts_sync.sync_accounts_snapshot_to_tables",
        lambda conn, accounts: calls.append(len(accounts)),
    )
    w = account_push.AccountTablesWriter({})
    conn = MagicMock()
    conn.closed = 0
    w._conn = conn
    assert w.write([_acc("U1"), _acc("U2")]) == 2
    assert health.raw_broker_write_ts is not None
    assert health.raw_broker_accounts_written == 2

    def fail(conn, accounts):
        raise RuntimeError("server closed the connection")

    monkeypatch.setattr(
        "bifrost_core.persistence.postgres.accounts_sync.sync_accounts_snapshot_to_tables", fail
    )
    before = health.raw_broker_write_ts
    assert w.write([_acc("U1", nl="101")]) == 0
    assert health.raw_broker_write_failures == 1
    assert health.raw_broker_write_ts == before


def test_write_summary_is_info_and_rate_limited(monkeypatch, caplog):
    monkeypatch.setattr(account_push._observability, "HEALTH", observability.DaemonHealth())
    monkeypatch.setattr("bifrost_core.core.daemon_flags.daemon_broker_writes_off", lambda: False)
    monkeypatch.setattr(
        "bifrost_core.persistence.postgres.accounts_sync.sync_accounts_snapshot_to_tables",
        lambda conn, accounts: None,
    )
    w = account_push.AccountTablesWriter({})
    conn = MagicMock()
    conn.closed = 0
    w._conn = conn
    with caplog.at_level(logging.DEBUG, logger=account_push.__name__):
        w.write([_acc("U1", nl="1")])
        w._summary_since -= account_push.WRITE_SUMMARY_INTERVAL_SEC
        w.write([_acc("U1", nl="2")])
        w.write([_acc("U1", nl="3")])
    summaries = [r for r in caplog.records if "raw_broker:" in r.getMessage()]
    assert [r.levelno for r in summaries] == [logging.INFO]
    assert "2 write(s)" in summaries[0].getMessage()
