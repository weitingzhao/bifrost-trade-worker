"""Daemon logging, liveness and the /metrics page (TD-215, TD-216).

Logging: nothing configured the root logger before worker 0.2.7, so Python's last-resort handler
printed WARNING and above only and every INFO line ([ib_edge] snapshot applied, account_push,
control commands, state transitions) was dropped. :func:`configure_logging` runs first thing in
``run_daemon``: level from ``LOG_LEVEL`` (default INFO), one line per record on stdout.

Liveness: the leader is the only writer of raw_broker.account / raw_broker.positions, and its
readinessProbe only checks that Postgres and Redis answer. :data:`HEALTH` records *timestamps*
(when this pod started leading, the last heartbeat-loop pass, the last successful raw_broker
write); readers compute the age, so a stuck writer shows up as a growing age instead of a frozen
"ok". A small stdlib HTTP server (no new dependency) serves them:

- ``GET /metrics`` — Prometheus text format, scraped by the ``bifrost-trade-daemon`` PodMonitor
  (bifrost-trade-infra ``k8s/monitoring/bifrost-trade-daemon.yaml``); the alert rules
  ``BifrostTradeDaemon*`` read it.
- ``GET /health`` — the livenessProbe: 503 when this pod leads and the heartbeat loop has not
  passed for :data:`HEARTBEAT_MAX_AGE_SEC`. A standby pod (no lease) is always live.

Requests other than ``/metrics`` are counted in ``http_requests_total{handler,method,status}``
(status grouped as ``2xx`` / ``5xx``, as the Trade APIs do): every target scraped in the
``bifrost-*`` namespaces must export it (alert ``BifrostAPIWithoutHttpMetrics``, TD-161), and a
503 from ``/health`` then also shows in the 5xx rate. The probe path is ``/health`` because the
APIs' ``/health`` is counted but never timed (core 0.55.0), so ``check_http_metrics_coverage.py
--live`` expects no latency histogram for a target that serves nothing else; this one has none.

Observability only: nothing here reads or writes an order path or the operator stream (D10).
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOG_LEVEL_ENV = "LOG_LEVEL"

METRICS_PORT_ENV = "BIFROST_DAEMON_METRICS_PORT"
DEFAULT_METRICS_PORT = 9108

#: The heartbeat loop sleeps at most 120 s per pass (``effective_heartbeat_interval`` clamps to
#: [5, 120]); five passes' worth without one means the loop is stuck, not slow.
HEARTBEAT_MAX_AGE_SEC = 600.0


def configure_logging(level: Optional[str] = None) -> int:
    """Configure the root logger for the daemon process; returns the level applied.

    ``level`` (or ``$LOG_LEVEL``) is a level name; unknown names fall back to INFO.
    """
    name = (level or os.environ.get(LOG_LEVEL_ENV) or "INFO").strip().upper()
    lvl = logging.getLevelName(name)
    if not isinstance(lvl, int):
        lvl = logging.INFO
    logging.basicConfig(level=lvl, format=LOG_FORMAT, stream=sys.stdout, force=True)
    return lvl


class DaemonHealth:
    """Timestamps and counters for liveness; written by the daemon, read by the HTTP thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.leader_since: Optional[float] = None
        self.heartbeat_ts: Optional[float] = None
        self.raw_broker_write_ts: Optional[float] = None
        self.raw_broker_accounts_written = 0
        self.raw_broker_write_failures = 0
        # Present from the start so the series exists before the first probe.
        self.http_requests: dict[tuple[str, str, str], int] = {("/health", "GET", "2xx"): 0}

    def mark_leading(self, now: Optional[float] = None) -> None:
        with self._lock:
            self.leader_since = now if now is not None else time.time()

    def mark_heartbeat(self, now: Optional[float] = None) -> None:
        with self._lock:
            self.heartbeat_ts = now if now is not None else time.time()

    def mark_raw_broker_write(self, accounts: int, now: Optional[float] = None) -> None:
        with self._lock:
            self.raw_broker_write_ts = now if now is not None else time.time()
            self.raw_broker_accounts_written += int(accounts)

    def mark_raw_broker_failure(self) -> None:
        with self._lock:
            self.raw_broker_write_failures += 1

    def count_request(self, handler: str, method: str, code: int) -> None:
        key = (handler, method, f"{code // 100}xx")
        with self._lock:
            self.http_requests[key] = self.http_requests.get(key, 0) + 1

    def live(self, now: Optional[float] = None) -> tuple[bool, str]:
        """(live, reason). Leading with no heartbeat pass for too long is not live."""
        now = now if now is not None else time.time()
        with self._lock:
            leader_since, hb = self.leader_since, self.heartbeat_ts
        if leader_since is None:
            return True, "standby"
        last = max(leader_since, hb or 0.0)
        age = now - last
        if age > HEARTBEAT_MAX_AGE_SEC:
            what = "heartbeat" if hb is not None else "first heartbeat since leading"
            return False, f"no {what} for {age:.0f}s (limit {HEARTBEAT_MAX_AGE_SEC:.0f}s)"
        return True, f"heartbeat {age:.0f}s ago"

    def render_metrics(self) -> str:
        """Prometheus text exposition. Timestamps are omitted until first set."""
        with self._lock:
            leader_since = self.leader_since
            hb = self.heartbeat_ts
            wts = self.raw_broker_write_ts
            written = self.raw_broker_accounts_written
            failed = self.raw_broker_write_failures
            requests = sorted(self.http_requests.items())
        out: list[str] = []

        def metric(name: str, kind: str, help_: str, value: Optional[float]) -> None:
            out.append(f"# HELP {name} {help_}")
            out.append(f"# TYPE {name} {kind}")
            if value is not None:
                out.append(f"{name} {value!r}")

        metric(
            "bifrost_daemon_leader", "gauge",
            "1 while this pod holds the daemon lease and runs the FSM.",
            1.0 if leader_since is not None else 0.0,
        )
        metric(
            "bifrost_daemon_heartbeat_timestamp_seconds", "gauge",
            "Unix time of the last heartbeat-loop pass (leader only).", hb,
        )
        metric(
            "bifrost_daemon_raw_broker_last_write_timestamp_seconds", "gauge",
            "Unix time of the last successful raw_broker.account/positions write (leader only).",
            wts,
        )
        metric(
            "bifrost_daemon_raw_broker_accounts_written_total", "counter",
            "Accounts written to raw_broker.account/positions by this process.", float(written),
        )
        metric(
            "bifrost_daemon_raw_broker_write_failures_total", "counter",
            "Failed raw_broker.account/positions writes by this process.", float(failed),
        )
        out.append("# HELP http_requests_total Requests served by the daemon's HTTP endpoint.")
        out.append("# TYPE http_requests_total counter")
        for (handler, method, status), n in requests:
            out.append(
                f'http_requests_total{{handler="{handler}",method="{method}",status="{status}"}} '
                f"{float(n)!r}"
            )
        return "\n".join(out) + "\n"


#: Process-wide health record (one daemon per process).
HEALTH = DaemonHealth()


class _Handler(BaseHTTPRequestHandler):
    health: DaemonHealth = HEALTH

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        path = self.path.split("?", 1)[0]
        if path == "/metrics":
            self._send(200, self.health.render_metrics(), "text/plain; version=0.0.4")
            return
        if path == "/health":
            ok, reason = self.health.live()
            code, body = (200 if ok else 503), reason + "\n"
        else:
            path, code, body = "none", 404, "not found\n"
        self.health.count_request(path, "GET", code)
        self._send(code, body, "text/plain")

    def _send(self, code: int, body: str, ctype: str) -> None:
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return  # probes and scrapes every few seconds; not worth a log line each


def start_http_server(
    port: Optional[int] = None, health: DaemonHealth = HEALTH, host: str = "0.0.0.0"
) -> Optional[ThreadingHTTPServer]:
    """Serve /metrics and /health on a daemon thread. Port 0 from the env disables it."""
    if port is None:
        raw = os.environ.get(METRICS_PORT_ENV, str(DEFAULT_METRICS_PORT)).strip()
        try:
            port = int(raw)
        except ValueError:
            logger.warning("[observability] %s=%r is not a port; metrics server off", METRICS_PORT_ENV, raw)
            return None
        if port == 0:
            return None
    handler = type("DaemonHealthHandler", (_Handler,), {"health": health})
    try:
        server = ThreadingHTTPServer((host, port), handler)
    except OSError as e:
        logger.warning("[observability] metrics server on :%s failed: %s", port, e)
        return None
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="daemon-metrics", daemon=True).start()
    logger.info("[observability] /metrics and /health on :%s", server.server_address[1])
    return server
