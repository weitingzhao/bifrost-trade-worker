"""Celery app for bars backfill worker. Broker and result backend use Redis from config.

Usage:
  celery -A src.workers.celery_app worker -l info -Q stocks_ib --pool=solo
  celery -A src.workers.celery_app worker -l info -Q options_massive --pool=solo   # Massive (no IB; one job per instance by default)
  # Stock reference jobs use queues stocks_massive / stocks_massive_high (see src.massive.celery_queues).

Or: python scripts/systemd/run_celery.py [config_path]  # pool from profile; sets BIFROST_CELERY_QUEUES

Celery Beat (Massive schedules): python scripts/init/run_celery_beat.py

``stocks_ib``: solo pool, one IB connection (client_id). Massive profiles: solo by default (see ``build_celery_worker_pool_argv``).
Stop-poll runs in worker_init (solo) or worker_process_init (prefork) so Stop button works.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import sys
import threading
import time
from pathlib import Path
from typing import List, Optional

# Ensure src/ on path (not bifrost_worker/ — that would shadow the pip ``celery`` package).
_here = Path(__file__).resolve().parent
_bifrost_worker_pkg = _here.parent
_src_root = _bifrost_worker_pkg.parent
_repo_root = _src_root.parent
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))
if os.getcwd() != str(_repo_root):
    try:
        os.chdir(_repo_root)
    except OSError:
        pass

logger = logging.getLogger(__name__)


def _redis_url_from_config() -> str:
    """Build Celery broker URL — ``redis_queue`` when configured, else legacy db=1 on ``redis`` host."""
    explicit = (os.environ.get("CELERY_BROKER_URL") or os.environ.get("REDIS_URL") or "").strip()
    if explicit:
        return explicit
    try:
        from bifrost_core.config.startup import read_config
        from bifrost_core.core.redis_url import celery_redis_url_from_config

        config, _ = read_config()
    except Exception as e:
        logger.warning("read_config for Celery failed: %s; using default Redis URL", e)
        return "redis://127.0.0.1:6379/1"
    r = config.get("redis") or {}
    if not r.get("enabled", True) and "enabled" in r:
        return "redis://127.0.0.1:6379/1"
    return celery_redis_url_from_config(config)


broker_url = _redis_url_from_config()
result_backend = broker_url

from celery import Celery  # noqa: E402

from bifrost_worker.data.massive.beat_schedule_public import build_celery_beat_schedule  # noqa: E402

app = Celery(
    "bifrost.bars",
    broker=broker_url,
    backend=result_backend,
    include=["bifrost_worker.data.bars.tasks", "bifrost_worker.data.massive.tasks"],
)
app.conf.update(
    # One reserved message per worker child so multiple workers on the same queue share work fairly
    # (solo pool: one task at a time per instance; scale throughput by adding worker instances).
    worker_prefetch_multiplier=1,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_default_queue="stocks_ib",
    task_routes={
        "src.bars.tasks.backfill_bars": {"queue": "stocks_ib"},
        # Default route; API enqueues with explicit queue= (options: options_massive/*_high, stocks: stocks_massive*).
        "src.massive.tasks.run_massive_job": {"queue": "options_massive"},
        "src.massive.tasks.beat_eod_pipeline": {"queue": "options_massive"},
        "src.massive.tasks.beat_corporate_watchlist": {"queue": "options_massive"},
        "src.massive.tasks.beat_reconcile": {"queue": "options_massive"},
        "src.massive.tasks.beat_trim_massive_jobs": {"queue": "options_massive"},
        "src.massive.tasks.beat_refresh_expirations": {"queue": "options_massive"},
    },
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    result_expires=86400,
    beat_schedule=build_celery_beat_schedule(),
)


def get_celery_broker_connected() -> bool:
    """Try to connect to Celery broker (Redis) and ping. Used by GET /status for Celery status display."""
    try:
        import redis
        r = redis.from_url(broker_url)
        r.ping()
        return True
    except Exception:
        return False


# Inspect ping/stats must outlast solo-worker busy windows (e.g. bars between-job cooldown ~10s)
# so Dashboard/Ops do not flip to Degraded when the worker is healthy but slow to answer control.
CELERY_INSPECT_TIMEOUT_SEC = 15.0

WORKER_IB_STATUS_KEY = "bifrost:worker_ib_status"
WORKER_IB_STATUS_TTL_SEC = 90
WORKER_STOP_REQUESTED_KEY = "bifrost:worker_stop_requested"
# Per-worker Redis Stream keys: bifrost:celery_console:w:{celery_nodename} (nodename from -n, e.g. workerib-1@host).
CELERY_LOG_STREAM_MAXLEN = 50


def celery_console_stream_key(worker_id: str) -> str:
    """Redis Stream key for one Celery worker's console log (UI tail). Uses worker nodename verbatim for visibility in Redis."""
    return f"bifrost:celery_console:w:{worker_id.strip()}"

# Ops Dashboard Runtime Snapshot: fast worker list via Redis keys (no Celery control.inspect).
OPS_WORKER_PRESENCE_KEY_PREFIX = "bifrost:ops:worker_presence:"
OPS_WORKER_PRESENCE_TTL_SEC = 45
OPS_WORKER_PRESENCE_INTERVAL_SEC = 15


def _presence_queue_names_for_worker(worker: object) -> List[str]:
    """Best-effort queue names for JSON payload (may be empty)."""
    out: List[str] = []
    try:
        app = getattr(worker, "app", None)
        if app is not None:
            for q in getattr(app.conf, "task_queues", None) or []:
                name = getattr(q, "name", None) or (q.get("name") if isinstance(q, dict) else None)
                if name:
                    out.append(str(name))
        consumer = getattr(worker, "consumer", None)
        if consumer and getattr(consumer, "queues", None):
            for q in consumer.queues:
                name = getattr(q, "name", None)
                if name:
                    out.append(str(name))
    except Exception:
        pass
    return sorted(set(out))


def _bifrost_config_profile_label() -> Optional[str]:
    """Map ``BIFROST_CONFIG`` path to ``dev`` / ``prod`` for Ops UI (Redis presence)."""
    raw = (os.environ.get("BIFROST_CONFIG") or "").strip()
    if not raw:
        return None
    name = Path(raw).name.lower()
    if name == "config.prod.yaml":
        return "prod"
    if name == "config.dev.yaml":
        return "dev"
    return None


def _start_ops_worker_presence_heartbeat(worker: object) -> None:
    """Background SETEX so Ops can list workers via SCAN without control.inspect."""
    try:
        wid = getattr(worker, "hostname", None)
    except Exception:
        wid = None
    if not wid or not isinstance(wid, str):
        return

    def loop() -> None:
        import redis

        r = redis.from_url(
            broker_url,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        key = f"{OPS_WORKER_PRESENCE_KEY_PREFIX}{wid}"
        while True:
            try:
                payload = {
                    "worker_id": wid,
                    "queues": _presence_queue_names_for_worker(worker),
                    "ts": time.time(),
                    "config_profile": _bifrost_config_profile_label(),
                }
                r.setex(key, OPS_WORKER_PRESENCE_TTL_SEC, json.dumps(payload))
            except Exception as e:
                logger.debug("ops worker presence heartbeat: %s", e)
            time.sleep(OPS_WORKER_PRESENCE_INTERVAL_SEC)

    t = threading.Thread(target=loop, daemon=True, name="bifrost-ops-presence")
    t.start()


def get_worker_ib_status() -> Optional[dict]:
    """Read Worker IB connection status from Redis (written by bars worker). Returns {connected, client_id} or None."""
    try:
        import redis

        r = redis.from_url(broker_url)
        raw = r.get(WORKER_IB_STATUS_KEY)
        if not raw:
            return None
        data = json.loads(raw)
        if isinstance(data.get("connected"), bool) and data["connected"]:
            return {"connected": True, "client_id": data.get("client_id")}
        return None
    except Exception:
        return None


def get_celery_workers_ping(timeout: float = CELERY_INSPECT_TIMEOUT_SEC) -> list[str]:
    """Ping Celery workers via broker; return list of worker names that responded. Used for UI 'Running workers' list."""
    try:
        i = app.control.inspect(timeout=timeout)
        result = i.ping()
        if not result or not isinstance(result, dict):
            logger.info("get_celery_workers_ping: no workers responded (result=%s)", result)
            return []
        return sorted(result.keys())
    except Exception as e:
        logger.info("get_celery_workers_ping failed: %s", e)
        return []


class _RedisStreamLogHandler(logging.Handler):
    """Logging handler that pushes each log record to a Redis Stream for UI console tail.

    The stream is capped at a small fixed size so the latest console history is
    available after tab switches without unbounded Redis growth.
    """

    def __init__(self, redis_url: str, stream_key: str, maxlen: int = 50) -> None:
        super().__init__()
        self._redis_url = redis_url
        self._stream_key = stream_key
        self._maxlen = maxlen

    def emit(self, record: logging.LogRecord) -> None:
        try:
            import redis
            line = self.format(record)
            r = redis.from_url(self._redis_url)
            r.xadd(
                self._stream_key,
                {"line": line},
                maxlen=self._maxlen,
                approximate=True,
            )
        except Exception:
            pass


def _start_stop_polling() -> None:
    """Start a daemon thread in this worker process that polls Redis for stop request; exits process when set.
    Must run in the worker process (worker_process_init), not the main process (worker_ready), so that with
    --concurrency=1 the process that has the IB connection is the one that polls and calls disconnect + exit.
    """
    import threading
    import time

    def poll() -> None:
        try:
            import redis
            interval_sec = 2
            logger.info("Celery worker stop-poll thread started (checks Redis every %ds)", interval_sec)
            while True:
                logger.debug("Stop-poll: next check in %ds", interval_sec)
                time.sleep(interval_sec)
                try:
                    with redis.from_url(broker_url) as r:
                        if not r.get(WORKER_STOP_REQUESTED_KEY):
                            continue
                        try:
                            r.delete(WORKER_STOP_REQUESTED_KEY)
                        except Exception:
                            pass
                    # connection closed; disconnect IB and exit
                    try:
                        from bifrost_worker.data.bars.tasks import disconnect_worker_ib_sync
                        disconnect_worker_ib_sync(timeout=5.0)
                    except Exception as e:
                        logger.warning("Worker stop: disconnect_worker_ib_sync failed: %s", e)
                    logger.info("Celery worker stop requested via API; exiting process.")
                    os._exit(0)
                except Exception as e:
                    logger.debug("Worker stop-poll Redis check: %s", e)
        except Exception as e:
            logger.warning("Worker stop-poll thread error: %s", e)

    t = threading.Thread(target=poll, daemon=True, name="celery-worker-stop-poll")
    t.start()


from celery.signals import worker_ready, worker_process_init, worker_init  # noqa: E402


def _resolve_celery_worker_id(sender: object | None) -> str:
    """Celery nodename (e.g. workermassive-8@host.local); must match Dashboard / inspect worker_id.

    ``worker_init`` / ``worker_process_init`` often run before ``Worker.hostname`` is set (solo / sender=None),
    so ``scripts/systemd/run_celery.py`` sets ``BIFROST_CELERY_NODENAME`` to match ``-n`` when using ``--instance``.
    """
    env_id = (os.environ.get("BIFROST_CELERY_NODENAME") or "").strip()
    if env_id:
        return env_id
    if sender is not None:
        wid = getattr(sender, "hostname", None)
        if isinstance(wid, str) and wid.strip():
            return wid.strip()
    host = socket.gethostname()
    logger.warning(
        "Celery worker hostname unavailable; using fallback worker id unknown@%s",
        host,
    )
    return f"unknown@{host}"


def _attach_redis_stream_log_handler(sender: object | None = None) -> None:
    """Attach Redis stream log handler to this worker process (one stream per worker nodename)."""
    root = logging.getLogger()
    for h in root.handlers[:]:
        if isinstance(h, _RedisStreamLogHandler):
            return
    worker_id = _resolve_celery_worker_id(sender)
    stream_key = celery_console_stream_key(worker_id)
    handler = _RedisStreamLogHandler(
        broker_url,
        stream_key,
        maxlen=CELERY_LOG_STREAM_MAXLEN,
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    root.addHandler(handler)


@worker_init.connect
def _on_worker_init(sender=None, **kwargs: object) -> None:
    """Solo pool: start stop-poll (single process, no worker_process_init). Redis console attaches in worker_ready."""
    # Solo pool: single process, tasks run in this process. Start stop-poll here so Stop button works.
    if sender is not None and getattr(sender, "pool", None) is not None:
        pool_module = getattr(type(sender.pool), "__module__", "") or ""
        if "solo" in pool_module:
            _start_stop_polling()


@worker_process_init.connect
def _on_worker_process_init(sender=None, **kwargs: object) -> None:
    # Stop polling must run in worker process (the one with IB connection when concurrency=1), not main process.
    _start_stop_polling()


@worker_ready.connect
def _on_worker_ready(sender, **kwargs) -> None:
    # Hostname/nodename is reliable here; worker_init often runs too early (hostname still unset → unknown@).
    _attach_redis_stream_log_handler(sender)
    if sender is not None:
        _start_ops_worker_presence_heartbeat(sender)
