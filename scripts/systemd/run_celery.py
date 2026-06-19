#!/usr/bin/env python3
"""Start Celery worker for bars backfill and Massive jobs (Ops subprocess / systemd).

Usage:
  python scripts/systemd/run_celery.py
  python scripts/systemd/run_celery.py --instance stocks_ib-1
"""

from __future__ import annotations

import os
import re
import signal
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
os.chdir(_PROJECT_ROOT)


def _strip_instance_from_argv(argv: list[str]) -> tuple[str | None, list[str]]:
    out: list[str] = []
    instance: str | None = None
    i = 0
    while i < len(argv):
        if argv[i] == "--instance" and i + 1 < len(argv):
            instance = argv[i + 1]
            i += 2
            continue
        out.append(argv[i])
        i += 1
    return instance, out


def _kill_pids_from_pgrep(cmd: list[str]) -> None:
    import subprocess

    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if out.returncode != 0 or not out.stdout.strip():
            return
        pids = [x.strip() for x in out.stdout.strip().splitlines() if x.strip().isdigit()]
        my_pid = str(os.getpid())
        killed = False
        for pid in pids:
            if pid == my_pid:
                continue
            try:
                os.kill(int(pid), signal.SIGTERM)
                sys.stderr.write(f"[run_celery] Sent SIGTERM to existing worker PID {pid}\n")
                killed = True
            except (ProcessLookupError, ValueError):
                pass
        if killed:
            time.sleep(1)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass


def _kill_existing_celery_workers(instance: str | None) -> None:
    try:
        if instance is not None:
            safe = instance.replace("\\", "\\\\").replace(".", "\\.")
            _kill_pids_from_pgrep(
                ["pgrep", "-f", f"python.*run_celery\\.py.*--instance {safe}"]
            )
            return
        _kill_pids_from_pgrep(["pgrep", "-f", "python.*run_celery\\.py"])
        _kill_pids_from_pgrep(["pgrep", "-f", "celery.*worker.*stocks_ib"])
    except Exception as e:
        sys.stderr.write(f"[run_celery] Warning: could not kill existing workers: {e}\n")


_INSTANCE_PROFILE_RE = re.compile(r"^(?P<profile>[a-zA-Z0-9_]+)-(?P<seq>\d+)$")


def _parse_instance_profile(instance_id: str) -> tuple[str | None, str | None]:
    m = _INSTANCE_PROFILE_RE.match(instance_id)
    if m:
        return m.group("profile"), m.group("seq")
    return None, None


def _resolve_queues_for_instance(instance: str | None, config_path: str) -> str:
    from bifrost_core.config.startup import read_config
    from bifrost_worker.celery.celery_queue_names import (
        CANONICAL_BROKER_QUEUE_NAMES,
        load_canonical_broker_queue_names,
        ops_celery_config_validation_errors,
    )

    if instance is None:
        try:
            cfg, _ = read_config(config_path)
        except Exception as exc:
            sys.stderr.write(
                f"[run_celery] WARNING: cannot read {config_path}: {exc}; "
                f"using built-in default queue order.\n"
            )
            return ",".join(CANONICAL_BROKER_QUEUE_NAMES)

        errs = ops_celery_config_validation_errors(cfg if isinstance(cfg, dict) else None)
        if errs:
            for msg in errs:
                sys.stderr.write(f"[run_celery] ERROR: ops celery config: {msg}\n")
            sys.exit(1)
        return ",".join(load_canonical_broker_queue_names(cfg if isinstance(cfg, dict) else None))

    profile_key, _seq = _parse_instance_profile(instance)
    if profile_key is None:
        sys.stderr.write(
            f"[run_celery] WARNING: instance {instance!r} does not match "
            f"<profile>-<seq> pattern; using default queues.\n"
        )
        try:
            cfg, _ = read_config(config_path)
            return ",".join(load_canonical_broker_queue_names(cfg if isinstance(cfg, dict) else None))
        except Exception:
            return ",".join(CANONICAL_BROKER_QUEUE_NAMES)

    try:
        cfg, _ = read_config(config_path)
    except Exception as exc:
        sys.stderr.write(
            f"[run_celery] WARNING: cannot read {config_path}: {exc}; "
            f"using built-in default queue order.\n"
        )
        return ",".join(CANONICAL_BROKER_QUEUE_NAMES)

    profiles = (cfg.get("ops") or {}).get("worker_profiles") or {}
    entry = profiles.get(profile_key)
    if entry is None or not isinstance(entry, dict):
        sys.stderr.write(
            f"[run_celery] ERROR: profile {profile_key!r} not found in "
            f"ops.worker_profiles ({config_path}). Exiting.\n"
        )
        sys.exit(1)

    queues = entry.get("queues") or []
    if isinstance(queues, str):
        queues = [queues]
    queues = [str(q).strip() for q in queues if str(q).strip()]
    if not queues:
        sys.stderr.write(
            f"[run_celery] ERROR: profile {profile_key!r} has empty queues. Exiting.\n"
        )
        sys.exit(1)

    return ",".join(queues)


if __name__ == "__main__":
    import socket

    from bifrost_core.config.startup import read_config, resolve_startup_config_path
    from bifrost_worker.celery.celery_app import app
    from bifrost_worker.celery.celery_queue_names import build_celery_worker_pool_argv

    argv_raw = sys.argv[1:]
    instance, argv_for_config = _strip_instance_from_argv(argv_raw)
    config_path, _ = resolve_startup_config_path(str(_PROJECT_ROOT), argv_for_config)
    os.environ["BIFROST_CONFIG"] = config_path
    _kill_existing_celery_workers(instance)

    queue_str = _resolve_queues_for_instance(instance, config_path)
    sys.stderr.write(f"[run_celery] queues={queue_str} instance={instance}\n")

    cfg, _cfg_path = read_config(config_path)
    os.environ["BIFROST_CELERY_QUEUES"] = queue_str

    profile_key, _seq = _parse_instance_profile(instance) if instance else (None, None)
    instance_ok = bool(instance and profile_key)
    profile_entry = None
    if instance_ok:
        profiles = (cfg.get("ops") or {}).get("worker_profiles") or {}
        if isinstance(profiles, dict):
            profile_entry = profiles.get(profile_key or "")
    pool_argv = build_celery_worker_pool_argv(
        instance_profile_resolved=instance_ok,
        profile_key=profile_key,
        worker_profile_entry=profile_entry if isinstance(profile_entry, dict) else None,
        ops_celery=(cfg.get("ops") or {}).get("celery") or {},
    )
    sys.stderr.write(f"[run_celery] pool_argv={pool_argv}\n")

    worker_argv = ["worker", "-l", "info", "-Q", queue_str, *pool_argv]
    if instance is not None:
        host = socket.gethostname()
        nodename = f"worker{instance}@{host}"
        worker_argv.extend(["-n", nodename])
        os.environ["BIFROST_CELERY_NODENAME"] = nodename

    app.worker_main(argv=worker_argv)
