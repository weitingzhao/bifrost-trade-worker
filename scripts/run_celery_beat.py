#!/usr/bin/env python3
"""Start Celery Beat (same app as workers; beat_schedule is empty after Massive removal).

Usage:
  python scripts/run_celery_beat.py
  python scripts/run_celery_beat.py /path/to/config.yaml
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
os.chdir(_PROJECT_ROOT)


def main() -> None:
    from bifrost_core.config.startup import resolve_startup_config_path
    from bifrost_worker.celery.celery_app import app

    config_path, _ = resolve_startup_config_path(str(_PROJECT_ROOT), sys.argv[1:])
    os.environ["BIFROST_CONFIG"] = config_path
    sys.stderr.write(f"[run_celery_beat] config={config_path}\n")

    # Celery Beat entry — same app as workers (beat_schedule is empty).
    app.start(argv=["beat", "--loglevel=info"])


if __name__ == "__main__":
    main()
