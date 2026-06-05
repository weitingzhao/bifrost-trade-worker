"""Celery worker entry (mirrors engine scripts/systemd/run_celery.py)."""

from __future__ import annotations

import sys


def main() -> None:
    from bifrost_worker.celery.celery_app import app

    argv = ["worker", "--loglevel=info"]
    if len(sys.argv) > 1:
        argv = sys.argv[1:]
    app.worker_main(argv=argv)


if __name__ == "__main__":
    main()
