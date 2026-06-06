"""Entry point: trading daemon (GsTrading).

Usage:
    python scripts/run_daemon.py [config/config.yaml]
"""
from __future__ import annotations

import sys


def main() -> None:
    from bifrost_worker.daemon.app.entry import run_daemon

    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    run_daemon(config_path)


if __name__ == "__main__":
    main()
