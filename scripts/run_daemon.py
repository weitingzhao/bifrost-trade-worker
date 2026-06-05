"""Entry point: trading daemon (GsTrading).

Usage:
    python scripts/run_daemon.py [config/config.yaml]
"""
from __future__ import annotations

import asyncio
import sys


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/config.yaml"
    from bifrost_worker.daemon.app.gs_trading import GsTrading

    engine = GsTrading(config_path=config_path)
    asyncio.run(engine.run())


if __name__ == "__main__":
    main()
