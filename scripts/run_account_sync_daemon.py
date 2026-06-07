"""Entry point: Account Sync Daemon — consume ib:account:stream:v1 → diff → PostgreSQL.

Usage:
    python scripts/run_account_sync_daemon.py
    python scripts/run_account_sync_daemon.py --config config/config.prod.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from bifrost_core.config.startup import read_config
from bifrost_core.core.logging_redis_stream import RedisStreamLogHandler
from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url
from bifrost_worker.daemon.account_sync.redis_keys import ACCOUNT_SYNC_LOG_STREAM_KEY

_LOG_STREAM_MAXLEN = 2000

logger = logging.getLogger("account_sync_daemon")


def _console_log_redis_url(config: dict) -> str:
    return format_redis_url(effective_redis_dict(config, default_db=0))


def _setup_logging(level: int, config: dict) -> None:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s  %(message)s"))
    redis_handler = RedisStreamLogHandler(
        _console_log_redis_url(config),
        ACCOUNT_SYNC_LOG_STREAM_KEY,
        maxlen=_LOG_STREAM_MAXLEN,
    )
    redis_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(h)
    root.addHandler(redis_handler)
    root.setLevel(level)


def main() -> None:
    parser = argparse.ArgumentParser(description="Account Sync Daemon (Redis stream → PostgreSQL)")
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    config, _resolved_path = read_config(args.config)
    _setup_logging(getattr(logging, args.log_level), config)

    from bifrost_worker.daemon.account_sync.app import AccountSyncDaemon

    app = AccountSyncDaemon(config)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
