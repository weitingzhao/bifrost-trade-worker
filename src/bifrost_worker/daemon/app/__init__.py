"""Application entry: gamma scalping strategy and run_daemon."""

from bifrost_worker.daemon.app.gs_trading import GsTrading
from bifrost_worker.daemon.app.entry import run_daemon

__all__ = ["GsTrading", "run_daemon"]
