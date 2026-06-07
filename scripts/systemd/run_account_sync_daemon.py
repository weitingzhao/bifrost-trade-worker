"""systemd / Ops subprocess entry — delegates to scripts/run_account_sync_daemon.py."""

from __future__ import annotations

import runpy
from pathlib import Path

_TARGET = Path(__file__).resolve().parent.parent / "run_account_sync_daemon.py"

if __name__ == "__main__":
    runpy.run_path(str(_TARGET), run_name="__main__")
