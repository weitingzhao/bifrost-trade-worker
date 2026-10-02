"""The daemon reads the IB account snapshot through core, from redis-ib (debt TD-04)."""

from __future__ import annotations

import importlib
import inspect

import pytest


def test_the_worker_fork_is_gone() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("bifrost_worker.daemon.ib_edge")


def test_running_state_refreshes_through_core() -> None:
    from bifrost_worker.daemon.app import daemon_handlers

    src = inspect.getsource(daemon_handlers.handle_running)
    assert "from bifrost_core.portfolio.ib_edge import refresh_accounts_from_redis_edge" in src


def test_core_reads_the_ib_redis() -> None:
    from bifrost_core.portfolio import ib_edge

    # The plugin writes ib:account:snapshot:v1 to redis-ib; the per-env Redis never has it.
    assert "effective_ib_redis_dict" in inspect.getsource(ib_edge)
