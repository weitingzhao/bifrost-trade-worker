"""Worker test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

_CORE_CONFIG_EXAMPLE = (
    Path(__file__).resolve().parents[2] / "bifrost-trade-core" / "config" / "config.yaml.example"
)


@pytest.fixture(autouse=True)
def bifrost_config_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point read_config() at bifrost-trade-core example YAML (worker repo has no config/)."""
    if _CORE_CONFIG_EXAMPLE.is_file():
        monkeypatch.setenv("BIFROST_CONFIG", str(_CORE_CONFIG_EXAMPLE))


@pytest.fixture
def sample_config():
    return {"gates": {"state": {"delta": {"threshold_hedge_shares": 25}}}}
