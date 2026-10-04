"""TD-80 (C1-a): import core's canonical module paths, never its aliases.

core keeps three pure re-export modules and the package-level re-exports of
``bifrost_core.monitor.reader`` only while a downstream still imports them; core 0.46.0
(TD-80 C1-b) deletes them, and an image built against that core would fail at import
time. Import the defining module instead:

    bifrost_core.config.startup                       -> bifrost_core.config.yaml_config
    bifrost_core.monitor.redis_url                    -> bifrost_core.core.redis_url
    bifrost_core.monitor.integrations.daemon_ib_edge  -> bifrost_core.monitor.integrations.platform_ib_gateway
    from bifrost_core.monitor.reader import <name>    -> the module that defines <name>
                                                         (StatusReader: monitor.reader.common; accounts
                                                         writers: portfolio.reader.accounts; ...)

``from bifrost_core.monitor.reader import <submodule>`` stays fine: that is a real module.
"""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from typing import Dict, Iterator, List, Set

ROOT = Path(__file__).resolve().parents[1]
SOURCES = sorted(p for d in ("src", "scripts", "tests") for p in (ROOT / d).rglob("*.py") if p != Path(__file__).resolve())

ALIAS_MODULES = (
    "bifrost_core.config.startup",
    "bifrost_core.monitor.redis_url",
    "bifrost_core.monitor.integrations.daemon_ib_edge",
)
READER_PACKAGE = "bifrost_core.monitor.reader"
ALLOWED: Dict[str, Set[str]] = {}


def _is_alias_module(module: str) -> bool:
    return any(module == a or module.startswith(a + ".") for a in ALIAS_MODULES)


def _is_reader_submodule(name: str) -> bool:
    return importlib.util.find_spec(f"{READER_PACKAGE}.{name}") is not None


def _alias_uses(source: str) -> Iterator[str]:
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            if _is_alias_module(node.module):
                yield from (f"{node.module}.{a.name}" for a in node.names)
            elif node.module == READER_PACKAGE:
                yield from (f"{node.module}.{a.name}" for a in node.names if not _is_reader_submodule(a.name))
        elif isinstance(node, ast.Import):
            yield from (a.name for a in node.names if _is_alias_module(a.name))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            # mock.patch / monkeypatch targets spelled as dotted strings
            value = node.value
            if _is_alias_module(value):
                yield value
            elif value.startswith(READER_PACKAGE + "."):
                head = value[len(READER_PACKAGE) + 1 :].split(".", 1)[0]
                if head.isidentifier() and not _is_reader_submodule(head):
                    yield value


def test_no_core_alias_path_is_imported() -> None:
    found: Dict[str, List[str]] = {}
    for path in SOURCES:
        rel = str(path.relative_to(ROOT))
        uses = sorted(set(_alias_uses(path.read_text())) - ALLOWED.get(rel, set()))
        if uses:
            found[rel] = uses
    assert found == {}


def test_the_guard_sees_an_alias() -> None:
    probe = (
        "import bifrost_core.monitor.redis_url\n"
        "from bifrost_core.config.startup import read_config\n"
        "from bifrost_core.monitor.reader import StatusReader, common\n"
        "patch('bifrost_core.monitor.reader.write_ib_config')\n"
        "patch('bifrost_core.monitor.reader.common.StatusReader')\n"
    )
    assert sorted(_alias_uses(probe)) == [
        "bifrost_core.config.startup.read_config",
        "bifrost_core.monitor.reader.StatusReader",
        "bifrost_core.monitor.reader.write_ib_config",
        "bifrost_core.monitor.redis_url",
    ]


def test_the_heartbeat_reads_the_gateway_through_the_canonical_module(monkeypatch) -> None:
    # The import sits behind `except Exception`: a missing module would not fail, it would
    # silently report ib_connected=False. Prove the call reaches platform_ib_gateway.
    from types import SimpleNamespace

    from bifrost_core.monitor.integrations import platform_ib_gateway
    from bifrost_worker.daemon.app import control_heartbeat

    seen = {}

    def fake(r, ib_cfg):
        seen["r"] = r
        return {"ib_connected": True, "ib_client_id": 7}

    monkeypatch.setattr(platform_ib_gateway, "derive_daemon_ib_heartbeat_from_redis", fake)
    client = object()
    app = SimpleNamespace(_redis_quotes_reader=SimpleNamespace(available=True, redis_client=client), config={})
    assert control_heartbeat.ib_edge_heartbeat_fields(app) == {"ib_connected": True, "ib_client_id": 7}
    assert seen["r"] is client
