"""TD-20: the worker imports core's public names only (core 0.34.0 published them)."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_no_private_core_name_is_imported() -> None:
    found = []
    for path in sorted((ROOT / "src").rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("bifrost_core"):
                found += [f"{path.relative_to(ROOT)}: {node.module}.{a.name}" for a in node.names if a.name.startswith("_")]
    assert found == []
