"""TD-240 plan B: the quote mirror and its read/write helpers stay deleted."""

from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
PATTERN = re.compile(r"write_contract_quote_live|get_contract_quotes|quote_mirror")


def test_src_has_no_quote_mirror_or_contract_quote_rw() -> None:
    hits: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if PATTERN.search(line):
                rel = path.relative_to(SRC)
                hits.append(f"{rel}:{lineno}:{line.strip()}")
    assert hits == []
