"""TD-240 plan B: the quote mirror and its read/write helpers stay deleted."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PATTERN = re.compile(r"write_contract_quote_live|get_contract_quotes|quote_mirror")
# A doc line naming the table as something the daemon writes ("contract_quote_live ... → Golden Source").
DOC_WRITE = re.compile(r"contract_quote_live[^\n]*→")


def test_src_has_no_quote_mirror_or_contract_quote_rw() -> None:
    hits: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if PATTERN.search(line):
                rel = path.relative_to(SRC)
                hits.append(f"{rel}:{lineno}:{line.strip()}")
    assert hits == []


def test_claude_md_does_not_list_contract_quote_live_as_a_daemon_write() -> None:
    """TD-260: the agent doc told the next reader the daemon still mirrors quotes."""
    lines = (ROOT / "CLAUDE.md").read_text(encoding="utf-8").splitlines()
    assert [f"CLAUDE.md:{n}:{line.strip()}" for n, line in enumerate(lines, start=1) if DOC_WRITE.search(line)] == []
