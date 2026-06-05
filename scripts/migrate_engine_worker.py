#!/usr/bin/env python3
"""Migrate engine daemon + celery + data pipeline into bifrost_worker."""

from __future__ import annotations

import re
from pathlib import Path

ENGINE = Path(__file__).resolve().parents[2] / "bifrost-trader-engine"
WORKER = Path(__file__).resolve().parents[1] / "src" / "bifrost_worker"

DIRS = [
    ("src/daemon", "daemon"),
    ("src/workers", "celery"),
    ("src/bars", "data/bars"),
    ("src/massive", "data/massive"),
    ("src/vendor/massive", "data/massive/vendor"),
]

REPLS = [
    (r"\bfrom src\.daemon\b", "from bifrost_worker.daemon"),
    (r"\bfrom src\.workers\b", "from bifrost_worker.celery"),
    (r"\bfrom src\.bars\b", "from bifrost_worker.data.bars"),
    (r"\bfrom src\.massive\b", "from bifrost_worker.data.massive"),
    (r"\bfrom src\.vendor\.massive\b", "from bifrost_worker.data.massive.vendor"),
    (r"\bfrom src\.config\b", "from bifrost_core.config"),
    (r"\bfrom src\.core\b", "from bifrost_core.core"),
    (r"\bfrom src\.persistence\b", "from bifrost_core.persistence"),
    (r"\bfrom src\.portfolio\b", "from bifrost_core.portfolio"),
    (r"\bfrom src\.monitor\b", "from bifrost_core.monitor"),
    (r"\bfrom src\.ib_operator\b", "from bifrost_core.ib_operator"),
    (r"\bfrom src\.app\.config\b", "from bifrost_core.config.startup"),
    (r"\bfrom src\.daemon\.ib_edge\b", "from bifrost_core.portfolio.ib_edge"),
    (r"\bfrom src\.vendor\.ib_account_agent\.redis_keys\b", "from bifrost_core.core.realtime.ib_account_keys"),
    (r"\bfrom src\.bifrost\.redis_health_keys\b", "from bifrost_core.core.redis_health_keys"),
    (r"\bfrom src\.vendor\.ib_ingestor\.redis_keys\b", "from bifrost_core.core.realtime.ib_ingestor_keys"),
    (r"\bfrom src\.monitor\.integrations\.ib_clients\b", "from bifrost_socket.ib.connector.ib_clients"),
    (r"\bfrom src\.connector\.ib\b", "from bifrost_socket.ib.connector.ib_connector"),
]


def rewrite(text: str) -> str:
    for pat, rep in REPLS:
        text = re.sub(pat, rep, text)
    return text


def copy_tree(src_rel: str, dst_rel: str) -> None:
    src_root = ENGINE / src_rel
    dst_root = WORKER / dst_rel
    for path in src_root.rglob("*.py"):
        rel = path.relative_to(src_root)
        dst = dst_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(rewrite(path.read_text(encoding="utf-8")), encoding="utf-8")
    print(f"OK {dst_rel}/")


def main() -> None:
    for s, d in DIRS:
        copy_tree(s, d)
    print("Done.")


if __name__ == "__main__":
    main()
