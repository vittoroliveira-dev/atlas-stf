"""Audit trail and logging for scraper."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("atlas_stf.scraper")


@dataclass
class ScrapeAuditRecord:
    target_base: str
    partition: str
    doc_count: int
    sha256: str
    first_publicacao_data: str | None
    last_publicacao_data: str | None
    duration_seconds: float
    timestamp: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


def append_audit(record: ScrapeAuditRecord, output_dir: Path) -> Path:
    """Append an audit record to ``_audit.jsonl`` (incremental, never overwrites)."""
    audit_path = output_dir / "_audit.jsonl"
    with open(audit_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")
    return audit_path


def setup_logging(output_dir: Path, verbose: bool = False) -> Path:
    """Configure file + console logging for scraper."""
    log_path = output_dir / "_scrape.log"
    level = logging.DEBUG if verbose else logging.INFO

    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(ch)

    return log_path
