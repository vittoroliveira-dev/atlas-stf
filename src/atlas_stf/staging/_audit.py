"""Audit trail for staging pipeline: JSON lines + logging."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("atlas_stf.staging")


@dataclass
class AuditRecord:
    filename: str
    raw_sha256: str
    staging_sha256: str
    raw_row_count: int
    staging_row_count: int
    transforms: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    timestamp: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


def _json_default(obj: object) -> object:
    if hasattr(obj, "__int__"):
        return int(obj)  # type: ignore[arg-type]
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def write_audit(records: list[AuditRecord], staging_dir: Path) -> Path:
    """Write audit records to JSONL file."""
    audit_path = staging_dir / "_audit.jsonl"
    with open(audit_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(asdict(record), ensure_ascii=False, default=_json_default) + "\n")
    return audit_path


def setup_logging(staging_dir: Path, verbose: bool = False) -> Path:
    """Configure logging to file and console."""
    log_path = staging_dir / "_cleaning.log"
    level = logging.DEBUG if verbose else logging.INFO

    # File handler
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(ch)

    return log_path
