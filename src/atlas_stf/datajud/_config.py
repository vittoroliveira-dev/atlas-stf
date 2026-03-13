"""Configuration for DataJud API client."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

DATAJUD_BASE_URL = "https://api-publica.datajud.cnj.jus.br"
DATAJUD_API_KEY_ENV = "DATAJUD_API_KEY"


@dataclass(frozen=True)
class DatajudFetchConfig:
    """Configuration for a DataJud fetch run."""

    api_key: str
    process_path: Path = field(default_factory=lambda: Path("data/curated/process.jsonl"))
    output_dir: Path = field(default_factory=lambda: Path("data/raw/datajud"))
    rate_limit_seconds: float = 1.0
    max_retries: int = 3
    timeout_seconds: int = 30
    dry_run: bool = False
