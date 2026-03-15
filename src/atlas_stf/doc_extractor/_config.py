"""Configuration for selective PDF document extraction."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DocExtractorConfig:
    """Configuration for a document extraction run.

    Targets representation edges with low confidence to enrich
    via PDF text extraction (procuracoes, peticoes).
    """

    curated_dir: Path = field(default_factory=lambda: Path("data/curated"))
    output_dir: Path = field(default_factory=lambda: Path("data/curated"))
    min_confidence_gap: float = 0.7  # Only process edges below this confidence
    max_documents: int | None = None  # Limit number of docs to process
    download_timeout: float = 30.0
    rate_limit_seconds: float = 2.0
