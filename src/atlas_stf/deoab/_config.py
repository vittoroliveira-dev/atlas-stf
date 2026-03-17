"""Configuration for DEOAB gazette extractor."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

DEOAB_BASE_URL = "https://deoab.oab.org.br/assets/diarios"
DEOAB_PDF_PATTERN = "diario-eletronico-oab-{date}.pdf"

# Parser version — bump when regex patterns change to trigger reprocessing
PARSER_VERSION = 1

# First known DEOAB edition
FIRST_YEAR = 2019


@dataclass
class DeoabFetchConfig:
    """Configuration for the DEOAB fetcher."""

    output_dir: Path = field(default_factory=lambda: Path("data/raw/deoab"))
    checkpoint_file: Path = field(default_factory=lambda: Path("data/raw/deoab/.checkpoint.json"))

    # Date range
    start_year: int = FIRST_YEAR
    end_year: int | None = None  # None = current year

    # Rate limiting
    rate_limit_seconds: float = 1.0

    # Retry policy
    max_retries: int = 3
    retry_delay_seconds: float = 5.0

    # Dry run
    dry_run: bool = False

    # Force reprocess (ignore parser_version check)
    force_reprocess: bool = False

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
