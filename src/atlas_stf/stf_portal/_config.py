"""Configuration for STF portal extractor."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class StfPortalConfig:
    """Configuration for the STF portal timeline extractor."""

    output_dir: Path = field(default_factory=lambda: Path("data/raw/stf_portal"))
    curated_dir: Path = field(default_factory=lambda: Path("data/curated"))
    checkpoint_file: Path = field(default_factory=lambda: Path("data/raw/stf_portal/.checkpoint.json"))

    # Rate limiting
    rate_limit_seconds: float = 2.0
    max_concurrent: int = 1  # Portal is single-threaded safe default

    # Batch control
    batch_size: int = 100
    max_processes: int | None = None  # None = all processes

    # Re-fetch policy
    refetch_after_days: int = 30

    # Retry policy
    max_retries: int = 3
    retry_delay_seconds: float = 5.0

    # TLS
    ignore_tls: bool = False

    # Timeouts
    page_timeout_ms: int = 30_000
    navigation_timeout_ms: int = 15_000

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
