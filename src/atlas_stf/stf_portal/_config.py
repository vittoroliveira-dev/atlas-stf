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

    # Rate limiting (conservative defaults to avoid WAF blocks)
    rate_limit_seconds: float = 3.0
    max_concurrent: int = 1  # Portal is single-threaded safe default

    # Global concurrency control
    max_in_flight: int = 4  # Max simultaneous HTTP requests across all workers
    tab_concurrency: int = 2  # Max concurrent tab fetches per process

    # Global rate limiter (inter-request interval, shared across all workers)
    global_rate_seconds: float = 1.0  # Minimum interval between any two HTTP requests

    # Batch control
    batch_size: int = 100
    max_processes: int | None = None  # None = all processes

    # Re-fetch policy
    refetch_after_days: int = 30

    # Retry policy
    max_retries: int = 4
    retry_delay_seconds: float = 8.0

    # TLS
    ignore_tls: bool = False

    # Timeouts
    page_timeout_ms: int = 30_000
    navigation_timeout_ms: int = 15_000

    # Circuit breaker (per-proxy)
    circuit_breaker_threshold: int = 5
    circuit_breaker_cooldown: float = 120.0

    # Proxy rotation (SOCKS5 URLs for SSH tunnels)
    proxies: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
