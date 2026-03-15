"""Configuration for OAB CNA/CNSA validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

OAB_API_KEY_ENV = "OAB_API_KEY"


@dataclass(frozen=True)
class OabValidationConfig:
    """Configuration for OAB validation run.

    Providers:
      - ``null``: no validation (default, offline-safe)
      - ``format``: format-only regex check via ``is_valid_oab_format``
      - ``cna``: CNA web service (requires api_key, rate-limited)
      - ``cnsa``: CNSA — currently unavailable
    """

    curated_dir: Path = field(default_factory=lambda: Path("data/curated"))
    output_dir: Path = field(default_factory=lambda: Path("data/curated"))
    provider: str = "null"
    rate_limit_seconds: float = 2.0
    max_retries: int = 3
    batch_size: int = 50
    api_key: str | None = None
