"""Configuration for CGU Portal da Transparencia data fetch."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

CGU_BASE_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"
CGU_DOWNLOAD_URL = "https://portaldatransparencia.gov.br/download-de-dados"
CGU_API_KEY_ENV = "CGU_API_KEY"


@dataclass(frozen=True)
class CguFetchConfig:
    """Configuration for a CGU CEIS/CNEP fetch run.

    Primary strategy: download bulk CSV files from Portal da Transparencia.
    Fallback: query REST API per entity name (requires api_key).
    """

    output_dir: Path = field(default_factory=lambda: Path("data/raw/cgu"))
    api_key: str = ""
    party_path: Path = field(default_factory=lambda: Path("data/curated/party.jsonl"))
    rate_limit_seconds: float = 0.7
    max_retries: int = 3
    timeout_seconds: int = 30
    dry_run: bool = False
    force_refresh: bool = False
