"""Configuration for OAB/SP society resolver."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

OABSP_SEARCH_URL = "https://www2.oabsp.org.br/asp/consultaSociedades/consultaSociedades02.asp"
OABSP_DETAIL_URL = "https://www2.oabsp.org.br/asp/consultaSociedades/consultaSociedades03.asp"
OABSP_INSCRITOS_URL = "https://www2.oabsp.org.br/asp/consultaInscritos/consulta01.asp"
PARSER_VERSION = 1


@dataclass
class OabSpFetchConfig:
    """Configuration for the OAB/SP society fetcher."""

    output_dir: Path = field(default_factory=lambda: Path("data/raw/oab_sp"))
    checkpoint_file: Path = field(default_factory=lambda: Path("data/raw/oab_sp/.checkpoint.json"))
    deoab_dir: Path = field(default_factory=lambda: Path("data/raw/deoab"))
    rate_limit_seconds: float = 1.5
    max_retries: int = 3
    retry_delay_seconds: float = 5.0
    timeout_seconds: int = 30
    dry_run: bool = False

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class OabSpLawyerLookupConfig:
    """Configuration for the OAB/SP lawyer lookup."""

    output_dir: Path = field(default_factory=lambda: Path("data/raw/oab_sp"))
    checkpoint_file: Path = field(default_factory=lambda: Path("data/raw/oab_sp/.checkpoint_lawyer.json"))
    deoab_dir: Path = field(default_factory=lambda: Path("data/raw/deoab"))
    curated_dir: Path = field(default_factory=lambda: Path("data/curated"))
    rate_limit_seconds: float = 1.5
    max_retries: int = 3
    retry_delay_seconds: float = 5.0
    timeout_seconds: int = 30
    dry_run: bool = False

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
