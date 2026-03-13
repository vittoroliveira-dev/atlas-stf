"""Configuration for TSE campaign donation data fetch."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

TSE_CDN_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas"
TSE_ELECTION_YEARS = (2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024)


@dataclass(frozen=True)
class TseFetchConfig:
    """Configuration for a TSE donation data fetch run."""

    output_dir: Path = field(default_factory=lambda: Path("data/raw/tse"))
    years: tuple[int, ...] = TSE_ELECTION_YEARS
    timeout_seconds: int = 120
    dry_run: bool = False
