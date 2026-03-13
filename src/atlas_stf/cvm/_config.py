"""Configuration for CVM processo sancionador data fetch."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

CVM_DATA_URL = "https://dados.cvm.gov.br/dados/PROCESSO/SANCIONADOR/DADOS/processo_sancionador.zip"


@dataclass(frozen=True)
class CvmFetchConfig:
    """Configuration for a CVM sanctions data fetch run.

    Downloads ZIP with processo sancionador CSVs from CVM open data portal.
    No authentication required.
    """

    output_dir: Path = field(default_factory=lambda: Path("data/raw/cvm"))
    timeout_seconds: int = 120
    dry_run: bool = False
