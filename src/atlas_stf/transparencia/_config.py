"""Configuration for STF transparency portal CSV fetcher."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

BASE_URL = "https://transparencia.stf.jus.br/extensions"

PAINEIS: dict[str, str] = {
    "acervo": "Acervo de processos em tramitação",
    "decisoes": "Decisões do STF",
    "distribuidos": "Registro e distribuição",
    "recebidos_baixados": "Recebimento e baixa",
    "repercussao_geral": "Repercussão Geral",
    "controle_concentrado": "Controle Concentrado",
    "plenario_virtual": "Plenário Virtual",
    "decisoes_covid": "Decisões Covid-19",
    "reclamacoes": "Reclamações",
    "taxa_provimento": "Taxa de Provimento",
    "omissao_inconstitucional": "Omissão Inconstitucional",
}

ALL_PAINEL_SLUGS: tuple[str, ...] = tuple(PAINEIS.keys())


def painel_url(slug: str) -> str:
    """Build the Qlik Sense dashboard URL for a panel slug."""
    return f"{BASE_URL}/{slug}/{slug}.html"


@dataclass(frozen=True)
class TransparenciaFetchConfig:
    """Configuration for an STF transparency portal fetch run."""

    output_dir: Path = field(default_factory=lambda: Path("data/raw/transparencia"))
    paineis: tuple[str, ...] = ALL_PAINEL_SLUGS
    headless: bool = True
    ignore_tls: bool = False
    dry_run: bool = False
    timeout_qlik_load_ms: int = 120_000
    timeout_download_ms: int = 600_000
