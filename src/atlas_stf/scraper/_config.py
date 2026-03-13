"""Declarative configuration for jurisprudência scraper."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ApiBase(str, Enum):
    DECISOES = "decisoes"
    ACORDAOS = "acordaos"


@dataclass(frozen=True)
class ScrapeTarget:
    base: ApiBase
    label: str
    output_subdir: str
    sort_fields: tuple[str, str]
    fields_to_extract: tuple[str, ...]
    text_fields: tuple[str, ...]
    page_size: int = 250


@dataclass(frozen=True)
class ScrapeConfig:
    target: ScrapeTarget
    output_dir: str = "data/raw/jurisprudencia"
    start_date: str | None = None
    end_date: str | None = None
    rate_limit_seconds: float = 0.5
    max_retries: int = 5
    retry_backoff_base: float = 2.0
    timeout_ms: int = 30_000
    headless: bool = True
    verbose: bool = False
    dry_run: bool = False


@dataclass
class CheckpointState:
    target_base: str
    current_partition: str
    search_after: list[str | int] | None = None
    partition_doc_count: int = 0
    total_doc_count: int = 0
    completed_partitions: list[str] = field(default_factory=list)
    last_updated: str = ""
    api_total_hits: int | None = None


DECISOES_TARGET = ScrapeTarget(
    base=ApiBase.DECISOES,
    label="Decisões Monocráticas",
    output_subdir="decisoes",
    sort_fields=("publicacao_data", "processo_numero"),
    fields_to_extract=(
        "processo_codigo_completo",
        "processo_numero",
        "processo_classe_processual_unificada_classe_sigla",
        "processo_classe_processual_unificada_extenso",
        "relator_processo_nome",
        "ministro_facet",
        "decisao_texto",
        "publicacao_data",
        "julgamento_data",
        "partes_lista_texto",
        "documental_legislacao_citada_texto",
        "documental_observacao_texto",
        "documental_indexacao_texto",
        "documental_publicacao_lista_texto",
        "acompanhamento_processual_url",
        "inteiro_teor_url",
        "procedencia_geografica_uf_sigla",
        "procedencia_geografica_completo",
        "titulo",
    ),
    text_fields=("decisao_texto", "partes_lista_texto", "documental_observacao_texto"),
)

ACORDAOS_TARGET = ScrapeTarget(
    base=ApiBase.ACORDAOS,
    label="Acórdãos",
    output_subdir="acordaos",
    sort_fields=("publicacao_data", "processo_numero"),
    fields_to_extract=(
        "processo_codigo_completo",
        "processo_numero",
        "processo_classe_processual_unificada_classe_sigla",
        "processo_classe_processual_unificada_extenso",
        "processo_classe_processual_unificada_sigla",
        "relator_processo_nome",
        "relator_acordao_nome",
        "revisor_processo_nome",
        "ministro_facet",
        "orgao_julgador",
        "ementa_texto",
        "acordao_ata",
        "inteiro_teor_texto",
        "publicacao_data",
        "julgamento_data",
        "partes_lista_texto",
        "documental_legislacao_citada_texto",
        "documental_observacao_texto",
        "documental_indexacao_texto",
        "documental_tese_texto",
        "documental_publicacao_lista_texto",
        "acompanhamento_processual_url",
        "inteiro_teor_url",
        "procedencia_geografica_uf_sigla",
        "procedencia_geografica_completo",
        "is_repercussao_geral",
        "is_questao_ordem",
        "titulo",
    ),
    text_fields=(
        "ementa_texto",
        "inteiro_teor_texto",
        "acordao_ata",
        "partes_lista_texto",
        "documental_observacao_texto",
    ),
)

TARGETS: dict[str, ScrapeTarget] = {
    "decisoes": DECISOES_TARGET,
    "acordaos": ACORDAOS_TARGET,
}
