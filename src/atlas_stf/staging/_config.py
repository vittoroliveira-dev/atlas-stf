"""Declarative per-file configuration for staging pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FileConfig:
    filename: str
    date_columns: list[str]
    required_fields: list[str]
    primary_key_columns: list[str]
    null_values: list[str] = field(default_factory=lambda: ["-", "*NI*"])
    has_multiline_fields: bool = False
    x000d_clean: bool = False
    assunto_column: str | None = None
    multi_value_separator: str | None = None  # ";#" for omissao
    multi_value_columns: list[str] = field(default_factory=list)
    reconcile_process_reference: bool = False


CONFIGS: dict[str, FileConfig] = {
    "distribuidos.csv": FileConfig(
        filename="distribuidos.csv",
        date_columns=["Data da autuação", "Data da baixa", "Data do andamento"],
        required_fields=["Classe", "Nº do processo", "Data da autuação"],
        primary_key_columns=["Classe", "Nº do processo", "Data do andamento", "Tipo de andamento"],
        null_values=["-"],
        assunto_column="Assunto completo",
    ),
    "recebidos_baixados.csv": FileConfig(
        filename="recebidos_baixados.csv",
        date_columns=["Data autuação", "Data baixa"],
        required_fields=["Classe", "Número", "Data autuação"],
        primary_key_columns=["Classe", "Número", "Tipo andamento", "Data autuação"],
        null_values=["-"],
        assunto_column="Assunto completo",
    ),
    "reclamacoes.csv": FileConfig(
        filename="reclamacoes.csv",
        date_columns=["Data Autuação"],
        required_fields=["Processo", "Número único", "Data Autuação"],
        primary_key_columns=["Processo"],
        null_values=["*NI*"],
    ),
    "acervo.csv": FileConfig(
        filename="acervo.csv",
        date_columns=[
            "Data autuação",
            "Data autuação agregada",
            "Data primeira distribuição",
            "Data última distribuição",
            "Data primeria decisão",
            "Data última decisão",
            "Data baixa",
            "Data último andamento",
        ],
        required_fields=["Processo", "Número único"],
        primary_key_columns=["Processo"],
        null_values=["-", "*NI*"],
        has_multiline_fields=True,
        x000d_clean=True,
    ),
    "decisoes.csv": FileConfig(
        filename="decisoes.csv",
        date_columns=["Data de autuação", "Data baixa", "Data da decisão"],
        required_fields=["idFatoDecisao", "Processo", "Data da decisão", "Tipo decisão", "Andamento decisão"],
        primary_key_columns=["idFatoDecisao"],
        null_values=["*NI*"],
        has_multiline_fields=True,
        reconcile_process_reference=True,
    ),
    "plenario_virtual.csv": FileConfig(
        filename="plenario_virtual.csv",
        date_columns=["Data autuação", "Data decisão", "Data baixa"],
        required_fields=["Processo", "Data decisão", "Tipo decisão"],
        primary_key_columns=["Processo", "Data decisão", "Tipo decisão", "Cod andamento"],
        null_values=["-"],
        has_multiline_fields=True,
        x000d_clean=True,
        assunto_column="Assunto completo",
        reconcile_process_reference=True,
    ),
    "decisoes_covid.csv": FileConfig(
        filename="decisoes_covid.csv",
        date_columns=["Data Autuação", "Data Preferência COVID", "Data Decisão"],
        required_fields=["Processo", "Data Decisão", "Tipo decisão"],
        primary_key_columns=["Processo", "Data Decisão", "Tipo decisão"],
        null_values=["-", "*NI*"],
        has_multiline_fields=True,
        x000d_clean=True,
        reconcile_process_reference=True,
    ),
    "repercussao_geral.csv": FileConfig(
        filename="repercussao_geral.csv",
        date_columns=[
            "Data autuação",
            "Data admissibilidade RG",
            "Data julgamento tema",
            "Data Determinação Suspensão Nacional",
            "Data Revogação Suspensão Nacional",
        ],
        required_fields=["Número Tema", "Processo Paradigma"],
        primary_key_columns=["Número Tema"],
        null_values=["-"],
        has_multiline_fields=True,
        x000d_clean=True,
    ),
    "controle_concentrado.csv": FileConfig(
        filename="controle_concentrado.csv",
        date_columns=[
            "Data Autuação",
            "Data Trânsito Julgado",
            "Data Baixa",
            "Data Publicação Pauta",
            "Data Publicação Pauta Primeira",
            "Data Publicação Pauta Última",
            "Data Publicação Decisão Colegiada",
            "Data Publicação Decisão Colegiada Primeira",
            "Data Publicação Decisão Colegiada Última",
            "Data Decisão Final",
            "Data Decisão Final Primeira",
            "Data Decisão Final Última",
            "Data Publicação Decisão Monocrática",
            "Data Publicação Decisão Monocrática Primeira",
            "Data Publicação Decisão Monocrática Última",
        ],
        required_fields=["Processo"],
        primary_key_columns=["Processo"],
        null_values=["*NI*"],
        has_multiline_fields=True,
        x000d_clean=True,
    ),
    "omissao_inconstitucional.csv": FileConfig(
        filename="omissao_inconstitucional.csv",
        date_columns=["Data julgamento"],
        required_fields=["Processo", "Data julgamento"],
        primary_key_columns=["Processo"],
        null_values=["*NI*"],
        has_multiline_fields=True,
        x000d_clean=True,
        multi_value_separator=";#",
        multi_value_columns=["Tipo de omissão", "Ramo do Direito"],
    ),
}
