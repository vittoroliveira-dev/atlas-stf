"""Validation rules for staging datasets."""

from __future__ import annotations

from .staging._cleaners import standardize_column_label
from .staging._config import CONFIGS

CRITICAL_RAW_COLUMNS: dict[str, list[str]] = {
    "acervo.csv": ["Processo", "Número único"],
    "controle_concentrado.csv": ["Processo"],
    "decisoes.csv": ["idFatoDecisao", "Processo", "Data da decisão", "Tipo decisão", "Andamento decisão"],
    "decisoes_covid.csv": ["Processo", "Data Decisão", "Tipo decisão"],
    "distribuidos.csv": ["Nº do processo", "Data da autuação"],
    "omissao_inconstitucional.csv": ["Processo", "Data julgamento"],
    "plenario_virtual.csv": ["Processo", "Data decisão", "Tipo decisão"],
    "recebidos_baixados.csv": ["Número", "Data autuação"],
    "reclamacoes.csv": ["Processo", "Número único", "Data Autuação"],
    "repercussao_geral.csv": ["Número Tema", "Processo Paradigma"],
}


def expected_staging_columns(filename: str) -> set[str]:
    config = CONFIGS.get(filename)
    raw_columns = set(CRITICAL_RAW_COLUMNS.get(filename, []))
    if config is not None:
        raw_columns.update(config.date_columns)
        if config.assunto_column:
            raw_columns.add(config.assunto_column)
        raw_columns.update(config.multi_value_columns)
    return {standardize_column_label(column) for column in raw_columns}
