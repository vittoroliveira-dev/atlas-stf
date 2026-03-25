"""Pure parsing functions for RFB Estabelecimentos CSV data."""

from __future__ import annotations

import csv
import logging
from typing import Any, TextIO

logger = logging.getLogger(__name__)

# Estabelecimentos CSV columns (positional, no header, semicolon-separated).
# Source: RFB layout documentation + verified against real data (2026-03).
# Total columns: 30 (indices 0–29).
_ESTABELECIMENTOS_SCHEMA: dict[int, str] = {
    0: "cnpj_basico",
    1: "cnpj_ordem",
    2: "cnpj_dv",
    3: "matriz_filial",
    4: "nome_fantasia",
    5: "situacao_cadastral",
    6: "data_situacao_cadastral",
    7: "motivo_situacao_cadastral",
    8: "nome_cidade_exterior",
    9: "cod_pais",
    10: "data_inicio_atividade",
    11: "cnae_fiscal",
    12: "cnae_fiscal_secundaria",
    13: "tipo_logradouro",
    14: "logradouro",
    15: "numero",
    16: "complemento",
    17: "bairro",
    18: "cep",
    19: "uf",
    20: "cod_municipio",
    21: "ddd1",
    22: "telefone1",
    23: "ddd2",
    24: "telefone2",
    25: "ddd_fax",
    26: "fax",
    27: "correio_eletronico",
    28: "situacao_especial",
    29: "data_situacao_especial",
}

_EXPECTED_MIN_COLUMNS = 21
_EXPECTED_FULL_COLUMNS = 30


def _col(row: list[str], index: int) -> str:
    """Safe positional access with bounds check."""
    if index < len(row):
        return row[index].strip()
    return ""


def parse_estabelecimentos_csv_filtered_text(
    text_stream: TextIO,
    target_cnpjs: set[str],
) -> list[dict[str, Any]]:
    """Parse Estabelecimentos CSV from text stream, filtering by cnpj_basico.

    Returns list of establishment records matching the target CNPJs.
    """
    reader = csv.reader(text_stream, delimiter=";")
    records: list[dict[str, Any]] = []

    for row in reader:
        if len(row) < _EXPECTED_MIN_COLUMNS:
            continue

        cnpj_basico = _col(row, 0)
        if cnpj_basico not in target_cnpjs:
            continue

        cnpj_ordem = _col(row, 1)
        cnpj_dv = _col(row, 2)
        cnpj_full = f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}"

        # Split secondary CNAEs (comma-separated within the field)
        cnae_sec_raw = _col(row, 12)
        cnae_secundaria = [c.strip() for c in cnae_sec_raw.split(",") if c.strip()] if cnae_sec_raw else []

        records.append(
            {
                "cnpj_basico": cnpj_basico,
                "cnpj_ordem": cnpj_ordem,
                "cnpj_dv": cnpj_dv,
                "cnpj_full": cnpj_full,
                "matriz_filial": _col(row, 3),
                "nome_fantasia": _col(row, 4),
                "situacao_cadastral": _col(row, 5),
                "data_situacao_cadastral": _col(row, 6),
                "motivo_situacao_cadastral": _col(row, 7),
                "data_inicio_atividade": _col(row, 10),
                "cnae_fiscal": _col(row, 11),
                "cnae_fiscal_secundaria": cnae_secundaria,
                "logradouro": _col(row, 14),
                "numero": _col(row, 15),
                "bairro": _col(row, 17),
                "cep": _col(row, 18),
                "uf": _col(row, 19),
                "municipio": _col(row, 20),
                "correio_eletronico": _col(row, 27),
            }
        )

    return records
