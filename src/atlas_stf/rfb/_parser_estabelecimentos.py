"""Pure parsing functions for RFB Estabelecimentos CSV data."""

from __future__ import annotations

import csv
import logging
from typing import Any, TextIO

logger = logging.getLogger(__name__)


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
        if len(row) < 21:
            continue

        cnpj_basico = row[0].strip()
        if cnpj_basico not in target_cnpjs:
            continue

        cnpj_ordem = row[1].strip()
        cnpj_dv = row[2].strip()
        cnpj_full = f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}"

        # Split secondary CNAEs (comma-separated within the field)
        cnae_sec_raw = row[12].strip() if len(row) > 12 else ""
        cnae_secundaria = [c.strip() for c in cnae_sec_raw.split(",") if c.strip()] if cnae_sec_raw else []

        records.append(
            {
                "cnpj_basico": cnpj_basico,
                "cnpj_ordem": cnpj_ordem,
                "cnpj_dv": cnpj_dv,
                "cnpj_full": cnpj_full,
                "matriz_filial": row[3].strip(),
                "nome_fantasia": row[4].strip(),
                "situacao_cadastral": row[5].strip(),
                "data_situacao_cadastral": row[6].strip(),
                "motivo_situacao_cadastral": row[7].strip(),
                "data_inicio_atividade": row[10].strip() if len(row) > 10 else "",
                "cnae_fiscal": row[11].strip() if len(row) > 11 else "",
                "cnae_fiscal_secundaria": cnae_secundaria,
                "logradouro": row[14].strip() if len(row) > 14 else "",
                "numero": row[15].strip() if len(row) > 15 else "",
                "bairro": row[17].strip() if len(row) > 17 else "",
                "cep": row[18].strip() if len(row) > 18 else "",
                "uf": row[19].strip() if len(row) > 19 else "",
                "municipio": row[20].strip() if len(row) > 20 else "",
            }
        )

    return records
