"""Pure functions for parsing TSE campaign expense CSV files.

Column aliases derived EXCLUSIVELY from empirical evidence (see docs/tse_despesas_evidence.md).
Six schema generations: Gen1 (2002), Gen2 (2004), Gen3 (2006), Gen4 (2008), Gen5 (2010), Gen6 (2022-2024).
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name
from ._parser import _parse_amount, _parse_donation_date, detect_encoding

logger = logging.getLogger(__name__)

_EXPENSE_COLUMN_ALIASES: dict[str, list[str]] = {
    "candidate_name": [
        "NM_CANDIDATO",  # Gen4 (2008), Gen6 (2022+)
        "NOME_CANDIDATO",  # Gen3 (2006)
        "Nome candidato",  # Gen5 (2010)
        "NO_CAND",  # Gen1 (2002), Gen2 (2004)
    ],
    "candidate_cpf": [
        "NR_CPF_CANDIDATO",  # Gen6 (2022+)
        "CPF do candidato",  # Gen5 (2010)
    ],
    "candidate_number": [
        "NR_CANDIDATO",  # Gen4 (2008), Gen6 (2022+)
        "NUMERO_CANDIDATO",  # Gen3 (2006)
        "Número candidato",  # Gen5 (2010)
        "NR_CAND",  # Gen1 (2002), Gen2 (2004)
    ],
    "position": [
        "DS_CARGO",  # Gen1 (2002), Gen4 (2008), Gen6 (2022+)
        "DESCRICAO_CARGO",  # Gen3 (2006)
        "Cargo",  # Gen5 (2010)
    ],
    "party_abbrev": [
        "SG_PARTIDO",  # Gen4 (2008), Gen6 (2022+)
        "SIGLA_PARTIDO",  # Gen3 (2006)
        "Sigla Partido",  # Gen5 (2010)
        "SG_PART",  # Gen1 (2002), Gen2 (2004)
    ],
    "party_name": [
        "NM_PARTIDO",  # Gen6 (2022+)
    ],
    "state": [
        "SG_UF",  # Gen1 (2002), Gen6 (2022+)
        "SG_UE",  # Gen2 (2004)
        "UF",  # Gen5 (2010)
        "UNIDADE_ELEITORAL_CANDIDATO",  # Gen3 (2006)
    ],
    "election_year": [
        "AA_ELEICAO",  # Gen6 (2022+)
    ],
    "expense_amount": [
        "VR_DESPESA_CONTRATADA",  # Gen6 (2022+)
        "VR_DESPESA",  # Gen1-Gen4 (2002-2008)
        "Valor despesa",  # Gen5 (2010)
    ],
    "expense_date": [
        "DT_DESPESA",  # Gen3 (2006), Gen4 (2008), Gen6 (2022+)
        "DT_DOC_DESP",  # Gen1 (2002), Gen2 (2004)
        "Data da despesa",  # Gen5 (2010)
    ],
    "expense_description": [
        "DS_DESPESA",  # Gen6 (2022+) — free-text
        "Descriçao da despesa",  # Gen5 (2010) — typo in original file (no tilde)
        "DS_TITULO",  # Gen1 (2002), Gen4 (2008)
        "RTRIM(LTRIM(DR.DS_TITULO))",  # Gen2 (2004) — SQL artifact column name
        "TIPO_DESPESA",  # Gen3 (2006) — type used as description fallback
    ],
    "expense_document_type": [
        "DS_TIPO_DOCUMENTO",  # Gen2 (2004), Gen4 (2008), Gen6 (2022+)
        "TIPO_DOCUMENTO",  # Gen3 (2006)
        "Tipo do documento",  # Gen5 (2010)
    ],
    "expense_document_number": [
        "NR_DOCUMENTO",  # Gen6 (2022+)
        "NUMERO_DOCUMENTO",  # Gen3 (2006)
        "DS_NR_DOCUMENTO",  # Gen4 (2008)
        "NR_DOC_DESP",  # Gen2 (2004)
        "Número do documento",  # Gen5 (2010)
    ],
    "expense_type": [
        "Tipo despesa",  # Gen5 (2010) — separate type field
    ],
    "supplier_name": [
        "NM_FORNECEDOR",  # Gen4 (2008), Gen6 (2022+)
        "NOME_FORNECEDOR",  # Gen3 (2006)
        "Nome do fornecedor",  # Gen5 (2010)
        "NO_FOR",  # Gen1 (2002), Gen2 (2004)
    ],
    "supplier_tax_id": [
        "NR_CPF_CNPJ_FORNECEDOR",  # Gen6 (2022+)
        "CD_CPF_CNPJ_FORNECEDOR",  # Gen4 (2008)
        "NUMERO_CPF_CGC_FORNECEDOR",  # Gen3 (2006)
        "CPF/CNPJ do fornecedor",  # Gen5 (2010)
        "CD_CPF_CGC",  # Gen1 (2002), Gen2 (2004)
    ],
    "supplier_name_rfb": [
        "NM_FORNECEDOR_RFB",  # Gen6 (2022+)
    ],
    "supplier_cnae_code": [
        "CD_CNAE_FORNECEDOR",  # Gen6 (2022+)
    ],
    "supplier_cnae_desc": [
        "DS_CNAE_FORNECEDOR",  # Gen6 (2022+)
    ],
    "supplier_state": [
        "SG_UF_FORNECEDOR",  # Gen1 (2002), Gen6 (2022+)
        "UNIDADE_ELEITORAL_FORNECEDOR",  # Gen3 (2006)
    ],
    "origin_description": [
        "DS_ORIGEM_DESPESA",  # Gen6 (2022+)
    ],
}


def _resolve_expense_column(header: list[str], field_key: str) -> str | None:
    """Find the actual column name in the CSV header for a given field key."""
    aliases = _EXPENSE_COLUMN_ALIASES.get(field_key, [])
    header_upper = [h.upper().strip() for h in header]
    for alias in aliases:
        alias_upper = alias.upper().strip()
        if alias_upper in header_upper:
            idx = header_upper.index(alias_upper)
            return header[idx]
    return None


def _safe_get_expense(row: dict[str, str], header: list[str], field_key: str) -> str | None:
    """Get a value from a CSV row dict using expense column aliases.

    Returns None when the field does not exist in this generation's header
    (distinguishing 'absent field' from 'present but empty value').
    """
    col = _resolve_expense_column(header, field_key)
    if col is None:
        return None
    val = row.get(col, "").strip()
    return "" if val in ("", "nan", "NaN") else val


def _iter_despesas_csv(csv_path: Path) -> Iterator[dict[str, Any]]:
    """Yield expense dicts one by one from a despesas CSV (';' separator).

    Records with empty supplier_name are preserved (nullable), not skipped.
    Only structurally empty rows are discarded.
    """
    encoding = detect_encoding(csv_path)
    with csv_path.open("r", encoding=encoding, newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";", quotechar='"')
        if reader.fieldnames is None:
            return

        header = list(reader.fieldnames)

        for row in reader:
            if not any(v.strip() for v in row.values() if v):
                continue

            supplier_name = _safe_get_expense(row, header, "supplier_name")
            if supplier_name is not None and not supplier_name:
                logger.debug("Expense record with empty supplier in %s", csv_path.name)

            yield {
                "election_year_raw": _safe_get_expense(row, header, "election_year"),
                "state": _safe_get_expense(row, header, "state"),
                "position": _safe_get_expense(row, header, "position"),
                "candidate_name": _safe_get_expense(row, header, "candidate_name"),
                "candidate_cpf": _safe_get_expense(row, header, "candidate_cpf"),
                "candidate_number": _safe_get_expense(row, header, "candidate_number"),
                "party_abbrev": _safe_get_expense(row, header, "party_abbrev"),
                "party_name": _safe_get_expense(row, header, "party_name"),
                "expense_amount_raw": _safe_get_expense(row, header, "expense_amount"),
                "expense_date_raw": _safe_get_expense(row, header, "expense_date"),
                "expense_description": _safe_get_expense(row, header, "expense_description"),
                "expense_document_type": _safe_get_expense(row, header, "expense_document_type"),
                "expense_document_number": _safe_get_expense(row, header, "expense_document_number"),
                "expense_type": _safe_get_expense(row, header, "expense_type"),
                "supplier_name": supplier_name,
                "supplier_tax_id": _safe_get_expense(row, header, "supplier_tax_id"),
                "supplier_name_rfb": _safe_get_expense(row, header, "supplier_name_rfb"),
                "supplier_cnae_code": _safe_get_expense(row, header, "supplier_cnae_code"),
                "supplier_cnae_desc": _safe_get_expense(row, header, "supplier_cnae_desc"),
                "supplier_state": _safe_get_expense(row, header, "supplier_state"),
                "origin_description": _safe_get_expense(row, header, "origin_description"),
            }


def parse_despesas_csv(csv_path: Path) -> list[dict[str, Any]]:
    """Parse a despesas CSV and return raw dicts.

    Convenience wrapper that materializes the generator into a list.
    """
    return list(_iter_despesas_csv(csv_path))


def normalize_expense_record(raw: dict[str, Any], year: int) -> dict[str, Any]:
    """Normalize a raw expense record into the canonical schema.

    Fields absent from the source CSV generation are preserved as None.
    """
    supplier_name: str | None = raw.get("supplier_name")
    supplier_name_rfb: str | None = raw.get("supplier_name_rfb")
    name_for_norm = supplier_name_rfb or supplier_name
    supplier_normalized: str | None = normalize_entity_name(name_for_norm) if name_for_norm else None

    return {
        "election_year": year,
        "state": raw.get("state"),
        "position": raw.get("position"),
        "candidate_name": raw.get("candidate_name"),
        "candidate_cpf": raw.get("candidate_cpf"),
        "candidate_number": raw.get("candidate_number"),
        "party_abbrev": raw.get("party_abbrev"),
        "party_name": raw.get("party_name"),
        "expense_amount": _parse_amount(raw.get("expense_amount_raw") or ""),
        "expense_date": _parse_donation_date(raw.get("expense_date_raw") or ""),
        "expense_description": raw.get("expense_description"),
        "expense_document_type": raw.get("expense_document_type"),
        "expense_document_number": raw.get("expense_document_number"),
        "expense_type": raw.get("expense_type"),
        "supplier_name": supplier_name,
        "supplier_tax_id": raw.get("supplier_tax_id"),
        "supplier_name_normalized": supplier_normalized,
        "supplier_name_rfb": supplier_name_rfb,
        "supplier_cnae_code": raw.get("supplier_cnae_code"),
        "supplier_cnae_desc": raw.get("supplier_cnae_desc"),
        "supplier_state": raw.get("supplier_state"),
        "origin_description": raw.get("origin_description"),
    }
