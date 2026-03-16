"""Pure functions for parsing TSE party organ finance CSV files.

Handles two CSV types from ``prestacao_de_contas_eleitorais_orgaos_partidarios_{year}.zip``:
  - ``receitas_orgaos_partidarios_{year}_BRASIL.csv``  (48 columns, revenue)
  - ``despesas_contratadas_orgaos_partidarios_{year}_BRASIL.csv``  (46 columns, expense)

Headers are stable across all supported years (2018-2024).
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

# Revenue CSV column map (receitas_orgaos_partidarios)
_REVENUE_COLUMNS: dict[str, list[str]] = {
    "election_year": ["AA_ELEICAO"],
    "state": ["SG_UF"],
    "org_scope": ["DS_ESFERA_PARTIDARIA"],
    "org_party_name": ["NM_PARTIDO"],
    "org_party_abbrev": ["SG_PARTIDO"],
    "org_cnpj": ["NR_CNPJ_PRESTADOR_CONTA"],
    "counterparty_name": ["NM_DOADOR"],
    "counterparty_name_rfb": ["NM_DOADOR_RFB"],
    "counterparty_tax_id": ["NR_CPF_CNPJ_DOADOR"],
    "counterparty_cnae_code": ["CD_CNAE_DOADOR"],
    "counterparty_cnae_desc": ["DS_CNAE_DOADOR"],
    "amount": ["VR_RECEITA"],
    "date": ["DT_RECEITA"],
    "description": ["DS_RECEITA"],
}

# Expense CSV column map (despesas_contratadas_orgaos_partidarios)
_EXPENSE_COLUMNS: dict[str, list[str]] = {
    "election_year": ["AA_ELEICAO"],
    "state": ["SG_UF"],
    "org_scope": ["DS_ESFERA_PARTIDARIA"],
    "org_party_name": ["NM_PARTIDO"],
    "org_party_abbrev": ["SG_PARTIDO"],
    "org_cnpj": ["NR_CNPJ_PRESTADOR_CONTA"],
    "counterparty_name": ["NM_FORNECEDOR"],
    "counterparty_name_rfb": ["NM_FORNECEDOR_RFB"],
    "counterparty_tax_id": ["NR_CPF_CNPJ_FORNECEDOR"],
    "counterparty_cnae_code": ["CD_CNAE_FORNECEDOR"],
    "counterparty_cnae_desc": ["DS_CNAE_FORNECEDOR"],
    "amount": ["VR_DESPESA_CONTRATADA"],
    "date": ["DT_DESPESA"],
    "description": ["DS_DESPESA"],
}


def _resolve_column(header: list[str], aliases: list[str]) -> str | None:
    """Find the actual column name in the CSV header from a list of aliases."""
    header_upper = [h.upper().strip() for h in header]
    for alias in aliases:
        alias_upper = alias.upper().strip()
        if alias_upper in header_upper:
            idx = header_upper.index(alias_upper)
            return header[idx]
    return None


def _safe_get(row: dict[str, str], header: list[str], column_map: dict[str, list[str]], field_key: str) -> str:
    """Get a value from a CSV row dict using column aliases."""
    aliases = column_map.get(field_key, [])
    col = _resolve_column(header, aliases)
    if col is None:
        return ""
    val = row.get(col, "").strip()
    return "" if val in ("", "nan", "NaN") else val


def _iter_party_org_csv(
    csv_path: Path,
    record_kind: str,
    column_map: dict[str, list[str]],
) -> Iterator[dict[str, Any]]:
    """Yield raw dicts one by one from a party org CSV (';' separator).

    Generator approach avoids loading all records into memory at once.
    """
    encoding = detect_encoding(csv_path)
    missing_counterparty_count = 0
    total_count = 0

    with csv_path.open("r", encoding=encoding, newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";", quotechar='"')
        if reader.fieldnames is None:
            return

        header = list(reader.fieldnames)

        for row in reader:
            total_count += 1
            counterparty_name = _safe_get(row, header, column_map, "counterparty_name")
            amount_raw = _safe_get(row, header, column_map, "amount")

            # Structurally invalid: no amount AND no counterparty AND no description
            description = _safe_get(row, header, column_map, "description")
            if not amount_raw and not counterparty_name and not description:
                continue

            if not counterparty_name:
                missing_counterparty_count += 1

            yield {
                "record_kind": record_kind,
                "election_year_raw": _safe_get(row, header, column_map, "election_year"),
                "state": _safe_get(row, header, column_map, "state"),
                "org_scope": _safe_get(row, header, column_map, "org_scope"),
                "org_party_name": _safe_get(row, header, column_map, "org_party_name"),
                "org_party_abbrev": _safe_get(row, header, column_map, "org_party_abbrev"),
                "org_cnpj": _safe_get(row, header, column_map, "org_cnpj"),
                "counterparty_name": counterparty_name,
                "counterparty_name_rfb": _safe_get(row, header, column_map, "counterparty_name_rfb"),
                "counterparty_tax_id": _safe_get(row, header, column_map, "counterparty_tax_id"),
                "counterparty_cnae_code": _safe_get(row, header, column_map, "counterparty_cnae_code"),
                "counterparty_cnae_desc": _safe_get(row, header, column_map, "counterparty_cnae_desc"),
                "amount_raw": amount_raw,
                "date_raw": _safe_get(row, header, column_map, "date"),
                "description": description,
            }

    if missing_counterparty_count:
        logger.info(
            "%s: %d/%d records with missing counterparty in %s",
            record_kind,
            missing_counterparty_count,
            total_count,
            csv_path.name,
        )


def iter_receitas_csv(csv_path: Path) -> Iterator[dict[str, Any]]:
    """Yield raw revenue dicts from a receitas CSV."""
    return _iter_party_org_csv(csv_path, "revenue", _REVENUE_COLUMNS)


def iter_despesas_csv(csv_path: Path) -> Iterator[dict[str, Any]]:
    """Yield raw expense dicts from a despesas contratadas CSV."""
    return _iter_party_org_csv(csv_path, "expense", _EXPENSE_COLUMNS)


def normalize_party_org_record(raw: dict[str, Any], year: int) -> dict[str, Any]:
    """Normalize a raw party org record into the canonical schema."""
    counterparty_name = raw.get("counterparty_name", "")
    counterparty_name_rfb = raw.get("counterparty_name_rfb", "")

    return {
        "record_kind": raw.get("record_kind", ""),
        "actor_kind": "party_org",
        "election_year": year,
        "state": raw.get("state", ""),
        "org_scope": raw.get("org_scope", ""),
        "org_party_name": raw.get("org_party_name", ""),
        "org_party_abbrev": raw.get("org_party_abbrev", ""),
        "org_cnpj": raw.get("org_cnpj", ""),
        "counterparty_name": counterparty_name,
        "counterparty_name_rfb": counterparty_name_rfb,
        "counterparty_tax_id": raw.get("counterparty_tax_id", ""),
        "counterparty_name_normalized": normalize_entity_name(counterparty_name_rfb or counterparty_name) or "",
        "counterparty_cnae_code": raw.get("counterparty_cnae_code", ""),
        "counterparty_cnae_desc": raw.get("counterparty_cnae_desc", ""),
        "transaction_amount": _parse_amount(raw.get("amount_raw", "")),
        "transaction_date": _parse_donation_date(raw.get("date_raw", "")),
        "transaction_description": raw.get("description", ""),
    }
