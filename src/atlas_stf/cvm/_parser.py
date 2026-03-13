"""Pure functions for parsing CVM processo sancionador CSV files."""

from __future__ import annotations

import csv
import io
import logging
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name

logger = logging.getLogger(__name__)

# Column name aliases for robustness across CVM schema changes.
_PROCESS_ALIASES: dict[str, list[str]] = {
    "process_number": ["NUP", "NUMERO_PROCESSO", "NR_PROCESSO", "numero_processo"],
    "subject": ["ASSUNTO", "DS_ASSUNTO", "assunto"],
    "opening_date": ["Data_Abertura", "DATA_ABERTURA", "DT_ABERTURA", "data_abertura"],
    "current_phase": ["Fase_Atual", "FASE_ATUAL", "DS_FASE", "fase_atual"],
    "object": ["Objeto", "OBJETO", "DS_OBJETO", "objeto"],
    "summary": ["Ementa", "EMENTA", "DS_EMENTA", "ementa"],
}

_ACCUSED_ALIASES: dict[str, list[str]] = {
    "process_number": ["NUP", "NUMERO_PROCESSO", "NR_PROCESSO", "numero_processo"],
    "accused_name": ["Nome_Acusado", "NOME_ACUSADO", "NM_ACUSADO", "nome_acusado", "NOME"],
    "accused_cpf_cnpj": ["CPF_CNPJ", "NR_CPF_CNPJ", "cpf_cnpj"],
}


def detect_encoding(path: Path) -> str:
    """Detect file encoding: try utf-8, fallback to latin-1."""
    try:
        path.read_text(encoding="utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _detect_delimiter(path: Path, encoding: str) -> str:
    """Detect CSV delimiter: ';' or ','."""
    first_line = path.read_text(encoding=encoding).split("\n", 1)[0]
    if ";" in first_line:
        return ";"
    return ","


def _resolve_column(header: list[str], aliases: list[str]) -> str | None:
    """Find the actual column name in the CSV header from a list of aliases."""
    header_upper = [h.upper().strip() for h in header]
    for alias in aliases:
        alias_upper = alias.upper().strip()
        if alias_upper in header_upper:
            idx = header_upper.index(alias_upper)
            return header[idx]
    return None


def _safe_get(row: dict[str, str], header: list[str], field_key: str, aliases: dict[str, list[str]]) -> str:
    """Get a value from a CSV row dict using column aliases."""
    field_aliases = aliases.get(field_key, [])
    col = _resolve_column(header, field_aliases)
    if col is None:
        return ""
    val = row.get(col, "").strip()
    return "" if val in ("", "nan", "NaN") else val


def parse_process_csv(path: Path) -> dict[str, dict[str, Any]]:
    """Parse processo_sancionador.csv and return {numero_processo: record}."""
    encoding = detect_encoding(path)
    delimiter = _detect_delimiter(path, encoding)
    text = path.read_text(encoding=encoding)

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter, quotechar='"')
    if reader.fieldnames is None:
        return {}

    header = list(reader.fieldnames)
    processes: dict[str, dict[str, Any]] = {}

    for row in reader:
        proc_num = _safe_get(row, header, "process_number", _PROCESS_ALIASES)
        if not proc_num:
            continue

        processes[proc_num] = {
            "process_number": proc_num,
            "subject": _safe_get(row, header, "subject", _PROCESS_ALIASES),
            "opening_date": _safe_get(row, header, "opening_date", _PROCESS_ALIASES),
            "current_phase": _safe_get(row, header, "current_phase", _PROCESS_ALIASES),
            "object": _safe_get(row, header, "object", _PROCESS_ALIASES),
            "summary": _safe_get(row, header, "summary", _PROCESS_ALIASES),
        }

    return processes


def parse_accused_csv(path: Path) -> list[dict[str, Any]]:
    """Parse processo_sancionador_acusado.csv and return list of accused records."""
    encoding = detect_encoding(path)
    delimiter = _detect_delimiter(path, encoding)
    text = path.read_text(encoding=encoding)

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter, quotechar='"')
    if reader.fieldnames is None:
        return []

    header = list(reader.fieldnames)
    accused: list[dict[str, Any]] = []

    for row in reader:
        name = _safe_get(row, header, "accused_name", _ACCUSED_ALIASES)
        if not name:
            continue

        accused.append(
            {
                "process_number": _safe_get(row, header, "process_number", _ACCUSED_ALIASES),
                "accused_name": name,
                "accused_cpf_cnpj": _safe_get(row, header, "accused_cpf_cnpj", _ACCUSED_ALIASES),
            }
        )

    return accused


def join_and_normalize(processes: dict[str, dict[str, Any]], accused: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Join accused with processes (1:N) and normalize to sanctions_raw schema."""
    records: list[dict[str, Any]] = []

    for acc in accused:
        proc_num = acc.get("process_number", "")
        proc = processes.get(proc_num)
        if proc is None:
            logger.debug("Skipping accused '%s': no matching process '%s'", acc.get("accused_name"), proc_num)
            continue

        entity_name = acc.get("accused_name", "")
        normalized_name = normalize_entity_name(entity_name)

        records.append(
            {
                "entity_name": normalized_name or entity_name,
                "entity_cnpj_cpf": acc.get("accused_cpf_cnpj", ""),
                "sanction_source": "cvm",
                "sanction_id": proc_num,
                "sanctioning_body": "CVM",
                "sanction_type": proc.get("subject", "") or proc.get("object", ""),
                "sanction_start_date": proc.get("opening_date", ""),
                "sanction_end_date": "",
                "sanction_description": proc.get("summary", "") or proc.get("object", ""),
            }
        )

    logger.info(
        "Joined %d accused records with %d processes → %d normalized records",
        len(accused),
        len(processes),
        len(records),
    )
    return records
