"""Pure parsing functions for RFB CNPJ CSV data."""

from __future__ import annotations

import _csv
import csv
import io
import logging
from typing import Any, TextIO

from ..core.identity import normalize_entity_name, normalize_tax_id

logger = logging.getLogger(__name__)

# Socios CSV columns (positional, no header)
_SOCIOS_COLS = {
    0: "cnpj_basico",
    1: "partner_type",
    2: "partner_name",
    3: "partner_cpf_cnpj",
    4: "qualification_code",
    5: "entry_date",
    7: "representative_cpf_cnpj",
    8: "representative_name",
    9: "representative_qualification",
}

# Empresas CSV columns (positional, no header)
_EMPRESAS_COLS = {
    0: "cnpj_basico",
    1: "razao_social",
    2: "natureza_juridica",
    4: "capital_social",
    5: "porte_empresa",
}


def detect_encoding(raw_bytes: bytes) -> str:
    """Detect encoding of raw bytes (UTF-8 vs ISO-8859-1) without lossy fallback."""
    try:
        raw_bytes.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "iso-8859-1"


def parse_socios_csv_filtered(
    csv_bytes: bytes,
    target_names: set[str],
    target_cnpjs: set[str],
    *,
    target_cpfs: set[str] | None = None,
    target_partner_cnpjs: set[str] | None = None,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Parse Socios CSV bytes, keeping only rows matching target_names or target_cnpjs."""
    encoding = detect_encoding(csv_bytes)
    text = csv_bytes.decode(encoding)
    return parse_socios_csv_filtered_text(
        io.StringIO(text),
        target_names,
        target_cnpjs,
        target_cpfs=target_cpfs,
        target_partner_cnpjs=target_partner_cnpjs,
    )


def _parse_socios_reader(
    reader: _csv.Reader,
    target_names: set[str],
    target_cnpjs: set[str],
    target_cpfs: set[str],
    target_partner_cnpjs: set[str],
) -> tuple[list[dict[str, Any]], set[str]]:
    records: list[dict[str, Any]] = []
    matched_cnpjs: set[str] = set()

    for row in reader:
        if len(row) < 6:
            continue

        cnpj_basico = row[0].strip()
        partner_name = row[2].strip()
        partner_name_normalized = normalize_entity_name(partner_name)

        partner_cpf_cnpj_raw = row[3].strip()
        partner_doc_normalized = normalize_tax_id(partner_cpf_cnpj_raw)

        rep_cpf_cnpj = row[7].strip() if len(row) > 7 else ""
        rep_name_raw = row[8].strip() if len(row) > 8 else ""
        rep_qual = row[9].strip() if len(row) > 9 else ""
        rep_name_normalized = normalize_entity_name(rep_name_raw) if rep_name_raw else ""

        name_match = partner_name_normalized in target_names if partner_name_normalized else False
        rep_match = rep_name_normalized in target_names if rep_name_normalized else False
        cnpj_match = cnpj_basico in target_cnpjs
        cpf_match = partner_doc_normalized in target_cpfs if partner_doc_normalized else False
        partner_cnpj_match = partner_doc_normalized in target_partner_cnpjs if partner_doc_normalized else False

        if not name_match and not rep_match and not cnpj_match and not cpf_match and not partner_cnpj_match:
            continue

        if name_match or rep_match or cpf_match or partner_cnpj_match:
            matched_cnpjs.add(cnpj_basico)

        records.append(
            {
                "cnpj_basico": cnpj_basico,
                "partner_type": row[1].strip(),
                "partner_name": partner_name,
                "partner_name_normalized": partner_name_normalized or partner_name,
                "partner_cpf_cnpj": partner_cpf_cnpj_raw,
                "qualification_code": row[4].strip(),
                "entry_date": row[5].strip(),
                "representative_cpf_cnpj": rep_cpf_cnpj,
                "representative_name": rep_name_raw,
                "representative_name_normalized": rep_name_normalized or rep_name_raw,
                "representative_qualification": rep_qual,
            }
        )

    return records, matched_cnpjs


def parse_socios_csv_filtered_text(
    text_stream: TextIO,
    target_names: set[str],
    target_cnpjs: set[str],
    *,
    target_cpfs: set[str] | None = None,
    target_partner_cnpjs: set[str] | None = None,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Parse Socios CSV from an already-open text stream."""
    reader = csv.reader(text_stream, delimiter=";")
    return _parse_socios_reader(
        reader,
        target_names,
        target_cnpjs,
        target_cpfs or set(),
        target_partner_cnpjs or set(),
    )


def parse_empresas_csv_filtered(
    csv_bytes: bytes,
    target_cnpjs: set[str],
) -> list[dict[str, Any]]:
    """Parse Empresas CSV bytes, keeping only rows where cnpj_basico is in target_cnpjs."""
    encoding = detect_encoding(csv_bytes)
    text = csv_bytes.decode(encoding)
    return parse_empresas_csv_filtered_text(io.StringIO(text), target_cnpjs)


def _parse_empresas_reader(
    reader: _csv.Reader,
    target_cnpjs: set[str],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for row in reader:
        if len(row) < 6:
            continue

        cnpj_basico = row[0].strip()
        if cnpj_basico not in target_cnpjs:
            continue

        capital_raw = row[4].strip().replace(",", ".") if len(row) > 4 else ""
        capital_social: float | None = None
        if capital_raw:
            try:
                capital_social = float(capital_raw)
            except ValueError:
                capital_social = None

        records.append(
            {
                "cnpj_basico": cnpj_basico,
                "razao_social": row[1].strip(),
                "natureza_juridica": row[2].strip(),
                "capital_social": capital_social,
                "porte_empresa": row[5].strip() if len(row) > 5 else "",
            }
        )

    return records


def parse_empresas_csv_filtered_text(
    text_stream: TextIO,
    target_cnpjs: set[str],
) -> list[dict[str, Any]]:
    """Parse Empresas CSV from an already-open text stream."""
    reader = csv.reader(text_stream, delimiter=";")
    return _parse_empresas_reader(reader, target_cnpjs)
