"""Pure functions for parsing TSE receitas CSV files."""

from __future__ import annotations

import codecs
import csv
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name

logger = logging.getLogger(__name__)

# Column aliases cover five TSE CSV generations:
#   2002:      NO_CAND, NO_DOADOR, CD_CPF_CGC, SG_PART, SG_UF, DS_CARGO
#   2004:      NO_CAND, NO_DOADOR, CD_CPF_CGC_DOA, SG_PART, SG_UE (UF), DS_CARGO
#   2006:      NOME_CANDIDATO, NOME_DOADOR, NUMERO_CPF_CGC_DOADOR, SIGLA_PARTIDO, DESCRICAO_CARGO
#   2008-2010: NM_CANDIDATO-style or "Nome candidato"-style (mixed)
#   2012-2014: "Nome candidato", "CPF/CNPJ do doador", "Sigla  Partido" (with double space)
#   2018+:     NM_CANDIDATO, NR_CPF_CNPJ_DOADOR, SG_PARTIDO
_COLUMN_ALIASES: dict[str, list[str]] = {
    "donor_cpf_cnpj": [
        "NR_CPF_CNPJ_DOADOR",
        "CPF_CNPJ_DOADOR",
        "CPF/CNPJ do doador",
        "CD_CPF_CGC",
        "CD_CPF_CGC_DOA",
        "NUMERO_CPF_CGC_DOADOR",
    ],
    "donor_name": [
        "NM_DOADOR",
        "Nome do doador",
        "NO_DOADOR",
        "NOME_DOADOR",
    ],
    "donor_name_originator": [
        "NM_DOADOR_ORIGINARIO",
        "Nome do doador originario",
        "NOME_DOADOR_ORIGINARIO",
    ],
    "donor_name_rfb": ["NM_DOADOR_RFB", "Nome do doador (Receita Federal)"],
    "donor_state": ["SG_UF_DOADOR", "UF_DOADOR", "UF do doador", "UNIDADE_ELEITORAL_DOADOR"],
    "donor_cnae_code": ["CD_CNAE_DOADOR", "CNAE do doador"],
    "donor_cnae_desc": ["DS_CNAE_DOADOR", "Descricao CNAE do doador"],
    "candidate_name": [
        "NM_CANDIDATO",
        "Nome candidato",
        "NO_CAND",
        "NOME_CANDIDATO",
    ],
    "candidate_cpf": ["NR_CPF_CANDIDATO", "CPF do candidato", "NUMERO_CNPJ_CANDIDATO"],
    "candidate_number": [
        "NR_CANDIDATO",
        "Numero candidato",
        "NR_CAND",
        "NUMERO_CANDIDATO",
    ],
    "position": ["DS_CARGO", "Descricao cargo", "DESCRICAO_CARGO", "Cargo"],
    "party_abbrev": [
        "SG_PARTIDO",
        "Sigla Partido",
        "Sigla  Partido",
        "SG_PART",
        "SIGLA_PARTIDO",
    ],
    "party_name": ["NM_PARTIDO", "Nome Partido"],
    "amount": ["VR_RECEITA", "Valor receita", "VALOR_RECEITA"],
    "description": ["DS_RECEITA", "Descricao receita", "TP_RECURSO", "TIPO_RECEITA"],
    "election_year": ["ANO_ELEICAO", "Ano eleicao"],
    "state": ["SG_UF", "UF", "SG_UE", "UNIDADE_ELEITORAL_CANDIDATO"],
    "donation_date": ["DT_RECEITA", "Data da receita", "DATA_RECEITA", "Data receita"],
}


def detect_encoding(csv_path: Path) -> str:
    """Detect file encoding: try utf-8, fallback to latin-1."""
    try:
        decoder = codecs.getincrementaldecoder("utf-8")()
        with csv_path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(64 * 1024), b""):
                decoder.decode(chunk)
            decoder.decode(b"", final=True)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _resolve_column(header: list[str], field_key: str) -> str | None:
    """Find the actual column name in the CSV header for a given field key."""
    aliases = _COLUMN_ALIASES.get(field_key, [])
    header_upper = [h.upper().strip() for h in header]
    for alias in aliases:
        alias_upper = alias.upper().strip()
        if alias_upper in header_upper:
            idx = header_upper.index(alias_upper)
            return header[idx]
    return None


def _safe_get(row: dict[str, str], header: list[str], field_key: str) -> str:
    """Get a value from a CSV row dict using column aliases."""
    col = _resolve_column(header, field_key)
    if col is None:
        return ""
    val = row.get(col, "").strip()
    return "" if val in ("", "nan", "NaN") else val


def _parse_amount(raw: str) -> float:
    """Parse a Brazilian-format decimal amount (comma as decimal separator)."""
    if not raw:
        return 0.0
    cleaned = raw.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_donation_date(raw: str) -> str:
    """Parse a TSE donation date string into ISO format (YYYY-MM-DD).

    Handles ``dd/MM/yyyy`` (common in TSE files) and ISO ``yyyy-MM-dd``.
    Returns empty string when the input cannot be parsed.
    """
    if not raw:
        return ""
    stripped = raw.strip()
    # Try dd/MM/yyyy first (most common in TSE CSVs)
    if "/" in stripped:
        parts = stripped.split("/")
        if len(parts) == 3 and len(parts[2]) == 4:
            try:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            except (ValueError, IndexError):
                return ""
    # Already ISO?
    if len(stripped) == 10 and stripped[4] == "-":
        return stripped
    return ""


def _iter_receitas_csv(csv_path: Path) -> Iterator[dict[str, Any]]:
    """Yield donation dicts one by one from a receitas CSV (';' separator).

    Generator approach avoids loading all records into memory at once.
    """
    encoding = detect_encoding(csv_path)
    with csv_path.open("r", encoding=encoding, newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";", quotechar='"')
        if reader.fieldnames is None:
            return

        header = list(reader.fieldnames)

        for row in reader:
            donor_name = _safe_get(row, header, "donor_name")
            donor_name_originator = _safe_get(row, header, "donor_name_originator")
            # Fallback: if donor_name is empty but originator exists
            # (can happen in old TSE formats where only NM_DOADOR_ORIGINARIO
            # is populated), use originator as the direct donor name.
            if not donor_name:
                if donor_name_originator:
                    donor_name = donor_name_originator
                else:
                    continue

            amount_raw = _safe_get(row, header, "amount")
            year_raw = _safe_get(row, header, "election_year")

            yield {
                "election_year_raw": year_raw,
                "state": _safe_get(row, header, "state"),
                "position": _safe_get(row, header, "position"),
                "candidate_name": _safe_get(row, header, "candidate_name"),
                "candidate_cpf": _safe_get(row, header, "candidate_cpf"),
                "candidate_number": _safe_get(row, header, "candidate_number"),
                "party_abbrev": _safe_get(row, header, "party_abbrev"),
                "party_name": _safe_get(row, header, "party_name"),
                "donor_name": donor_name,
                "donor_name_rfb": _safe_get(row, header, "donor_name_rfb"),
                "donor_name_originator": donor_name_originator,
                "donor_cpf_cnpj": _safe_get(row, header, "donor_cpf_cnpj"),
                "donor_cnae_code": _safe_get(row, header, "donor_cnae_code"),
                "donor_cnae_description": _safe_get(row, header, "donor_cnae_desc"),
                "donor_state": _safe_get(row, header, "donor_state"),
                "donation_amount_raw": amount_raw,
                "donation_date_raw": _safe_get(row, header, "donation_date"),
                "donation_description": _safe_get(row, header, "description"),
            }


def parse_receitas_csv(csv_path: Path) -> list[dict[str, Any]]:
    """Parse a receitas CSV with ';' separator and return raw dicts.

    Convenience wrapper that materializes the generator into a list.
    For memory-efficient iteration, use ``_iter_receitas_csv`` directly.
    """
    return list(_iter_receitas_csv(csv_path))


def normalize_donation_record(raw: dict[str, Any], year: int) -> dict[str, Any]:
    """Normalize a raw donation record into the canonical schema."""
    donor_name = raw.get("donor_name", "")
    donor_name_rfb = raw.get("donor_name_rfb", "")
    donor_name_originator = raw.get("donor_name_originator", "")

    return {
        "election_year": year,
        "state": raw.get("state", ""),
        "position": raw.get("position", ""),
        "candidate_name": raw.get("candidate_name", ""),
        "candidate_cpf": raw.get("candidate_cpf", ""),
        "candidate_number": raw.get("candidate_number", ""),
        "party_abbrev": raw.get("party_abbrev", ""),
        "party_name": raw.get("party_name", ""),
        "donor_name": donor_name,
        "donor_name_rfb": donor_name_rfb,
        "donor_name_originator": donor_name_originator,
        "donor_cpf_cnpj": raw.get("donor_cpf_cnpj", ""),
        "donor_name_normalized": normalize_entity_name(donor_name_rfb or donor_name),
        "donor_name_originator_normalized": normalize_entity_name(donor_name_originator) or "",
        "donation_amount": _parse_amount(raw.get("donation_amount_raw", "")),
        "donation_date": _parse_donation_date(raw.get("donation_date_raw", "")),
        "donation_description": raw.get("donation_description", ""),
        "donor_cnae_code": raw.get("donor_cnae_code", ""),
        "donor_cnae_description": raw.get("donor_cnae_description", ""),
        "donor_state": raw.get("donor_state", ""),
    }
