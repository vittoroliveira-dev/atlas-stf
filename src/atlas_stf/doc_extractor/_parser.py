"""Pure parsing functions for text extracted from PDFs."""

from __future__ import annotations

import re
from typing import Any

# OAB patterns in text:
# Format A: "OAB/SP 123456" — UF before number
_OAB_TEXT_A_RE = re.compile(
    r"OAB\s*[/]\s*([A-Z]{2})\s+(\d[\d.]{0,6})",
    re.IGNORECASE,
)
# Format B: "OAB nº 123.456 RJ" or "OAB 123456-SP" — number before UF
_OAB_TEXT_B_RE = re.compile(
    r"OAB\s*(?:n[°ºo]\.?\s*)?(\d[\d.]{0,6})\s*[/-]?\s*([A-Z]{2})",
    re.IGNORECASE,
)

# CNPJ pattern: 12.345.678/0001-90
_CNPJ_TEXT_RE = re.compile(r"(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})")

# CPF pattern: 123.456.789-00
_CPF_TEXT_RE = re.compile(r"(\d{3}\.\d{3}\.\d{3}-\d{2})")

# Common header markers for lawyer names in procuracoes
_PROCURACAO_HEADER_RE = re.compile(
    r"(?:constitui|nomeia|outorga)\s+(?:como\s+)?(?:seu\s+)?(?:procurador|advogado)\s+"
    r"(?:o\s+(?:Dr\.\s*|Doutor\s*)?)?([A-Z][A-Z\s]+?)(?:\s*,|\s+(?:inscrit|para\b|OAB|do\s+escrit))",
    re.IGNORECASE,
)

# Firm name pattern: typically after "do escritorio" or "sócio de"
_FIRM_NAME_RE = re.compile(
    r"(?:escrit[oó]rio|sociedade de advogados?)\s+([A-Z][A-Z\s,&.]+?)(?:\s*,|\s*CNPJ|\s*inscrit|\.|$)",
    re.IGNORECASE,
)

# Party represented pattern
_PARTY_REPRESENTED_RE = re.compile(
    r"(?:outorgante|mandante|constituinte)[:\s]+([A-Z][A-Z\s]+?)(?:\s*,|\s*(?:inscrit|CPF|CNPJ|portador))",
    re.IGNORECASE,
)

# Petition header: "EXCELENTISSIMO ... PETIÇÃO INICIAL / RECURSO ..."
_PETITION_TYPE_RE = re.compile(
    r"\b(PETI[CÇ][AÃ]O\s+INICIAL|RECURSO\s+(?:EXTRAORDIN[AÁ]RIO|ORDIN[AÁ]RIO|ESPECIAL)"
    r"|A[CÇ][AÃ]O\s+DIRETA|MANDADO\s+DE\s+SEGURAN[CÇ]A|HABEAS\s+CORPUS"
    r"|RECLAMA[CÇ][AÃ]O|AGRAVO|EMBARGOS?)\b",
    re.IGNORECASE,
)

# Petitioner pattern in petition header
_PETITIONER_RE = re.compile(
    r"(?:RECORRENTE|IMPETRANTE|REQUERENTE|AUTOR|RECLAMANTE)[:\s]+([A-Z][A-Z\s]+?)(?:\s*$|\s*\n|\s*,)",
    re.IGNORECASE | re.MULTILINE,
)


def extract_oab_from_text(text: str) -> list[dict[str, str]]:
    """Extract OAB numbers from free text. Returns list of {oab_number, oab_state}."""
    if not text:
        return []
    results: list[dict[str, str]] = []
    seen: set[str] = set()
    # Format A: OAB/SP 123456
    for match in _OAB_TEXT_A_RE.finditer(text):
        state = match.group(1).upper()
        number = match.group(2).replace(".", "")
        key = f"{number}-{state}"
        if key not in seen:
            seen.add(key)
            results.append({"oab_number": number, "oab_state": state})
    # Format B: OAB nº 123.456 RJ
    for match in _OAB_TEXT_B_RE.finditer(text):
        number = match.group(1).replace(".", "")
        state = match.group(2).upper()
        key = f"{number}-{state}"
        if key not in seen:
            seen.add(key)
            results.append({"oab_number": number, "oab_state": state})
    return results


def extract_cnpj_from_text(text: str) -> list[str]:
    """Extract CNPJ numbers from free text."""
    if not text:
        return []
    seen: set[str] = set()
    results: list[str] = []
    for match in _CNPJ_TEXT_RE.finditer(text):
        cnpj = match.group(1)
        if cnpj not in seen:
            seen.add(cnpj)
            results.append(cnpj)
    return results


def extract_cpf_from_text(text: str) -> list[str]:
    """Extract CPF numbers from free text."""
    if not text:
        return []
    seen: set[str] = set()
    results: list[str] = []
    for match in _CPF_TEXT_RE.finditer(text):
        cpf = match.group(1)
        if cpf not in seen:
            seen.add(cpf)
            results.append(cpf)
    return results


def extract_lawyer_names_from_header(text: str) -> list[str]:
    """Extract lawyer names from procuracao/petition headers."""
    if not text:
        return []
    results: list[str] = []
    seen: set[str] = set()
    for match in _PROCURACAO_HEADER_RE.finditer(text):
        name = match.group(1).strip()
        normalized = " ".join(name.split())
        if normalized and normalized not in seen:
            seen.add(normalized)
            results.append(normalized)
    return results


def parse_procuracao_text(text: str) -> dict[str, Any]:
    """Parse procuracao text to extract structured representation data.

    Returns dict with keys: lawyer_name, oab_number, oab_state,
    firm_name, cnpj, party_represented.
    """
    result: dict[str, Any] = {
        "lawyer_name": None,
        "oab_number": None,
        "oab_state": None,
        "firm_name": None,
        "cnpj": None,
        "party_represented": None,
    }

    if not text:
        return result

    # Extract lawyer name from header
    lawyer_names = extract_lawyer_names_from_header(text)
    if lawyer_names:
        result["lawyer_name"] = lawyer_names[0]

    # Extract OAB
    oabs = extract_oab_from_text(text)
    if oabs:
        result["oab_number"] = oabs[0]["oab_number"]
        result["oab_state"] = oabs[0]["oab_state"]

    # Extract firm name
    firm_match = _FIRM_NAME_RE.search(text)
    if firm_match:
        result["firm_name"] = " ".join(firm_match.group(1).strip().split())

    # Extract CNPJ
    cnpjs = extract_cnpj_from_text(text)
    if cnpjs:
        result["cnpj"] = cnpjs[0]

    # Extract party represented
    party_match = _PARTY_REPRESENTED_RE.search(text)
    if party_match:
        result["party_represented"] = " ".join(party_match.group(1).strip().split())

    return result


def parse_petition_header(text: str) -> dict[str, Any]:
    """Parse petition header to extract: petitioner_name, document_type, oab.

    Returns dict with keys: petitioner_name, document_type, oab_number, oab_state.
    """
    result: dict[str, Any] = {
        "petitioner_name": None,
        "document_type": None,
        "oab_number": None,
        "oab_state": None,
    }

    if not text:
        return result

    # Extract document type
    doc_match = _PETITION_TYPE_RE.search(text)
    if doc_match:
        result["document_type"] = doc_match.group(1).upper()

    # Extract petitioner name
    pet_match = _PETITIONER_RE.search(text)
    if pet_match:
        result["petitioner_name"] = " ".join(pet_match.group(1).strip().split())

    # Extract OAB from petition body
    oabs = extract_oab_from_text(text)
    if oabs:
        result["oab_number"] = oabs[0]["oab_number"]
        result["oab_state"] = oabs[0]["oab_state"]

    return result
