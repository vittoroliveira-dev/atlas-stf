"""PDF text extraction and representation data extraction."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def extract_text_from_pdf(pdf_path: Path) -> str | None:
    """Extract text from PDF using pdfplumber if available.

    Returns None if pdfplumber not installed or extraction fails.
    """
    try:
        import pdfplumber  # type: ignore[import-untyped]
    except ImportError:
        logger.warning("pdfplumber not installed — skipping PDF extraction")
        return None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages_text: list[str] = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            return "\n".join(pages_text) if pages_text else None
    except Exception:
        logger.exception("Failed to extract text from %s", pdf_path)
        return None


def extract_representation_from_pdf(pdf_path: Path) -> dict[str, Any] | None:
    """Extract representation data from a PDF document.

    Returns dict with lawyer_name, oab_number, oab_state,
    firm_name, cnpj, party_name, or None if extraction fails.
    """
    text = extract_text_from_pdf(pdf_path)
    if not text:
        return None

    from ._parser import extract_cnpj_from_text, extract_oab_from_text, parse_procuracao_text

    result = parse_procuracao_text(text)

    # Enrich with regex extraction if procuracao parsing missed fields
    oabs = extract_oab_from_text(text)
    cnpjs = extract_cnpj_from_text(text)

    if oabs and not result.get("oab_number"):
        result["oab_number"] = oabs[0]["oab_number"]
        result["oab_state"] = oabs[0]["oab_state"]

    if cnpjs and not result.get("cnpj"):
        result["cnpj"] = cnpjs[0]

    return result
