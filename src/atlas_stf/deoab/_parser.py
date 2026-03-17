"""Parse DEOAB gazette PDFs to extract law firm registration data.

Extracts OAB→sociedade links from four patterns found in DEOAB PDFs:
1. Detailed registration (smaller seccionais): full ementa with firm name + OAB
2. Compact registration (SP, large seccionais): Reg. nº + firm name + city/UF
3. Contract amendments: firm name + member names
4. Inline OAB references in disciplinary proceedings

Backend: pdftotext (poppler-utils) via subprocess.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OAB canonicalization for DEOAB-specific formats
# ---------------------------------------------------------------------------

# Pattern: OAB/UF NUMBER (standard)
_OAB_STANDARD_RE = re.compile(r"OAB/([A-Z]{2})\s+([\d.]+)")

# Pattern: OABUF–NUMBER or OABUF -NUMBER (AM, AP variants — UF glued)
_OAB_GLUED_RE = re.compile(r"OAB([A-Z]{2})\s*[–\-]\s*([\d.]+)")

# Pattern: OAB nº NUMBER UF
_OAB_NUMERO_RE = re.compile(r"OAB\s*n[ºo.]?\s*([\d.]+)\s*/?\s*([A-Z]{2})")

# Pattern: OAB UF NUMBER (space separated, no slash)
_OAB_SPACE_RE = re.compile(r"OAB\s+([A-Z]{2})\s+([\d.]+)")

_ALL_OAB_PATTERNS = [_OAB_STANDARD_RE, _OAB_GLUED_RE, _OAB_NUMERO_RE, _OAB_SPACE_RE]


def canonicalize_oab(text: str) -> list[tuple[str, str]]:
    """Extract and canonicalize all OAB numbers from text.

    Returns list of (number, uf) tuples with dots removed.
    E.g. ``OAB/SP 145.785`` → ``("145785", "SP")``,
         ``OABAM –20075`` → ``("20075", "AM")``.
    """
    results: list[tuple[str, str]] = []
    seen: set[str] = set()
    for pattern in _ALL_OAB_PATTERNS:
        for match in pattern.finditer(text):
            groups = match.groups()
            if pattern is _OAB_NUMERO_RE:
                # Groups are (number, uf) — reversed
                number_raw, uf = groups
            elif pattern is _OAB_STANDARD_RE or pattern is _OAB_GLUED_RE or pattern is _OAB_SPACE_RE:
                uf, number_raw = groups
            else:
                continue
            number = number_raw.replace(".", "").strip()
            if not number or not uf:
                continue
            key = f"{number}/{uf}"
            if key not in seen:
                seen.add(key)
                results.append((number, uf))
    return results


def format_oab(number: str, uf: str) -> str:
    """Format canonical OAB as ``NUMBER/UF``."""
    return f"{number}/{uf}"


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------


def extract_text_from_pdf(pdf_path: Path) -> str | None:
    """Extract text from PDF using pdftotext (poppler-utils).

    Returns None if pdftotext is not installed or extraction fails.
    """
    if not shutil.which("pdftotext"):
        logger.error("pdftotext not found — install poppler-utils")
        return None
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            logger.warning("pdftotext failed for %s: %s", pdf_path, result.stderr[:200])
            return None
        return result.stdout
    except subprocess.TimeoutExpired:
        logger.warning("pdftotext timed out for %s", pdf_path)
        return None


# ---------------------------------------------------------------------------
# Sociedade extraction patterns
# ---------------------------------------------------------------------------

# Detailed registration: Sociedade ... denominada "NOME"
# Covers ASCII quotes ("), typographic quotes (\u201c \u201d), and guillemets
_SOCIEDADE_DENOMINADA_RE = re.compile(
    r"[Ss]ociedade\s+(?:Individual\s+de\s+)?(?:Advocacia|Advogados)\s+denominada\s+"
    r'[\u201c\u201d"""]([^\u201c\u201d"""]+)[\u201c\u201d"""]',
    re.IGNORECASE,
)

# Compact registration: Reg. nº XXXXX - Nome Sociedade - Cidade/UF
_REGISTRO_COMPACTO_RE = re.compile(
    r"Reg\.\s*n[ºo]\s*(\d+)\s*[-–]\s*(.+?(?:Sociedade|Advogados)[^-–]*?)[-–]\s*([^-–\n]+/[A-Z]{2})",
    re.IGNORECASE,
)

# Titular pattern: advogado(a), NOME, OABUF-NUMERO
_TITULAR_RE = re.compile(
    r"advogado\s*\(a\)\s*,?\s*([^,]+?)\s*,\s*OAB",
    re.IGNORECASE,
)


def _clean_pdf_artifacts(text: str) -> str:
    """Remove common PDF artifacts: page headers, footers, digital signatures."""
    # Remove digital signature boilerplate
    text = re.sub(
        r"Documento assinado digitalmente conforme MP n[ºo]2\.200-2 de 24/08/2001,\s*"
        r"que\s+instituiu a Infraestrutura de Chaves P[uú]blicas Brasileira\s*-\s*ICP-Brasil",
        " ",
        text,
    )
    # Remove page headers (DIÁRIO ELETRÔNICO DA OAB ... | Pagina: N)
    text = re.sub(
        r"DI[AÁ]RIO ELETR[OÔ]NICO DA OAB\s+[^|]+\|\s*Pagina:\s*\d+",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    # Remove standalone page references
    text = re.sub(r"\|\s*Pagina:\s*\d+", " ", text)
    # Remove day/date headers that leak into text
    text = re.sub(
        r"(?:segunda|ter[çc]a|quarta|quinta|sexta|s[aá]bado|domingo)-feira,\s*\d+\s+de\s+\w+\s+de\s+\d{4}",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    return text


def parse_sociedade_records(text: str, source_url: str, pub_date: str) -> list[dict[str, Any]]:
    """Parse sociedade registration records from extracted PDF text.

    Returns a list of OAB→sociedade link records.
    """
    records: list[dict[str, Any]] = []
    # Clean PDF artifacts before normalizing
    text = _clean_pdf_artifacts(text)
    # Normalize whitespace for multi-line matching
    text_norm = re.sub(r"\n+", " ", text)
    text_norm = re.sub(r"\s+", " ", text_norm)

    # --- Pattern 1: Detailed registration ---
    for match in _SOCIEDADE_DENOMINADA_RE.finditer(text_norm):
        firm_name = match.group(1).strip()
        # Look for OAB in context (up to 500 chars after match)
        context = text_norm[match.start() : match.end() + 500]
        oabs = canonicalize_oab(context)
        # Look for titular name
        titular_match = _TITULAR_RE.search(context)
        titular_name = titular_match.group(1).strip() if titular_match else None

        for number, uf in oabs:
            records.append(
                {
                    "oab_number": format_oab(number, uf),
                    "advogado_nome": titular_name,
                    "sociedade_nome": firm_name,
                    "sociedade_tipo": "individual" if "individual" in firm_name.lower() else "plural",
                    "sociedade_registro": None,
                    "seccional": uf,
                    "cidade": None,
                    "tipo_ato": "registro",
                    "data_publicacao": pub_date,
                    "fonte": "DEOAB",
                    "fonte_url": source_url,
                    "confidence": 0.95,
                }
            )

    # --- Pattern 2: Compact registration ---
    for match in _REGISTRO_COMPACTO_RE.finditer(text_norm):
        reg_number = match.group(1).strip()
        firm_name = match.group(2).strip()
        city_uf = match.group(3).strip()
        parts = city_uf.rsplit("/", maxsplit=1)
        city = parts[0].strip() if len(parts) == 2 else None
        uf = parts[1].strip() if len(parts) == 2 else ""

        records.append(
            {
                "oab_number": None,
                "advogado_nome": None,
                "sociedade_nome": firm_name,
                "sociedade_tipo": "individual" if "individual" in firm_name.lower() else "plural",
                "sociedade_registro": reg_number,
                "seccional": uf,
                "cidade": city,
                "tipo_ato": "registro",
                "data_publicacao": pub_date,
                "fonte": "DEOAB",
                "fonte_url": source_url,
                "confidence": 0.85,
            }
        )

    return records
