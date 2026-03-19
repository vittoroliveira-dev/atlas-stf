"""HTML parser for OAB/SP inscritos (lawyer) search results."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

_NOT_FOUND_RE = re.compile(r"N[ãa]o\s+h[áa]\s+resultados", re.IGNORECASE)
_PARAM_RE = re.compile(r"consultaSociedades03\.asp\?param=(\d+)")
_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
_STRONG_RE = re.compile(r"<strong[^>]*>(.*?)</strong>", re.DOTALL | re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _clean(raw: str) -> str:
    text = _TAG_RE.sub(" ", raw)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    return _WS_RE.sub(" ", text).strip()


def classify_inscritos_response(html: str) -> tuple[str, list[dict[str, Any]]]:
    """Classify the search response and extract records.

    Returns (status, records) where status is one of:
    - "not_found": no results
    - "single_match": exactly 1 result
    - "multi_match": multiple results (rejected)
    - "unexpected": unrecognizable response
    """
    if _NOT_FOUND_RE.search(html):
        return "not_found", []

    # Find the results table
    table_match = re.search(
        r"Resultado da pesquisa.*?<table[^>]*>(.*?)</table>",
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if not table_match:
        # Maybe there's no table but also no "not found" message
        if "Resultado da pesquisa" in html:
            return "not_found", []
        return "unexpected", []

    table_html = table_match.group(1)
    rows = _ROW_RE.findall(table_html)

    records: list[dict[str, Any]] = []
    for row_html in rows:
        record = _parse_inscrito_row(row_html)
        if record:
            records.append(record)

    if len(records) == 0:
        return "unexpected", []
    if len(records) == 1:
        return "single_match", records
    return "multi_match", records


def _extract_after_label(row_html: str, label: str) -> str | None:
    """Extract text content after a <strong>label...</strong> tag.

    Matches partial label text, skips the rest of the <strong> tag,
    then captures until the next <strong> or end of content.
    """
    pattern = re.compile(
        re.escape(label) + r"[^<]*</strong>\s*(.*?)(?=<strong|</?td|</?tr|$)",
        re.DOTALL | re.IGNORECASE,
    )
    match = pattern.search(row_html)
    if match:
        cleaned = _clean(match.group(1))
        return cleaned if cleaned else None
    return None


def _parse_inscrito_row(row_html: str) -> dict[str, Any] | None:
    """Parse a single result row into a structured record."""
    # Extract name (first <strong> that isn't a label)
    strongs = _STRONG_RE.findall(row_html)
    if not strongs:
        return None

    name = _clean(strongs[0])
    if not name or ":" in name:
        # It's a label, not a name
        return None

    # Extract OAB number
    oab_raw = _extract_after_label(row_html, "OAB SP n")
    oab_number = None
    oab_type = None
    if oab_raw:
        # Format: "100000 - Definitivo"
        parts = oab_raw.split("-", 1)
        oab_number = parts[0].strip()
        if len(parts) > 1:
            oab_type = parts[1].strip()

    # Extract other fields
    inscription_date = _extract_after_label(row_html, "Data Inscri")
    subsection = _extract_after_label(row_html, "Subse")
    situation = _extract_after_label(row_html, "Situa")

    # Extract "Sócio de" — optional field
    firm_name = None
    firm_param = None
    socio_match = re.search(
        r"S[óo]cio\s+de:</strong>\s*<a[^>]*href=[^>]*param=(\d+)[^>]*>([^<]+)</a>",
        row_html,
        re.IGNORECASE,
    )
    if socio_match:
        firm_param = socio_match.group(1)
        firm_name = _clean(socio_match.group(2))
    else:
        # Try without link (just text)
        socio_text = _extract_after_label(row_html, "cio de:")
        if socio_text:
            firm_name = socio_text

    return {
        "lawyer_name": name,
        "oab_number": oab_number,
        "oab_type": oab_type,
        "inscription_date": inscription_date,
        "subsection": subsection,
        "situation": situation,
        "firm_name": firm_name,
        "firm_param": firm_param,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
