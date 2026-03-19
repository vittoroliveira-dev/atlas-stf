"""HTML parsers for OAB/SP society search and detail pages."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from ._config import PARSER_VERSION

# Pre-compiled patterns
_PARAM_RE = re.compile(r"consultaSociedades03\.asp\?param=(\d+)")
_NOT_FOUND_RE = re.compile(r"N[ãa]o\s+h[áa]\s+resultados", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_CEP_DIGITS_RE = re.compile(r"\d+")


def extract_param_from_search(html: str) -> tuple[str | None, str]:
    """Extract param ID from search results HTML.

    Returns (param_id, status) where status is one of:
    - "found": link to detail page found
    - "not_found": official "no results" message
    - "unexpected": neither link nor "no results" found
    """
    match = _PARAM_RE.search(html)
    if match:
        return match.group(1), "found"
    if _NOT_FOUND_RE.search(html):
        return None, "not_found"
    return None, "unexpected"


def _clean_text(raw: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    text = _TAG_RE.sub(" ", raw)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    return _WS_RE.sub(" ", text).strip()


def _extract_after_label(html: str, label: str) -> str | None:
    """Extract value from the next table cell after the cell containing the label.

    OAB/SP HTML pattern:
      <td><label>KEY:</label></td><td><label>VALUE</label></td>

    Strategy: find the label text, skip to the next <td>, extract inner content.
    """
    idx = html.find(label)
    if idx == -1:
        return None
    after = html[idx + len(label) :]
    # Skip to the next <td> opening tag (value cell)
    td_open = re.search(r"<td[^>]*>", after, re.IGNORECASE)
    if not td_open:
        return None
    cell_start = td_open.end()
    # Find the matching </td>
    td_close = re.search(r"</td>", after[cell_start:], re.IGNORECASE)
    if not td_close:
        return None
    cell_content = after[cell_start : cell_start + td_close.start()]
    cleaned = _clean_text(cell_content)
    return cleaned if cleaned else None


def _extract_firm_name(html: str) -> str | None:
    """Extract firm name from the boxCampo div's bold tag."""
    match = re.search(r'class="boxCampo"[^>]*>.*?<b>(.*?)</b>', html, re.DOTALL | re.IGNORECASE)
    if match:
        return _clean_text(match.group(1))
    return None


def _extract_society_type(html: str) -> str:
    """Extract society type from the last bold label in the table."""
    matches = re.findall(r"<label[^>]*><b>([^<]+)</b></label>", html, re.IGNORECASE)
    for m in reversed(matches):
        cleaned = _clean_text(m)
        if "individual" in cleaned.lower():
            return "individual"
        if "advogados" in cleaned.lower():
            return "sociedade_advogados"
    return "sociedade_advogados"


def _normalize_zip(raw: str | None) -> str | None:
    """Extract only digits from CEP."""
    if not raw:
        return None
    digits = "".join(_CEP_DIGITS_RE.findall(raw))
    return digits if digits else None


def _extract_city_state(html: str) -> tuple[str | None, str]:
    """Extract city and state from the combined field."""
    raw = _extract_after_label(html, "Cidade / Estado")
    if not raw:
        return None, "SP"
    # Format is typically "City / UF"
    parts = raw.split("/")
    city = parts[0].strip() if parts else None
    state = parts[1].strip() if len(parts) > 1 else "SP"
    return city or None, state or "SP"


def parse_society_detail(html: str, registration_number: str) -> dict[str, Any] | None:
    """Parse society detail HTML into a structured record.

    Returns None if the page has no detail table (invalid param).
    """
    # Check if the page has content (a table with labels)
    if "Nº de Registro" not in html and "N° de Registro" not in html and "Registro" not in html:
        return None

    firm_name = _extract_firm_name(html)
    if not firm_name:
        return None

    # Extract registration number from the page itself for validation
    page_reg = _extract_after_label(html, "Registro")
    if not page_reg:
        page_reg = _extract_after_label(html, "Registro:")
    if page_reg:
        # Clean to just digits
        page_reg = "".join(c for c in page_reg if c.isdigit())

    address = _extract_after_label(html, "Endere")
    if address:
        address = address.replace("<br>", "\n").replace("<BR>", "\n")
        address = _clean_text(address)

    neighborhood = _extract_after_label(html, "Bairro")
    zip_code = _normalize_zip(_extract_after_label(html, "CEP"))
    city, state = _extract_city_state(html)
    email = _extract_after_label(html, "Email")
    phone = _extract_after_label(html, "Telefone")
    society_type = _extract_society_type(html)

    return {
        "registration_number": page_reg or registration_number,
        "oab_sp_param": "",  # will be filled by caller
        "firm_name": firm_name,
        "address": address or None,
        "neighborhood": neighborhood or None,
        "zip_code": zip_code,
        "city": city,
        "state": state,
        "email": email or None,
        "phone": phone or None,
        "society_type": society_type,
        "detail_url": "",  # will be filled by caller
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "parser_version": PARSER_VERSION,
    }
