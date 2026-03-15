"""Parse STF portal process pages into structured data.

The STF portal (portal.stf.jus.br) uses ASP.NET server-rendered HTML.
This module extracts structured data from the HTML response.

The portal exposes process data across several tabs:
- Informações: process metadata (class, rapporteur, origin)
- Andamentos: procedural timeline events
- Deslocamentos: redistributions and transfers
- Petições: filed documents/petitions
- Sessão Virtual: virtual plenary session results

The parsing strategy is determined by probing (Phase 2B):
- If JSON endpoints exist → parse JSON directly
- If HTML-only → parse HTML tables and divs
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _clean_text(text: str | None) -> str | None:
    """Strip HTML tags and normalize whitespace."""
    if not text:
        return None
    cleaned = re.sub(r"<[^>]+>", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _parse_date(text: str | None) -> str | None:
    """Extract a date in YYYY-MM-DD from various PT-BR formats."""
    if not text:
        return None
    text = text.strip()

    # Try DD/MM/YYYY
    match = re.search(r"(\d{2})/(\d{2})/(\d{4})", text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"

    # Try YYYY-MM-DD already
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return match.group(0)

    return None


def parse_andamentos_html(html: str) -> list[dict[str, Any]]:
    """Parse andamentos (procedural events) from HTML.

    Expected structure: table or div list with date + description pairs.
    Returns list of {date, description, detail} dicts.
    """
    events: list[dict[str, Any]] = []

    # Pattern: rows with date and description cells
    # The exact selectors will be refined after Phase 2B probing
    row_pattern = re.compile(
        r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>(?:\s*<td[^>]*>(.*?)</td>)?",
        re.DOTALL | re.IGNORECASE,
    )

    for match in row_pattern.finditer(html):
        date_text = _clean_text(match.group(1))
        desc_text = _clean_text(match.group(2))
        detail_text = _clean_text(match.group(3)) if match.group(3) else None

        parsed_date = _parse_date(date_text)
        if not parsed_date or not desc_text:
            continue

        events.append({
            "date": parsed_date,
            "description": desc_text,
            "detail": detail_text,
            "tab_name": "Andamentos",
        })

    return events


def parse_deslocamentos_html(html: str) -> list[dict[str, Any]]:
    """Parse deslocamentos (transfers/redistributions) from HTML.

    Returns list of {date, origin, destination, reason} dicts.
    """
    events: list[dict[str, Any]] = []

    row_pattern = re.compile(
        r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>"
        r"\s*<td[^>]*>(.*?)</td>(?:\s*<td[^>]*>(.*?)</td>)?",
        re.DOTALL | re.IGNORECASE,
    )

    for match in row_pattern.finditer(html):
        date_text = _clean_text(match.group(1))
        origin = _clean_text(match.group(2))
        destination = _clean_text(match.group(3))
        reason = _clean_text(match.group(4)) if match.group(4) else None

        parsed_date = _parse_date(date_text)
        if not parsed_date:
            continue

        events.append({
            "date": parsed_date,
            "origin": origin,
            "destination": destination,
            "reason": reason,
            "tab_name": "Deslocamentos",
        })

    return events


def parse_peticoes_html(html: str) -> list[dict[str, Any]]:
    """Parse petições (filed documents) from HTML.

    Returns list of {date, type, protocol} dicts.
    """
    events: list[dict[str, Any]] = []

    row_pattern = re.compile(
        r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>"
        r"(?:\s*<td[^>]*>(.*?)</td>)?",
        re.DOTALL | re.IGNORECASE,
    )

    for match in row_pattern.finditer(html):
        date_text = _clean_text(match.group(1))
        doc_type = _clean_text(match.group(2))
        protocol = _clean_text(match.group(3)) if match.group(3) else None

        parsed_date = _parse_date(date_text)
        if not parsed_date:
            continue

        events.append({
            "date": parsed_date,
            "type": doc_type,
            "protocol": protocol,
            "tab_name": "Petições",
        })

    return events


def parse_sessao_virtual_html(html: str) -> list[dict[str, Any]]:
    """Parse sessão virtual (virtual plenary sessions) from HTML.

    Returns list of {start_date, end_date, result} dicts.
    """
    events: list[dict[str, Any]] = []

    row_pattern = re.compile(
        r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>"
        r"\s*<td[^>]*>(.*?)</td>",
        re.DOTALL | re.IGNORECASE,
    )

    for match in row_pattern.finditer(html):
        start_text = _clean_text(match.group(1))
        end_text = _clean_text(match.group(2))
        result_text = _clean_text(match.group(3))

        start_date = _parse_date(start_text)
        end_date = _parse_date(end_text)
        if not start_date:
            continue

        events.append({
            "start_date": start_date,
            "end_date": end_date,
            "result": result_text,
            "tab_name": "Sessão Virtual",
        })

    return events


def parse_informacoes_html(html: str) -> dict[str, Any]:
    """Parse informações (process metadata) from HTML.

    Returns dict with class, rapporteur, judging body, origin, prevention.
    """
    info: dict[str, Any] = {"tab_name": "Informações"}

    # Common pattern: label/value pairs in definition lists or tables
    # These selectors will be refined after probing
    label_value_pattern = re.compile(
        r"<(?:dt|th|label)[^>]*>(.*?)</(?:dt|th|label)>\s*"
        r"<(?:dd|td|span)[^>]*>(.*?)</(?:dd|td|span)>",
        re.DOTALL | re.IGNORECASE,
    )

    field_map: dict[str, str] = {
        "classe": "classe",
        "relator": "relator_atual",
        "órgão julgador": "orgao_julgador",
        "orgao julgador": "orgao_julgador",
        "origem": "origem",
        "procedência": "origem",
        "procedencia": "origem",
        "prevenção": "prevencao",
        "prevencao": "prevencao",
    }

    for match in label_value_pattern.finditer(html):
        label = _clean_text(match.group(1))
        value = _clean_text(match.group(2))
        if not label or not value:
            continue
        label_lower = label.lower()
        for key, field_name in field_map.items():
            if key in label_lower and field_name not in info:
                info[field_name] = value
                break

    return info


def build_process_document(
    process_number: str,
    source_url: str,
    raw_html: str,
    andamentos: list[dict[str, Any]],
    deslocamentos: list[dict[str, Any]],
    peticoes: list[dict[str, Any]],
    sessao_virtual: list[dict[str, Any]],
    informacoes: dict[str, Any],
) -> dict[str, Any]:
    """Assemble a complete process document with provenance metadata."""
    return {
        "process_number": process_number,
        "source_system": "stf_portal",
        "source_url": source_url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "raw_html_hash": _sha256(raw_html),
        "andamentos": andamentos,
        "deslocamentos": deslocamentos,
        "peticoes": peticoes,
        "sessao_virtual": sessao_virtual,
        "informacoes": informacoes,
    }
