"""Pure text-to-domain transformers. No I/O, no pandas dependency at module level."""

from __future__ import annotations

import math
import re
from typing import Any


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        import pandas as pd

        if value is pd.NA or value is pd.NaT:
            return True
    except Exception:
        pass
    return isinstance(value, float) and math.isnan(value)


def as_optional_str(value: Any) -> str | None:
    if is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def first_non_null(row: dict[str, Any], *columns: str) -> Any:
    for column in columns:
        value = row.get(column)
        if not is_missing(value):
            return value
    return None


def split_subjects(value: Any) -> list[str] | None:
    text = as_optional_str(value)
    if text is None:
        return None
    parts = [part.strip() for part in text.split("|")]
    normalized = [part for part in parts if part]
    return normalized or None


def infer_process_number(row: dict[str, Any]) -> str | None:
    processo_text = as_optional_str(row.get("processo"))
    if processo_text:
        return processo_text

    classe_text = as_optional_str(first_non_null(row, "classe", "classe_processo"))
    numero_text = as_optional_str(first_non_null(row, "no_do_processo", "numero", "numero_processo"))
    if classe_text and numero_text:
        return f"{classe_text} {numero_text}"

    processo_paradigma_text = as_optional_str(row.get("processo_paradigma"))
    if processo_paradigma_text:
        return processo_paradigma_text

    return None


def parse_bool_collegiate(value: Any) -> bool | None:
    if value is None:
        return None
    try:
        import pandas as pd

        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip().upper()
    if "COLEGIADA" in text:
        return True
    if "MONOCR" in text:
        return False
    return None


def parse_decision_year(value: Any) -> int | None:
    if value is None:
        return None
    try:
        import pandas as pd

        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return int(text) if text.isdigit() else None


def _normalize_name_for_dedup(value: Any) -> str | None:
    text = as_optional_str(value)
    if text is None:
        return None
    return re.sub(r"\s+", " ", text).upper()


def split_party_names(value: Any) -> list[str]:
    text = as_optional_str(value)
    if text is None:
        return []
    normalized = re.sub(r"\s+", " ", text).strip()
    parts = re.split(r"\s+(?:VS\.?|X)\s+", normalized, flags=re.IGNORECASE)
    cleaned = [part.strip(" ;,-") for part in parts if part.strip(" ;,-")]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in cleaned:
        key = _normalize_name_for_dedup(part)
        if key is None or key in seen:
            continue
        seen.add(key)
        deduped.append(part)
    return deduped


def split_name_list(value: Any) -> list[str]:
    text = as_optional_str(value)
    if text is None:
        return []
    parts = re.split(r"\s*[;|]\s*|\s+E OUTRO\(A/S\)\s+|\s+E OUTROS\(AS\)\s+", text, flags=re.IGNORECASE)
    cleaned = [part.strip() for part in parts if part.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in cleaned:
        key = _normalize_name_for_dedup(part)
        if key is None or key in seen:
            continue
        seen.add(key)
        deduped.append(part)
    return deduped


ROLE_LABEL_PATTERN = re.compile(r"([A-ZÇÁÉÍÓÚÃÕÜ0-9./()\-]{3,})\s*:\s*")
COUNSEL_ROLE_MARKERS = ("ADV", "PROC", "DEF")


def _parse_role_entries(value: Any) -> list[tuple[str, str]]:
    text = as_optional_str(value)
    if text is None:
        return []
    matches = list(ROLE_LABEL_PATTERN.finditer(text))
    if not matches:
        return []

    entries: list[tuple[str, str]] = []
    for index, match in enumerate(matches):
        label = re.sub(r"\s+", " ", match.group(1)).strip()
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        content = text[start:end].strip(" ;,-")
        if content:
            entries.append((label, content))
    return entries


def party_entries_from_juris_partes(value: Any) -> list[tuple[str | None, str]]:
    entries = _parse_role_entries(value)
    if not entries:
        return [(None, name) for name in split_party_names(value)]

    party_entries: list[tuple[str | None, str]] = []
    for label, content in entries:
        if any(marker in label for marker in COUNSEL_ROLE_MARKERS):
            continue
        party_entries.append((label, content))
    return party_entries


def counsel_entries_from_juris_partes(value: Any) -> list[tuple[str | None, str, str | None]]:
    entries = _parse_role_entries(value)
    if not entries:
        return []

    counsel_entries: list[tuple[str | None, str, str | None]] = []
    current_party_role: str | None = None
    for label, content in entries:
        if any(marker in label for marker in COUNSEL_ROLE_MARKERS):
            for name in split_name_list(content):
                counsel_entries.append((label, name, current_party_role))
            continue
        current_party_role = label
    return counsel_entries
