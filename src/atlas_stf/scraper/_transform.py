"""Text cleaning for jurisprudência records."""

from __future__ import annotations

import html
import re

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_WS_RE = re.compile(r"\s+")


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, collapsing whitespace."""
    if not text:
        return text
    text = html.unescape(text)
    text = _HTML_TAG_RE.sub(" ", text)
    text = _MULTI_WS_RE.sub(" ", text)
    return text.strip()


def clean_record(record: dict, text_fields: tuple[str, ...]) -> dict:
    """Apply strip_html to specified text fields in a record."""
    for field in text_fields:
        value = record.get(field)
        if isinstance(value, str):
            record[field] = strip_html(value)
    return record
