"""Schema signature helpers: normalise headers and produce layout fingerprints."""

from __future__ import annotations

import hashlib
import unicodedata


def normalize_header_value(raw: str) -> str:
    """Normalize a single header value: NFKD, lowercase, strip, collapse spaces."""
    return " ".join(unicodedata.normalize("NFKD", raw).lower().split())


def normalize_header_for_signature(header: list[str]) -> str:
    joined = "|".join(normalize_header_value(v) for v in header)
    return hashlib.sha256(joined.encode()).hexdigest()
