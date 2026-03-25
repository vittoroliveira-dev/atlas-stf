"""Entity identity primitives: classify identifier types, quality, and priority.

NOT full entity resolution — just the minimal base to distinguish:
- explicit identifier (CPF/CNPJ validated)
- extracted identifier (tax ID parsed but not validated)
- normalized name (string normalization applied)
- fuzzy match candidate (name requiring approximate matching)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from ..core.identity import (
    is_valid_cnpj,
    is_valid_cpf,
    normalize_entity_name,
    normalize_tax_id,
)


class IdentifierType(str, Enum):
    """Type of entity identifier."""

    CNPJ_VALIDATED = "cnpj_validated"
    CPF_VALIDATED = "cpf_validated"
    TAX_ID_UNVALIDATED = "tax_id_unvalidated"
    TAX_ID_MASKED = "tax_id_masked"
    NAME_NORMALIZED = "name_normalized"
    NAME_RAW = "name_raw"
    PROCESS_NUMBER = "process_number"
    INTERNAL_ID = "internal_id"


class IdentifierQuality(str, Enum):
    """Quality assessment of an identifier for matching purposes."""

    DETERMINISTIC = "deterministic"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class EntityIdentifier:
    """Classified entity identifier with quality assessment."""

    raw_value: str
    normalized_value: str
    identifier_type: IdentifierType
    quality: IdentifierQuality
    source_column: str
    source_file: str


# RFB masked CPF pattern: ***123456** (2-3 asterisks + 6 digits + 2 asterisks)
_MASKED_TAX_ID_RE = re.compile(r"^\*{2,3}\d{6}\*{2}$")
_FORMATTING_RE = re.compile(r"[.\-/\s]")


def classify_identifier(
    value: str,
    source_column: str = "",
    source_file: str = "",
) -> EntityIdentifier:
    """Classify an identifier value into type and quality.

    Determines the strongest classification possible for a raw value:
    validated tax IDs first, then unvalidated numeric IDs, then names.
    """

    def _make(
        raw: str,
        normalized: str,
        id_type: IdentifierType,
        quality: IdentifierQuality,
    ) -> EntityIdentifier:
        return EntityIdentifier(raw, normalized, id_type, quality, source_column, source_file)

    if not value or not value.strip():
        return _make(value or "", "", IdentifierType.NAME_RAW, IdentifierQuality.BLOCKED)

    stripped = value.strip()

    # Check masked pattern before digit extraction (masks contain asterisks)
    if _MASKED_TAX_ID_RE.match(stripped):
        return _make(stripped, stripped, IdentifierType.TAX_ID_MASKED, IdentifierQuality.BLOCKED)

    # Tax ID path: strip formatting, check if all digits
    digits_only = _FORMATTING_RE.sub("", stripped)
    if digits_only.isdigit() and len(digits_only) >= 8:
        normalized = normalize_tax_id(stripped)
        if normalized and len(normalized) == 14 and is_valid_cnpj(normalized):
            return _make(stripped, normalized, IdentifierType.CNPJ_VALIDATED, IdentifierQuality.DETERMINISTIC)
        if normalized and len(normalized) == 11 and is_valid_cpf(normalized):
            return _make(stripped, normalized, IdentifierType.CPF_VALIDATED, IdentifierQuality.DETERMINISTIC)
        if normalized:
            return _make(stripped, normalized, IdentifierType.TAX_ID_UNVALIDATED, IdentifierQuality.MEDIUM)

    # Name path
    name_normalized = normalize_entity_name(stripped)
    if name_normalized:
        return _make(stripped, name_normalized, IdentifierType.NAME_NORMALIZED, IdentifierQuality.MEDIUM)

    return _make(stripped, stripped, IdentifierType.NAME_RAW, IdentifierQuality.LOW)


# ---------------------------------------------------------------------------
# Priority and join key helpers
# ---------------------------------------------------------------------------

_IDENTIFIER_PRIORITY: dict[IdentifierType, int] = {
    IdentifierType.CNPJ_VALIDATED: 1,
    IdentifierType.CPF_VALIDATED: 2,
    IdentifierType.TAX_ID_UNVALIDATED: 3,
    IdentifierType.PROCESS_NUMBER: 4,
    IdentifierType.INTERNAL_ID: 5,
    IdentifierType.NAME_NORMALIZED: 6,
    IdentifierType.TAX_ID_MASKED: 7,
    IdentifierType.NAME_RAW: 8,
}


def best_identifier(identifiers: list[EntityIdentifier]) -> EntityIdentifier | None:
    """Return the highest-priority non-blocked identifier, or None."""
    candidates = [i for i in identifiers if i.quality != IdentifierQuality.BLOCKED]
    if not candidates:
        return None
    return min(candidates, key=lambda i: _IDENTIFIER_PRIORITY.get(i.identifier_type, 99))


def canonical_join_key(identifier: EntityIdentifier) -> str | None:
    """Produce a canonical string for join operations, or None if blocked."""
    if identifier.quality == IdentifierQuality.BLOCKED:
        return None
    id_type = identifier.identifier_type
    if id_type in {IdentifierType.CNPJ_VALIDATED, IdentifierType.CPF_VALIDATED, IdentifierType.TAX_ID_UNVALIDATED}:
        return f"taxid:{identifier.normalized_value}"
    if id_type == IdentifierType.PROCESS_NUMBER:
        return f"proc:{identifier.normalized_value}"
    if id_type in {IdentifierType.NAME_NORMALIZED, IdentifierType.NAME_RAW}:
        return f"name:{identifier.normalized_value}"
    return None
