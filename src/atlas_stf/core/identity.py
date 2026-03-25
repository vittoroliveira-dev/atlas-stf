"""Deterministic identity generation and entity normalization."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any

from .parsers import as_optional_str

_NON_WORD_RE = re.compile(r"[^\w\s]+", flags=re.UNICODE)
_WHITESPACE_RE = re.compile(r"\s+")
_CORPORATE_SUFFIX_RE = re.compile(r"\s+(?:S\.?\s*A\.?|S/A|SA|S/S)\s*$")
_CORPORATE_SUFFIX_TOKENS = {
    "SA",
    "LTDA",
    "LTD",
    "ME",
    "EPP",
    "EIRELI",
    "EI",
    "SS",
}
_INITIAL_MATCH_BLOCKLIST = {"A", "E", "O"}


def strip_accents(text: str) -> str:
    """Remove diacritical marks, preserving base characters."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def stable_id(prefix: str, value: str, length: int = 16) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{prefix}{digest[:length]}"


def normalize_entity_name(value: Any) -> str | None:
    text = as_optional_str(value)
    if text is None:
        return None
    return _WHITESPACE_RE.sub(" ", strip_accents(text)).upper()


def normalize_tax_id(value: Any) -> str | None:
    text = as_optional_str(value)
    if text is None:
        return None
    digits = re.sub(r"\D+", "", text)
    return digits or None


def is_valid_cpf(value: Any) -> bool:
    digits = normalize_tax_id(value)
    if digits is None or len(digits) != 11 or len(set(digits)) == 1:
        return False
    total = sum(int(digits[idx]) * (10 - idx) for idx in range(9))
    first_check = (total * 10 % 11) % 10
    if first_check != int(digits[9]):
        return False
    total = sum(int(digits[idx]) * (11 - idx) for idx in range(10))
    second_check = (total * 10 % 11) % 10
    return second_check == int(digits[10])


def is_valid_cnpj(value: Any) -> bool:
    digits = normalize_tax_id(value)
    if digits is None or len(digits) != 14 or len(set(digits)) == 1:
        return False

    def _check_digit(base: str, weights: list[int]) -> int:
        total = sum(int(char) * weight for char, weight in zip(base, weights, strict=False))
        remainder = total % 11
        return 0 if remainder < 2 else 11 - remainder

    first_weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    second_weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    first_check = _check_digit(digits[:12], first_weights)
    if first_check != int(digits[12]):
        return False
    second_check = _check_digit(digits[:13], second_weights)
    return second_check == int(digits[13])


def canonicalize_entity_name(value: Any) -> str | None:
    normalized = normalize_entity_name(value)
    if normalized is None:
        return None
    cleaned = _CORPORATE_SUFFIX_RE.sub("", normalized).strip()
    if not cleaned:
        cleaned = normalized
    stripped = _WHITESPACE_RE.sub(" ", _NON_WORD_RE.sub(" ", cleaned)).strip()
    if not stripped:
        return None
    tokens = [token for token in stripped.split(" ") if token]
    simplified = [token for token in tokens if token not in _CORPORATE_SUFFIX_TOKENS]
    return " ".join(simplified or tokens)


def build_identity_key(
    value: Any,
    *,
    entity_tax_id: Any = None,
    canonical_name: Any = None,
) -> str | None:
    tax_id = normalize_tax_id(entity_tax_id)
    if tax_id:
        return f"tax:{tax_id}"
    canonical = canonicalize_entity_name(canonical_name if canonical_name is not None else value)
    if canonical:
        return f"name:{canonical}"
    return None


def _tokenize_for_similarity(value: Any) -> list[str]:
    canonical = canonicalize_entity_name(value)
    if canonical is None:
        return []
    # canonicalize_entity_name already strips accents via normalize_entity_name
    return [token for token in canonical.split(" ") if token]


def _tokens_match(left: str, right: str) -> bool:
    if left == right:
        return True
    if len(left) == 1 and left not in _INITIAL_MATCH_BLOCKLIST and right.startswith(left):
        return True
    if len(right) == 1 and right not in _INITIAL_MATCH_BLOCKLIST and left.startswith(right):
        return True
    return False


def jaccard_similarity(left: Any, right: Any) -> float:
    left_tokens = _tokenize_for_similarity(left)
    right_tokens = _tokenize_for_similarity(right)
    if not left_tokens or not right_tokens:
        return 0.0

    smaller = left_tokens if len(left_tokens) <= len(right_tokens) else right_tokens
    larger = right_tokens if smaller is left_tokens else left_tokens
    remaining = larger.copy()
    matches = 0
    for token in smaller:
        for idx, candidate in enumerate(remaining):
            if _tokens_match(token, candidate):
                matches += 1
                remaining.pop(idx)
                break
    union_size = len(left_tokens) + len(right_tokens) - matches
    if union_size == 0:
        return 0.0
    return matches / union_size


def levenshtein_distance(left: Any, right: Any) -> int:
    left_text = strip_accents(canonicalize_entity_name(left) or "")
    right_text = strip_accents(canonicalize_entity_name(right) or "")
    if left_text == right_text:
        return 0
    if not left_text:
        return len(right_text)
    if not right_text:
        return len(left_text)

    prev = list(range(len(right_text) + 1))
    for i, left_char in enumerate(left_text, start=1):
        curr = [i]
        for j, right_char in enumerate(right_text, start=1):
            cost = 0 if left_char == right_char else 1
            curr.append(
                min(
                    prev[j] + 1,
                    curr[j - 1] + 1,
                    prev[j - 1] + cost,
                )
            )
        prev = curr
    return prev[-1]


_PROCESS_CODE_RE = re.compile(r"^([A-Za-z]+)\s+(\d+)")


def normalize_process_code(code: str) -> str:
    """Normalize to 'SIGLA NUMERO' (uppercase, trimmed, no incident suffix)."""
    text = code.strip().upper()
    match = _PROCESS_CODE_RE.match(text)
    if match:
        return f"{match.group(1)} {match.group(2)}"
    return text


def infer_process_class_from_number(process_number: str | None) -> str | None:
    if not process_number:
        return None
    match = re.match(r"^([A-Za-z]+)\s+\d+", str(process_number).strip())
    if not match:
        return None
    return match.group(1).upper()


# ---------------------------------------------------------------------------
# OAB / CNSA / Representation Network identity helpers
# ---------------------------------------------------------------------------

VALID_UF_CODES: frozenset[str] = frozenset(
    {
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    }
)

_OAB_RE = re.compile(r"^(\d{1,6})/([A-Z]{2})$")


def normalize_oab_number(value: Any) -> str | None:
    """Normalize OAB number to ``NNNNNN/UF`` format.

    Strips dots/spaces before the slash, uppercases the UF,
    and validates the resulting format.  Returns ``None`` for
    values that cannot be normalised into a valid OAB number.
    """
    text = as_optional_str(value)
    if text is None:
        return None
    text = text.upper().strip()
    slash_idx = text.rfind("/")
    if slash_idx < 0:
        return None
    number_part = re.sub(r"\D+", "", text[:slash_idx])
    uf_part = text[slash_idx + 1 :].strip()
    if not number_part or not uf_part:
        return None
    candidate = f"{number_part}/{uf_part}"
    if _OAB_RE.match(candidate) and uf_part in VALID_UF_CODES:
        return candidate
    return None


def is_valid_oab_format(value: Any) -> bool:
    """Return ``True`` when *value* matches the OAB format ``1-6 digits / valid UF``."""
    return normalize_oab_number(value) is not None


def normalize_cnsa_number(value: Any) -> str | None:
    """Strip formatting from a CNSA number, returning digits only.

    Returns ``None`` when the value contains no digits.
    """
    text = as_optional_str(value)
    if text is None:
        return None
    digits = re.sub(r"\D+", "", text)
    return digits or None


def build_lawyer_identity_key(
    *,
    name: Any,
    oab_number: Any = None,
    tax_id: Any = None,
) -> str | None:
    """Build a deterministic identity key for a lawyer entity.

    Priority: ``oab:NNNNNN/UF`` > ``tax:CPF`` > ``name:CANONICAL``.
    """
    oab = normalize_oab_number(oab_number)
    if oab:
        return f"oab:{oab}"
    tid = normalize_tax_id(tax_id)
    if tid:
        return f"tax:{tid}"
    canonical = canonicalize_entity_name(name)
    if canonical:
        return f"name:{canonical}"
    return None


def build_firm_identity_key(
    *,
    name: Any,
    cnpj: Any = None,
    cnsa_number: Any = None,
) -> str | None:
    """Build a deterministic identity key for a law firm entity.

    Priority: ``tax:CNPJ`` > ``cnsa:NNNN`` > ``name:CANONICAL``.
    """
    tid = normalize_tax_id(cnpj)
    if tid:
        return f"tax:{tid}"
    cnsa = normalize_cnsa_number(cnsa_number)
    if cnsa:
        return f"cnsa:{cnsa}"
    canonical = canonicalize_entity_name(name)
    if canonical:
        return f"name:{canonical}"
    return None
