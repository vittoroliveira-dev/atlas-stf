"""Shared donor identity key helper — used by donation_match and donor_corporate_link."""

from __future__ import annotations

from ..core.identity import normalize_tax_id


def donor_identity_key(name: str, cpf_cnpj: str) -> str:
    """Build a stable identity key for a donor.

    Uses ``normalize_tax_id(donor_cpf_cnpj)`` when it produces a valid
    digit-only string (prevents homonym fusion and normalizes formatting).
    Masked documents (``***.***-**``) and empty strings resolve to ``None``
    and fall back to ``donor_name_normalized``.

    Note: the name fallback still carries residual homonymy risk when
    two distinct people share the same normalized name and neither has
    a CPF/CNPJ on file.  This is a known limitation documented in the
    audit (section E.1).
    """
    normalized_id = normalize_tax_id(cpf_cnpj)
    if normalized_id:
        return f"cpf:{normalized_id}"
    return f"name:{name}"
