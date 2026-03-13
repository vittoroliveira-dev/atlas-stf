"""Assunto field normalization and separator fixes."""

from __future__ import annotations

import re

import pandas as pd


def _normalize_assunto_value(value: str) -> str:
    """Normalize a single assunto field value.

    - Newlines between numbered items become ' | '
    - Internal hierarchy '||' is preserved
    - Whitespace around separators is standardized
    """
    if not value:
        return value

    # Replace newlines between numbered items: "...TEXT\n2 - ..." → "...TEXT | 2 - ..."
    value = re.sub(r"\s*\n\s*(?=\d+ - )", " | ", value)

    # Replace remaining newlines with space
    value = re.sub(r"\s*\n\s*", " ", value)

    # Standardize || spacing
    value = re.sub(r"\s*\|\|\s*", " || ", value)

    # Standardize | spacing (but not ||)
    value = re.sub(r"(?<!\|)\s*\|\s*(?!\|)", " | ", value)

    return value.strip()


def fix_assuntos(df: pd.DataFrame, column: str) -> tuple[pd.DataFrame, int]:
    """Normalize assunto column values.

    Returns the dataframe and count of modified values.
    """
    if column not in df.columns:
        return df, 0

    mask = df[column].notna()
    original = df.loc[mask, column].copy()
    df.loc[mask, column] = df.loc[mask, column].map(_normalize_assunto_value)
    changed = (original != df.loc[mask, column]).sum()
    return df, int(changed)


def normalize_multi_value(df: pd.DataFrame, columns: list[str], separator: str) -> tuple[pd.DataFrame, int]:
    """Normalize multi-value fields with custom separator (e.g. ';#' in omissao).

    Pattern: 'Value1;#id1;#Value2;#id2' → 'Value1 | Value2'
    The numeric IDs after ;# are internal identifiers and are dropped.

    Returns the dataframe and count of modified values.
    """
    count = 0
    for col in columns:
        if col not in df.columns:
            continue
        mask = df[col].notna() & df[col].str.contains(separator, na=False, regex=False)
        n = mask.sum()
        if n > 0:
            df.loc[mask, col] = df.loc[mask, col].map(
                lambda v: _clean_multi_value(v, separator) if isinstance(v, str) else v
            )
            count += n
    return df, count


def _clean_multi_value(value: str, separator: str) -> str:
    """Parse 'Value1;#id1;#Value2;#id2' → 'Value1 | Value2'."""
    parts = value.split(separator)
    # SharePoint-style payload alternates value/id/value/id; keep only value positions.
    values = [part.strip() for idx, part in enumerate(parts) if idx % 2 == 0 and part.strip()]
    return " | ".join(values) if values else value
