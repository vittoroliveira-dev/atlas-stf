"""Shared cleaning transforms."""

from __future__ import annotations

import re
import unicodedata

import pandas as pd


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    str_cols = df.select_dtypes(include=["object", "str"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()
    return df


def clean_x000d(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Replace _x000D_ (XML-escaped carriage return) with space in all string columns.

    Returns the dataframe and count of replacements.
    """
    count = 0
    str_cols = df.select_dtypes(include=["object", "str"]).columns
    for col in str_cols:
        mask = df[col].str.contains("_x000D_", na=False)
        n = mask.sum()
        if n > 0:
            df[col] = df[col].str.replace("_x000D_", " ", regex=False)
            count += n
    return df, count


def normalize_residual_nulls(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Convert residual null markers ('-', '*NI*') that survived initial read.

    These can appear inside multi-value fields where na_values didn't catch them.
    Returns the dataframe and count of replacements.
    """
    count = 0
    str_cols = df.select_dtypes(include=["object", "str"]).columns
    for col in str_cols:
        for marker in ("-", "*NI*"):
            mask = df[col].eq(marker)
            n = mask.sum()
            if n > 0:
                df.loc[mask, col] = pd.NA
                count += n
    return df, count


def _remove_accents(text: str) -> str:
    """Remove accents from text, preserving base characters."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def standardize_column_label(label: str) -> str:
    """Convert a single column label to snake_case without accents."""
    new = _remove_accents(label)
    new = new.strip().lower()
    new = re.sub(r"[^a-z0-9]+", "_", new)
    new = new.strip("_")
    return new.rstrip("_")


def standardize_column_names(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """Convert column names to snake_case without accents.

    Returns the dataframe and a mapping of old → new names.
    """
    mapping = {}
    for col in df.columns:
        new = standardize_column_label(col)
        mapping[col] = new

    # Handle duplicates by appending suffix
    seen: dict[str, int] = {}
    final_mapping: dict[str, str] = {}
    for old, new in mapping.items():
        if new in seen:
            seen[new] += 1
            final_mapping[old] = f"{new}_{seen[new]}"
        else:
            seen[new] = 0
            final_mapping[old] = new

    df = df.rename(columns=final_mapping)
    return df, final_mapping
