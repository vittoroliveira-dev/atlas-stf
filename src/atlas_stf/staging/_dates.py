"""Date normalization: 4 formats → yyyy-MM-dd strings."""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd

# Date format patterns ordered by specificity
_FORMATS = [
    # yyyy-MM-dd HH:mm:ss (reclamacoes, repercussao_geral, omissao)
    (re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"), "%Y-%m-%d %H:%M:%S"),
    # yyyy-MM-dd (already normalized)
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "%Y-%m-%d"),
    # dd/MM/yyyy HH:mm:ss (decisoes)
    (re.compile(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"), "%d/%m/%Y %H:%M:%S"),
    # dd/MM/yyyy (distribuidos, recebidos_baixados)
    (re.compile(r"^\d{2}/\d{2}/\d{4}$"), "%d/%m/%Y"),
    # d/M/yyyy - no zero padding (decisoes_covid)
    (re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$"), "%d/%m/%Y"),
]


def _parse_single_date(value: str) -> str | None:
    """Parse a single date string to yyyy-MM-dd format."""
    value = value.strip()
    if not value:
        return None

    for pattern, fmt in _FORMATS:
        if pattern.match(value):
            try:
                dt = datetime.strptime(value, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def normalize_date_column(series: pd.Series) -> pd.Series:
    """Normalize a date column to yyyy-MM-dd string format."""
    return series.map(lambda v: _parse_single_date(v) if pd.notna(v) and str(v).strip() else pd.NA)


def normalize_all_dates(df: pd.DataFrame, date_columns: list[str]) -> tuple[pd.DataFrame, int]:
    """Normalize all date columns in the dataframe.

    Returns the dataframe and count of dates successfully normalized.
    """
    count = 0
    for col in date_columns:
        if col not in df.columns:
            continue
        series: pd.Series = df.loc[:, col]
        original = series.copy()
        df[col] = normalize_date_column(series)
        changed = (original != df[col]) & original.notna()
        count += int(changed.sum())
    return df, count
