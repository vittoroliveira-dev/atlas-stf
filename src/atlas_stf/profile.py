"""Profiling for staging datasets."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from .profile_models import ColumnProfile, DatasetProfile

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _resolve_input_file(input_dir: Path, filename: str) -> Path:
    candidate = (input_dir / filename).resolve()
    try:
        candidate.relative_to(input_dir)
    except ValueError as exc:
        raise ValueError("filename must resolve inside input_dir") from exc
    return candidate


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=True, low_memory=False)


def _sample_values(series: pd.Series, limit: int = 5) -> list[str]:
    non_null = series.dropna()
    if non_null.empty:
        return []
    counts = non_null.astype(str).value_counts().head(limit)
    return [str(value) for value in counts.index.tolist()]


def _looks_like_date(series: pd.Series) -> bool:
    non_null = series.dropna().astype(str)
    if non_null.empty:
        return False
    return bool(non_null.map(lambda value: bool(_DATE_RE.match(value))).all())


def _looks_like_key(column_name: str, series: pd.Series) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return False
    unique_ratio = non_null.nunique(dropna=True) / len(non_null)
    name_hint = any(token in column_name for token in ("id", "processo", "numero"))
    return unique_ratio >= 0.98 and name_hint


def profile_dataset(path: Path) -> DatasetProfile:
    df = _load_csv(path)
    columns: list[ColumnProfile] = []

    for column_name in df.columns:
        col = str(column_name)
        series: pd.Series = df.loc[:, col]
        non_null_count = int(series.notna().sum().item())
        null_count = int(series.isna().sum().item())
        non_null = series.dropna().astype(str)
        max_length = int(non_null.map(len).max().item()) if not non_null.empty else 0
        columns.append(
            ColumnProfile(
                name=col,
                non_null_count=non_null_count,
                null_count=null_count,
                distinct_count=int(series.nunique(dropna=True)),
                sample_values=_sample_values(series),
                max_length=max_length,
                looks_like_key=_looks_like_key(col, series),
                looks_like_date=_looks_like_date(series),
            )
        )

    return DatasetProfile(
        filename=path.name,
        row_count=len(df),
        column_count=len(df.columns),
        duplicate_row_count=int(df.duplicated().sum()),
        columns=columns,
    )


def profile_staging(
    input_dir: Path,
    output_dir: Path | None = None,
    filename: str | None = None,
) -> list[DatasetProfile]:
    input_dir = input_dir.resolve()
    output_dir = output_dir or input_dir / "_profile"
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = [_resolve_input_file(input_dir, filename)] if filename else sorted(input_dir.glob("*.csv"))
    profiles = [profile_dataset(path) for path in paths]

    for profile in profiles:
        path = output_dir / f"{Path(profile.filename).stem}.json"
        path.write_text(json.dumps(profile.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "dataset_count": len(profiles),
        "datasets": [
            {
                "filename": profile.filename,
                "row_count": profile.row_count,
                "column_count": profile.column_count,
                "duplicate_row_count": profile.duplicate_row_count,
            }
            for profile in profiles
        ],
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return profiles
