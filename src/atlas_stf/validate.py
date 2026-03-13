"""Structural validation for staging datasets."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .validation_rules import expected_staging_columns


@dataclass(frozen=True)
class FileValidation:
    filename: str
    exists: bool
    row_count: int
    column_count: int
    missing_columns: list[str]
    status: str


@dataclass(frozen=True)
class JoinValidation:
    key: str
    anchor_file: str
    target_file: str
    anchor_non_null: int
    target_non_null: int
    overlap_count: int
    overlap_ratio: float
    status: str


@dataclass(frozen=True)
class ValidationReport:
    generated_at: str
    files: list[FileValidation] = field(default_factory=list)
    join_checks: list[JoinValidation] = field(default_factory=list)
    overall_status: str = "ok"


def _resolve_input_file(input_dir: Path, filename: str) -> str:
    candidate = (input_dir / filename).resolve()
    try:
        candidate.relative_to(input_dir)
    except ValueError as exc:
        raise ValueError("filename must resolve inside input_dir") from exc
    return candidate.name


def _read_header(path: Path) -> list[str]:
    return pd.read_csv(path, nrows=0).columns.tolist()


def _read_column_values(path: Path, column: str) -> pd.Series:
    df = pd.read_csv(path, usecols=[column], dtype=str, low_memory=False)
    series: pd.Series = df.loc[:, column]
    return series


def _validate_file(path: Path) -> FileValidation:
    exists = path.exists()
    if not exists:
        return FileValidation(
            filename=path.name, exists=False, row_count=0, column_count=0, missing_columns=[], status="missing"
        )

    df = pd.read_csv(path, dtype=str, low_memory=False)
    expected = expected_staging_columns(path.name)
    missing = sorted(column for column in expected if column not in df.columns)
    status = "ok" if not missing else "failed"
    return FileValidation(
        filename=path.name,
        exists=True,
        row_count=len(df),
        column_count=len(df.columns),
        missing_columns=missing,
        status=status,
    )


def _join_checks(input_dir: Path, filenames: list[str]) -> list[JoinValidation]:
    process_files = []
    for filename in filenames:
        path = input_dir / filename
        if not path.exists():
            continue
        header = _read_header(path)
        if "processo" in header:
            process_files.append(filename)

    if len(process_files) < 2:
        return []

    anchor = "decisoes.csv" if "decisoes.csv" in process_files else process_files[0]
    anchor_values = set(_read_column_values(input_dir / anchor, "processo").dropna().astype(str))
    checks: list[JoinValidation] = []
    for target in process_files:
        if target == anchor:
            continue
        target_values = set(_read_column_values(input_dir / target, "processo").dropna().astype(str))
        overlap = anchor_values & target_values
        overlap_ratio = (len(overlap) / len(target_values)) if target_values else 0.0
        status = "ok" if overlap_ratio > 0 else "incerto"
        checks.append(
            JoinValidation(
                key="processo",
                anchor_file=anchor,
                target_file=target,
                anchor_non_null=len(anchor_values),
                target_non_null=len(target_values),
                overlap_count=len(overlap),
                overlap_ratio=round(overlap_ratio, 6),
                status=status,
            )
        )
    return checks


def validate_staging(
    input_dir: Path,
    output_path: Path | None = None,
    filename: str | None = None,
) -> ValidationReport:
    input_dir = input_dir.resolve()
    output_path = output_path or input_dir / "_validation.json"
    if filename:
        filenames = [_resolve_input_file(input_dir, filename)]
    else:
        filenames = sorted(path.name for path in input_dir.glob("*.csv"))

    file_results = [_validate_file(input_dir / name) for name in filenames]
    join_checks = _join_checks(input_dir, filenames) if not filename else []
    overall_status = "ok"
    if any(result.status in {"failed", "missing"} for result in file_results):
        overall_status = "failed"

    report = ValidationReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        files=file_results,
        join_checks=join_checks,
        overall_status=overall_status,
    )
    output_path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2), encoding="utf-8")
    return report
