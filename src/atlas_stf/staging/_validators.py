"""Non-blocking data integrity validators for staging datasets."""

from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd

from ..core.identity import normalize_process_code
from ..core.parsers import as_optional_str, infer_process_number
from ._cleaners import standardize_column_label
from ._config import FileConfig

PROCESS_NUMBER_RE = re.compile(r"^[A-Za-z]{1,10}\s+\d+$")
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MAX_WARNING_SAMPLES = 5
DECISION_FILENAMES = {"decisoes.csv", "decisoes_covid.csv", "plenario_virtual.csv"}


def _standardized(columns: Iterable[str]) -> list[str]:
    return [standardize_column_label(column) for column in columns]


def _sample_list(values: Iterable[str], limit: int = MAX_WARNING_SAMPLES) -> list[str]:
    samples: list[str] = []
    for value in values:
        text = str(value)
        if text not in samples:
            samples.append(text)
        if len(samples) >= limit:
            break
    return samples


def _sample_repr(values: Iterable[str]) -> str:
    return "[" + ", ".join(repr(value) for value in _sample_list(values)) + "]"


def _nonnull_mask(df: pd.DataFrame, column: str) -> pd.Series:
    return df.loc[:, column].map(lambda value: as_optional_str(value) is not None)


def _build_process_number_samples(df: pd.DataFrame) -> list[str]:
    invalid_rows: list[str] = []
    for row in df.to_dict("records"):
        process_number = infer_process_number(row)
        if process_number is None:
            continue
        normalized = normalize_process_code(process_number)
        if PROCESS_NUMBER_RE.fullmatch(normalized):
            continue
        invalid_rows.append(normalized)
    return invalid_rows


def validate_dataframe(df: pd.DataFrame, config: FileConfig) -> list[str]:
    warnings: list[str] = []

    required_columns = _standardized(config.required_fields)
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        warnings.append(f"required_fields:{config.filename}:missing_columns={','.join(missing_columns)}")

    required_value_failures: list[str] = []
    for column in required_columns:
        if column not in df.columns:
            continue
        failed_rows = df.index[~_nonnull_mask(df, column)].tolist()
        if failed_rows:
            required_value_failures.append(f"{column}:{len(failed_rows)}")
    if required_value_failures:
        warnings.append(f"required_fields:{config.filename}:missing_values={','.join(required_value_failures)}")

    primary_key_columns = [column for column in _standardized(config.primary_key_columns) if column in df.columns]
    if primary_key_columns:
        pk_frame = df.loc[:, primary_key_columns]
        complete_mask = pk_frame.notna().all(axis=1)
        complete_frame = pk_frame.loc[complete_mask]
        duplicate_mask = complete_frame.duplicated(subset=primary_key_columns, keep=False)
        if duplicate_mask.any():
            duplicate_samples = (
                complete_frame.loc[duplicate_mask, primary_key_columns]
                .drop_duplicates()
                .astype(str)
                .agg(" | ".join, axis=1)
                .tolist()
            )
            warnings.append(
                f"duplicate_primary_key:{config.filename}:count={int(duplicate_mask.sum())}:"
                f"samples={_sample_repr(duplicate_samples)}"
            )

    invalid_dates: list[str] = []
    for column in _standardized(config.date_columns):
        if column not in df.columns:
            continue
        for value in df.loc[_nonnull_mask(df, column), column]:
            text = as_optional_str(value)
            if text is None or ISO_DATE_RE.fullmatch(text):
                continue
            invalid_dates.append(f"{column}={text}")
    if invalid_dates:
        warnings.append(
            f"date_format:{config.filename}:count={len(invalid_dates)}:samples={_sample_repr(invalid_dates)}"
        )

    invalid_process_numbers = _build_process_number_samples(df)
    if invalid_process_numbers:
        warnings.append(
            f"process_number_format:{config.filename}:count={len(invalid_process_numbers)}:"
            f"samples={_sample_repr(invalid_process_numbers)}"
        )

    return warnings


def collect_process_reference_keys(df: pd.DataFrame) -> set[str]:
    keys: set[str] = set()
    for row in df.to_dict("records"):
        process_number = infer_process_number(row)
        if process_number is None:
            continue
        keys.add(normalize_process_code(process_number))
    return keys


def collect_reconcilable_processes(df: pd.DataFrame) -> list[str]:
    processes: list[str] = []
    for row in df.to_dict("records"):
        process_number = infer_process_number(row)
        if process_number is None:
            continue
        processes.append(normalize_process_code(process_number))
    return processes


def validate_reconciliation_for_file(
    filename: str,
    process_numbers: Iterable[str],
    reference_keys: set[str],
) -> list[str]:
    process_list = list(process_numbers)
    orphaned = [process_number for process_number in process_list if process_number not in reference_keys]
    if not orphaned:
        return []
    return [f"cross_file_reconciliation:{filename}:count={len(orphaned)}:samples={_sample_repr(orphaned)}"]


def validate_cross_file_reconciliation(
    frames_by_file: dict[str, pd.DataFrame],
    configs: dict[str, FileConfig],
) -> dict[str, list[str]]:
    reference_keys: set[str] = set()
    decision_processes: dict[str, list[str]] = {}

    for filename, df in frames_by_file.items():
        config = configs.get(filename)
        if config is None:
            continue
        if config.reconcile_process_reference:
            decision_processes[filename] = collect_reconcilable_processes(df)
            continue
        reference_keys.update(collect_process_reference_keys(df))

    warnings_by_file: dict[str, list[str]] = {}
    for filename, process_numbers in decision_processes.items():
        warnings = validate_reconciliation_for_file(filename, process_numbers, reference_keys)
        if warnings:
            warnings_by_file[filename] = warnings
    return warnings_by_file
