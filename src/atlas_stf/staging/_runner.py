"""Pipeline orchestration and CLI."""

from __future__ import annotations

import argparse
import csv
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..io_hash import file_sha256
from ._assuntos import fix_assuntos, normalize_multi_value
from ._audit import AuditRecord, logger, setup_logging, write_audit
from ._cleaners import clean_x000d, normalize_residual_nulls, standardize_column_names, strip_whitespace
from ._config import CONFIGS, FileConfig
from ._dates import normalize_all_dates
from ._readers import read_csv
from ._validators import (
    validate_cross_file_reconciliation,
    validate_dataframe,
)

_ProgressFn = Callable[[int, int, str], None]

RAW_DIR = Path("data/raw/transparencia")
STAGING_DIR = Path("data/staging/transparencia")


class RowCountMismatchError(RuntimeError):
    """Raised when staging transforms unexpectedly change the number of rows."""


@dataclass
class PreparedFile:
    config: FileConfig
    df: pd.DataFrame
    raw_hash: str
    raw_row_count: int
    transforms: dict[str, int]
    warnings: list[str]


def _prepare_file(config: FileConfig, raw_dir: Path) -> PreparedFile:
    transforms: dict[str, int] = {}
    warnings: list[str] = []

    logger.info("Processing %s ...", config.filename)

    # 1. Read
    df = read_csv(raw_dir, config)
    raw_row_count = len(df)
    logger.info("  Read %d rows, %d columns", raw_row_count, len(df.columns))

    # 2. Strip whitespace
    df = strip_whitespace(df)

    # 3. Clean _x000D_
    if config.x000d_clean:
        df, n = clean_x000d(df)
        transforms["x000d_removed"] = n
        if n > 0:
            logger.info("  Cleaned %d _x000D_ occurrences", n)

    # 4. Normalize residual nulls
    df, n = normalize_residual_nulls(df)
    transforms["residual_nulls"] = n
    if n > 0:
        logger.info("  Normalized %d residual null values", n)

    # 5. Normalize dates
    df, n = normalize_all_dates(df, config.date_columns)
    transforms["dates_normalized"] = n
    if n > 0:
        logger.info("  Normalized %d date values", n)

    # 6. Fix assuntos
    if config.assunto_column:
        df, n = fix_assuntos(df, config.assunto_column)
        transforms["assuntos_fixed"] = n
        if n > 0:
            logger.info("  Fixed %d assunto values", n)

    # 7. Normalize multi-value fields
    if config.multi_value_separator and config.multi_value_columns:
        df, n = normalize_multi_value(df, config.multi_value_columns, config.multi_value_separator)
        transforms["multi_value_normalized"] = n
        if n > 0:
            logger.info("  Normalized %d multi-value fields", n)

    # 8. Standardize column names
    df, col_mapping = standardize_column_names(df)
    transforms["columns_renamed"] = sum(1 for old, new in col_mapping.items() if old != new)
    warnings.extend(validate_dataframe(df, config))

    # Validate row count
    staging_row_count = len(df)
    if staging_row_count != raw_row_count:
        message = f"Row count mismatch: raw={raw_row_count}, staging={staging_row_count}"
        logger.error("  %s", message)
        raise RowCountMismatchError(message)

    for warning in warnings:
        logger.warning("  %s", warning)

    return PreparedFile(
        config=config,
        df=df,
        raw_hash=file_sha256(raw_dir / config.filename),
        raw_row_count=raw_row_count,
        transforms=transforms,
        warnings=warnings,
    )


def _write_prepared_file(prepared: PreparedFile, staging_dir: Path) -> Path:
    staging_row_count = len(prepared.df)
    staging_dir.mkdir(parents=True, exist_ok=True)
    output_path = staging_dir / prepared.config.filename
    prepared.df.to_csv(
        output_path,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        lineterminator="\n",
    )
    logger.info("  Wrote %s (%d rows)", output_path, staging_row_count)
    return output_path


def _build_audit_record(prepared: PreparedFile, output_path: Path) -> AuditRecord:
    return AuditRecord(
        filename=prepared.config.filename,
        raw_sha256=prepared.raw_hash,
        staging_sha256=file_sha256(output_path),
        raw_row_count=prepared.raw_row_count,
        staging_row_count=len(prepared.df),
        transforms=prepared.transforms,
        warnings=prepared.warnings,
    )


def process_file(config: FileConfig, raw_dir: Path, staging_dir: Path, dry_run: bool = False) -> AuditRecord | None:
    """Run the full cleaning pipeline for a single file."""
    prepared = _prepare_file(config, raw_dir)

    if dry_run:
        if config.reconcile_process_reference:
            logger.warning("  cross_file_reconciliation:%s:skipped_single_file_mode", config.filename)
        logger.info("  [DRY RUN] Would write %d rows", len(prepared.df))
        return None

    if config.reconcile_process_reference:
        prepared.warnings.append(f"cross_file_reconciliation:{config.filename}:skipped_single_file_mode")
        logger.warning("  %s", prepared.warnings[-1])

    output_path = _write_prepared_file(prepared, staging_dir)
    return _build_audit_record(prepared, output_path)


def clean_all(
    raw_dir: Path = RAW_DIR,
    staging_dir: Path = STAGING_DIR,
    dry_run: bool = False,
    on_progress: _ProgressFn | None = None,
) -> list[AuditRecord]:
    """Process all configured files."""
    records: list[AuditRecord] = []
    prepared_files: dict[str, PreparedFile] = {}
    configs = list(CONFIGS.values())
    total = len(configs) + 1  # +1 for cross-file reconciliation

    for i, config in enumerate(configs):
        if on_progress:
            on_progress(i, total, f"Staging: {config.filename}")
        prepared = _prepare_file(config, raw_dir)
        if dry_run:
            logger.info("  [DRY RUN] Would write %d rows", len(prepared.df))
            continue

        output_path = _write_prepared_file(prepared, staging_dir)
        prepared_files[config.filename] = prepared
        records.append(_build_audit_record(prepared, output_path))

    if on_progress:
        on_progress(len(configs), total, "Staging: Validação cruzada...")
    if not dry_run and prepared_files:
        frames_by_file = {filename: pf.df for filename, pf in prepared_files.items()}
        warnings_by_file = validate_cross_file_reconciliation(frames_by_file, CONFIGS)
        records_by_file = {record.filename: record for record in records}
        for filename, warnings in warnings_by_file.items():
            records_by_file[filename].warnings.extend(warnings)
            for warning in warnings:
                logger.warning("  %s", warning)

    if records:
        audit_path = write_audit(records, staging_dir)
        logger.info("Audit trail: %s", audit_path)

    if on_progress:
        on_progress(total, total, "Staging: Concluído")
    return records


def clean_file(
    filename: str,
    raw_dir: Path = RAW_DIR,
    staging_dir: Path = STAGING_DIR,
    dry_run: bool = False,
) -> AuditRecord | None:
    """Process a single file by name."""
    if filename not in CONFIGS:
        raise ValueError(f"Unknown file: {filename}. Available: {', '.join(CONFIGS)}")
    return process_file(CONFIGS[filename], raw_dir, staging_dir, dry_run)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Atlas STF — Staging pipeline")
    parser.add_argument("--file", help="Process a single file (e.g. acervo.csv)")
    parser.add_argument("--dry-run", action="store_true", help="Show transforms without writing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug-level logging")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Raw data directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Staging output directory")
    args = parser.parse_args()

    args.staging_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(args.staging_dir, verbose=args.verbose)

    try:
        if args.file:
            record = clean_file(args.file, args.raw_dir, args.staging_dir, args.dry_run)
            if record:
                write_audit([record], args.staging_dir)
        else:
            if args.dry_run:
                clean_all(args.raw_dir, args.staging_dir, args.dry_run)
            else:
                def _log_progress(current: int, total: int, desc: str) -> None:
                    logger.info("[%d/%d] %s", current, total, desc)

                clean_all(args.raw_dir, args.staging_dir, args.dry_run, on_progress=_log_progress)
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)

    logger.info("Done.")
