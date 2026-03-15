"""Fetch and parse RFB reference/domain tables (Qualificacoes, Naturezas, etc.)."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ._config import RFB_REFERENCE_TABLES

logger = logging.getLogger(__name__)


def _parse_two_column_csv_text(text_stream: Any, delimiter: str = ";") -> dict[str, str]:
    """Parse a 2-column CSV (code;description) into a dict."""
    import csv

    result: dict[str, str] = {}
    reader = csv.reader(text_stream, delimiter=delimiter)
    for row in reader:
        if len(row) < 2:
            continue
        code = row[0].strip()
        description = row[1].strip()
        if code:
            result[code] = description
    return result


def fetch_reference_tables(
    output_dir: Path,
    base_url: str,
    timeout: int,
    *,
    download_zip: Any,
    parse_csv_from_zip_text: Any,
) -> dict[str, Path]:
    """Download and parse the 5 RFB domain tables.

    Returns mapping of table name -> output JSON path.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Path] = {}

    for table_name in RFB_REFERENCE_TABLES:
        output_path = output_dir / f"{table_name.lower()}.json"
        if output_path.exists() and output_path.stat().st_size > 0:
            logger.info("Reference table %s already cached", table_name)
            results[table_name] = output_path
            continue

        url = f"{base_url}/{table_name}.zip"
        zip_path = output_dir / f"{table_name}.zip"

        downloaded = download_zip(url, zip_path, timeout)
        if downloaded is None:
            logger.warning("Failed to download %s", table_name)
            continue

        parsed = parse_csv_from_zip_text(
            downloaded,
            _parse_two_column_csv_text,
        )
        if parsed is None:
            logger.warning("Failed to parse %s", table_name)
            zip_path.unlink(missing_ok=True)
            continue

        output_path.write_text(
            json.dumps(parsed, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        zip_path.unlink(missing_ok=True)
        logger.info("Reference table %s: %d entries", table_name, len(parsed))
        results[table_name] = output_path

    return results


def load_reference_table(path: Path) -> dict[str, str]:
    """Load a reference table JSON file into a dict."""
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_all_reference_tables(rfb_dir: Path) -> dict[str, dict[str, str]]:
    """Load all available reference tables from rfb_dir."""
    tables: dict[str, dict[str, str]] = {}
    for table_name in RFB_REFERENCE_TABLES:
        path = rfb_dir / f"{table_name.lower()}.json"
        tables[table_name.lower()] = load_reference_table(path)
    return tables
