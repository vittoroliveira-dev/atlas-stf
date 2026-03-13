"""CVM fetch runner: downloads processo sancionador ZIP from CVM open data portal."""

from __future__ import annotations

import json
import logging
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from ..core.http_stream_safety import write_limited_stream_to_file
from ..core.zip_safety import enforce_max_uncompressed_size
from ._config import CVM_DATA_URL, CvmFetchConfig
from ._parser import join_and_normalize, parse_accused_csv, parse_process_csv

logger = logging.getLogger(__name__)

_CVM_MAX_ZIP_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
_CVM_MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024


def _download_zip(url: str, destination: Path, timeout: int) -> Path | None:
    """Stream a ZIP download to disk to avoid buffering the whole response."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response:
            response.raise_for_status()
            write_limited_stream_to_file(
                response,
                destination,
                max_download_bytes=_CVM_MAX_DOWNLOAD_BYTES,
            )
    except (ValueError, httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to download CVM data: %s", exc)
        destination.unlink(missing_ok=True)
        return None

    return destination


def _locate_csvs(output_dir: Path) -> tuple[Path | None, Path | None]:
    """Locate process + accused CSV files in a directory."""
    process_path: Path | None = None
    accused_path: Path | None = None

    for csv_file in output_dir.glob("*.csv"):
        name_lower = csv_file.name.lower()
        if "acusado" in name_lower:
            accused_path = csv_file
        elif "processo_sancionador" in name_lower or "sancionador" in name_lower:
            process_path = csv_file

    if process_path is None:
        # Fallback: pick any CSV that isn't the accused file
        for csv_file in output_dir.glob("*.csv"):
            if csv_file != accused_path:
                process_path = csv_file
                break

    return process_path, accused_path


def _process_zip(zip_path: Path, output_dir: Path) -> list[dict[str, Any]]:
    """Extract a ZIP from disk, parse CSVs, join and normalize."""
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path) as zf:
            safe_members: list[zipfile.ZipInfo] = []
            for info in zf.infolist():
                member = info.filename
                if ".." in member or member.startswith("/"):
                    logger.warning("Skipping unsafe ZIP member: %s", member)
                    continue
                safe_members.append(info)
            enforce_max_uncompressed_size(
                safe_members,
                max_total_uncompressed_bytes=_CVM_MAX_ZIP_UNCOMPRESSED_BYTES,
            )
            for member in safe_members:
                zf.extract(member, output_dir)
    except zipfile.BadZipFile:
        logger.warning("Invalid ZIP content")
        return []
    except ValueError as exc:
        logger.warning("Refusing CVM ZIP: %s", exc)
        return []

    process_path, accused_path = _locate_csvs(output_dir)

    if process_path is None:
        logger.warning("No process CSV found in ZIP")
        return []

    if accused_path is None:
        logger.warning("No accused CSV found in ZIP")
        return []

    processes = parse_process_csv(process_path)
    accused = parse_accused_csv(accused_path)
    return join_and_normalize(processes, accused)


def fetch_cvm_data(
    config: CvmFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Fetch CVM sanctions data: download ZIP, parse CSVs, write sanctions_raw.jsonl.

    Returns the output directory path.
    """
    config.output_dir.mkdir(parents=True, exist_ok=True)

    if config.dry_run:
        logger.info("[dry-run] Would download CVM processo sancionador from %s", CVM_DATA_URL)
        return config.output_dir

    total = 3  # download, parse, write
    if on_progress:
        on_progress(0, total, "CVM: Baixando ZIP...")
    url = CVM_DATA_URL
    logger.info("Downloading CVM data from %s", url)
    zip_path = _download_zip(url, config.output_dir / "cvm_source.zip", config.timeout_seconds)
    if zip_path is None:
        return config.output_dir

    if on_progress:
        on_progress(1, total, "CVM: Processando CSVs...")
    try:
        records = _process_zip(zip_path, config.output_dir)
    finally:
        zip_path.unlink(missing_ok=True)

    if on_progress:
        on_progress(2, total, "CVM: Gravando resultados...")
    output_path = config.output_dir / "sanctions_raw.jsonl"
    with output_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    if on_progress:
        on_progress(total, total, "CVM: Concluído")
    logger.info(
        "CVM fetch complete: %d sanction records written to %s",
        len(records),
        output_path,
    )
    return config.output_dir
