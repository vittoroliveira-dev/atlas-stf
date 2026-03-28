"""Provenance capture for raw source files before ingest deletion."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .core.io_hash import file_sha256
from .core.schema_sig import normalize_header_for_signature, normalize_header_value

_SAMPLE_ROWS = 10
_FIRST_1MB = 1_048_576


def _read_csv_rows(reader: csv.reader) -> tuple[list[str], list[list[str]], int]:  # type: ignore[type-arg]
    raw_header = next(reader, [])
    sample: list[list[str]] = []
    count = 0
    for row in reader:
        count += 1
        if len(sample) < _SAMPLE_ROWS:
            sample.append(row)
    return raw_header, sample, count


@dataclass(frozen=True)
class SourceManifest:
    source: str
    file_name: str
    year_or_cycle: str
    sha256_full: str
    sha256_first_1mb: str
    encoding: str
    delimiter: str
    raw_header: list[str]
    normalized_header: list[str]
    layout_signature: str
    column_count: int
    row_count: int
    sample_rows: list[list[str]]
    parser_version: str
    source_file_fingerprint: str
    observed_at: str
    origin_url: str = ""
    etag: str = ""
    content_length: int = 0
    manifest_schema_version: str = field(default="1.0")
    layout_signature_algorithm: str = field(default="sha256-normalized-v1")


def _make(  # noqa: PLR0913
    source: str,
    file_name: str,
    year_or_cycle: str,
    origin_url: str,
    etag: str,
    content_length: int,
    sha256_full: str,
    sha256_first_1mb: str,
    encoding: str,
    delimiter: str,
    raw_header: list[str],
    sample_rows: list[list[str]],
    row_count: int,
    parser_version: str,
) -> SourceManifest:
    normalized = [normalize_header_value(v) for v in raw_header]
    return SourceManifest(
        source=source,
        file_name=file_name,
        year_or_cycle=year_or_cycle,
        origin_url=origin_url,
        etag=etag,
        content_length=content_length,
        sha256_full=sha256_full,
        sha256_first_1mb=sha256_first_1mb,
        encoding=encoding,
        delimiter=delimiter,
        raw_header=raw_header,
        normalized_header=normalized,
        layout_signature=normalize_header_for_signature(raw_header),
        column_count=len(raw_header),
        row_count=row_count,
        sample_rows=sample_rows,
        parser_version=parser_version,
        source_file_fingerprint=sha256_full,
        observed_at=datetime.now(timezone.utc).isoformat(),
    )


def capture_csv_manifest(
    path: Path,
    *,
    source: str,
    year_or_cycle: str,
    origin_url: str = "",
    etag: str = "",
    content_length: int = 0,
    parser_version: str = "1.0",
    encoding: str = "",
    delimiter: str = ";",
) -> SourceManifest:
    """Capture provenance manifest from a CSV file on disk."""
    if not encoding:
        try:
            path.read_text(encoding="utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            encoding = "latin-1"
    sha256_full = file_sha256(path)
    with path.open("rb") as fb:
        sha256_first_1mb = hashlib.sha256(fb.read(_FIRST_1MB)).hexdigest()
    with path.open(encoding=encoding, newline="") as ft:
        raw_header, sample_rows, row_count = _read_csv_rows(csv.reader(ft, delimiter=delimiter))
    return _make(
        source,
        path.name,
        year_or_cycle,
        origin_url,
        etag,
        content_length,
        sha256_full,
        sha256_first_1mb,
        encoding,
        delimiter,
        raw_header,
        sample_rows,
        row_count,
        parser_version,
    )


def capture_csv_manifest_from_stream(
    lines: list[str],
    *,
    source: str,
    year_or_cycle: str,
    file_name: str = "",
    origin_url: str = "",
    etag: str = "",
    content_length: int = 0,
    parser_version: str = "1.0",
    encoding: str = "utf-8",
    delimiter: str = ";",
) -> SourceManifest:
    """Capture provenance manifest from in-memory lines (e.g. RFB streamed CSVs).

    ``file_name`` identifies the source within its ZIP archive (e.g.
    ``"Socios0.zip:Socios0CSV.csv"``). Defaults to ``""`` for callers that do
    not have a meaningful filename.
    """
    content_bytes = "\n".join(lines).encode(encoding)
    sha256_full = hashlib.sha256(content_bytes).hexdigest()
    sha256_first_1mb = hashlib.sha256(content_bytes[:_FIRST_1MB]).hexdigest()
    raw_header, sample_rows, row_count = _read_csv_rows(csv.reader(lines, delimiter=delimiter))
    return _make(
        source,
        file_name,
        year_or_cycle,
        origin_url,
        etag,
        content_length,
        sha256_full,
        sha256_first_1mb,
        encoding,
        delimiter,
        raw_header,
        sample_rows,
        row_count,
        parser_version,
    )


def write_manifest(manifest: SourceManifest, output_dir: Path) -> Path:
    """Write manifest as JSON to output_dir, creating the directory if needed."""
    output_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{manifest.source}_{manifest.file_name}_{manifest.year_or_cycle}.manifest.json"
    dest = output_dir / fname
    dest.write_text(json.dumps(asdict(manifest), indent=2, ensure_ascii=False), encoding="utf-8")
    return dest
