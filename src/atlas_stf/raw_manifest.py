"""Raw-layer manifest generation."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from .core.io_hash import file_sha256

TRANSPARENCIA_BASE = "https://transparencia.stf.jus.br/extensions"


@dataclass(frozen=True)
class RawManifestRecord:
    filename: str
    path: str
    sha256: str
    size_bytes: int
    observed_at: str
    source_id: str
    origin_url: str | None
    filter_description: str
    coverage_note: str
    column_count: int
    row_count: int


def _sniff_csv_metadata(path: Path) -> tuple[list[str], int]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return [], 0
        row_count = sum(1 for _ in reader)
    return header, row_count


def _infer_origin_url(path: Path) -> str | None:
    stem = path.stem
    if not stem:
        return None
    return f"{TRANSPARENCIA_BASE}/{stem}/{stem}.html"


def build_raw_manifest(
    input_dir: Path,
    output_path: Path | None = None,
    source_id: str = "STF-TRANSP-REGDIST",
    filter_description: str = "INCERTO",
    coverage_note: str = "INCERTO",
) -> list[RawManifestRecord]:
    input_dir = input_dir.resolve()
    output_path = output_path or input_dir / "_manifest.jsonl"
    observed_at = datetime.now(timezone.utc).isoformat()

    records: list[RawManifestRecord] = []
    for path in sorted(input_dir.glob("*.csv")):
        header, row_count = _sniff_csv_metadata(path)
        records.append(
            RawManifestRecord(
                filename=path.name,
                path=str(path),
                sha256=file_sha256(path),
                size_bytes=path.stat().st_size,
                observed_at=observed_at,
                source_id=source_id,
                origin_url=_infer_origin_url(path),
                filter_description=filter_description,
                coverage_note=coverage_note,
                column_count=len(header),
                row_count=row_count,
            )
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")

    return records
