"""Shared helpers for donor_corporate_link tests."""

from __future__ import annotations

import json
from pathlib import Path


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _setup_donations(tse_dir: Path, donations: list[dict]) -> None:
    _write_jsonl(tse_dir / "donations_raw.jsonl", donations)


def _setup_rfb(
    rfb_dir: Path,
    *,
    partners: list[dict] | None = None,
    companies: list[dict] | None = None,
    establishments: list[dict] | None = None,
) -> None:
    rfb_dir.mkdir(parents=True, exist_ok=True)
    if partners is not None:
        _write_jsonl(rfb_dir / "partners_raw.jsonl", partners)
    if companies is not None:
        _write_jsonl(rfb_dir / "companies_raw.jsonl", companies)
    if establishments is not None:
        _write_jsonl(rfb_dir / "establishments_raw.jsonl", establishments)
