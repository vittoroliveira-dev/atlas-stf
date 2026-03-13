"""Build canonical subject records from curated processes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.rules import derive_thematic_key
from ..schema_validate import validate_records
from .common import read_jsonl_records, stable_id, utc_now_iso, write_jsonl

SCHEMA_PATH = Path("schemas/subject.schema.json")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/curated/subject.jsonl")
SUBJECT_NORMALIZATION_VERSION = "subject-v1"


def build_subject_records(process_path: Path = DEFAULT_PROCESS_PATH) -> list[dict[str, Any]]:
    processes = read_jsonl_records(process_path)
    subject_map: dict[str, dict[str, Any]] = {}
    timestamp = utc_now_iso()

    for process in processes:
        subjects_raw = process.get("subjects_raw") or []
        subjects_normalized = process.get("subjects_normalized") or []
        branch_of_law = process.get("branch_of_law")
        for index, raw_value in enumerate(subjects_raw):
            normalized_value = subjects_normalized[index] if index < len(subjects_normalized) else raw_value
            subject_key = str(normalized_value or raw_value).strip()
            if not subject_key:
                continue
            subject_map.setdefault(
                subject_key,
                {
                    "subject_id": stable_id("sub_", subject_key),
                    "subject_raw": raw_value,
                    "subject_normalized": normalized_value,
                    "subject_group": derive_thematic_key(
                        [normalized_value] if normalized_value else None,
                        branch_of_law,
                        fallback="",
                    )
                    or None,
                    "branch_of_law": branch_of_law,
                    "normalization_version": SUBJECT_NORMALIZATION_VERSION,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            )

    records = sorted(subject_map.values(), key=lambda item: item["subject_id"])
    validate_records(records, SCHEMA_PATH)
    return records


def build_subject_jsonl(process_path: Path = DEFAULT_PROCESS_PATH, output_path: Path = DEFAULT_OUTPUT_PATH) -> Path:
    records = build_subject_records(process_path=process_path)
    return write_jsonl(records, output_path)
