"""Build canonical counsel records from curated processes when source data is available."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.identity import build_identity_key, canonicalize_entity_name, normalize_entity_name, stable_id
from ..core.parsers import counsel_entries_from_juris_partes, split_name_list
from ..schema_validate import validate_records
from .common import (
    read_jsonl_records,
    utc_now_iso,
    write_jsonl,
)

SCHEMA_PATH = Path("schemas/counsel.schema.json")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/curated/counsel.jsonl")
COUNSEL_NORMALIZATION_VERSION = "counsel-v1"
COUNSEL_SOURCE_FIELDS = (
    "juris_advogados",
    "juris_procuradores",
    "counsel_raw",
)


def build_counsel_records(process_path: Path = DEFAULT_PROCESS_PATH) -> list[dict[str, Any]]:
    processes = read_jsonl_records(process_path)
    counsel_map: dict[str, dict[str, Any]] = {}
    timestamp = utc_now_iso()

    for process in processes:
        for label, counsel_name, party_role in counsel_entries_from_juris_partes(process.get("juris_partes")):
            normalized = normalize_entity_name(counsel_name)
            if normalized is None:
                continue
            canonical = canonicalize_entity_name(normalized)
            identity_key = build_identity_key(normalized, canonical_name=canonical)
            if identity_key is None:
                continue
            counsel_map.setdefault(
                normalized,
                {
                    "counsel_id": stable_id("csl_", normalized),
                    "counsel_name_raw": counsel_name,
                    "counsel_name_normalized": normalized,
                    "canonical_name_normalized": canonical,
                    "entity_tax_id": None,
                    "identity_key": identity_key,
                    "identity_strategy": "name",
                    "normalization_confidence": 1.0,
                    "normalization_version": COUNSEL_NORMALIZATION_VERSION,
                    "notes": party_role or label,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            )
        for field in COUNSEL_SOURCE_FIELDS:
            for counsel_name in split_name_list(process.get(field)):
                normalized = normalize_entity_name(counsel_name)
                if normalized is None:
                    continue
                canonical = canonicalize_entity_name(normalized)
                identity_key = build_identity_key(normalized, canonical_name=canonical)
                if identity_key is None:
                    continue
                counsel_map.setdefault(
                    normalized,
                    {
                        "counsel_id": stable_id("csl_", normalized),
                        "counsel_name_raw": counsel_name,
                        "counsel_name_normalized": normalized,
                        "canonical_name_normalized": canonical,
                        "entity_tax_id": None,
                        "identity_key": identity_key,
                        "identity_strategy": "name",
                        "normalization_confidence": 1.0,
                        "normalization_version": COUNSEL_NORMALIZATION_VERSION,
                        "notes": None,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                    },
                )

    records = sorted(counsel_map.values(), key=lambda item: item["counsel_id"])
    validate_records(records, SCHEMA_PATH)
    return records


def build_counsel_jsonl(process_path: Path = DEFAULT_PROCESS_PATH, output_path: Path = DEFAULT_OUTPUT_PATH) -> Path:
    records = build_counsel_records(process_path=process_path)
    return write_jsonl(records, output_path)
