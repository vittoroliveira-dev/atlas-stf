"""Build canonical party records from curated processes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.identity import build_identity_key, canonicalize_entity_name, normalize_entity_name, stable_id
from ..core.parsers import party_entries_from_juris_partes
from ..schema_validate import validate_records
from .common import (
    read_jsonl_records,
    utc_now_iso,
    write_jsonl,
)

SCHEMA_PATH = Path("schemas/party.schema.json")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/curated/party.jsonl")
PARTY_NORMALIZATION_VERSION = "party-v1"


def build_party_records(process_path: Path = DEFAULT_PROCESS_PATH) -> list[dict[str, Any]]:
    processes = read_jsonl_records(process_path)
    party_map: dict[str, dict[str, Any]] = {}
    timestamp = utc_now_iso()

    for process in processes:
        for role, party_name in party_entries_from_juris_partes(process.get("juris_partes")):
            normalized = normalize_entity_name(party_name)
            if normalized is None:
                continue
            canonical = canonicalize_entity_name(normalized)
            identity_key = build_identity_key(normalized, canonical_name=canonical)
            if identity_key is None:
                continue
            party_map.setdefault(
                normalized,
                {
                    "party_id": stable_id("party_", normalized),
                    "party_name_raw": party_name,
                    "party_name_normalized": normalized,
                    "canonical_name_normalized": canonical,
                    "entity_tax_id": None,
                    "identity_key": identity_key,
                    "identity_strategy": "name",
                    "party_type": None,
                    "normalization_confidence": 1.0,
                    "normalization_version": PARTY_NORMALIZATION_VERSION,
                    "notes": None,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            )

    records = sorted(party_map.values(), key=lambda item: item["party_id"])
    validate_records(records, SCHEMA_PATH)
    return records


def build_party_jsonl(process_path: Path = DEFAULT_PROCESS_PATH, output_path: Path = DEFAULT_OUTPUT_PATH) -> Path:
    records = build_party_records(process_path=process_path)
    return write_jsonl(records, output_path)
