"""Build process-to-entity link records from curated processes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.identity import build_identity_key, canonicalize_entity_name, normalize_entity_name, stable_id
from ..core.parsers import (
    counsel_entries_from_juris_partes,
    party_entries_from_juris_partes,
    split_name_list,
)
from ..schema_validate import validate_records
from .common import (
    SOURCE_ID,
    read_jsonl_records,
    utc_now_iso,
    write_jsonl,
)

PARTY_SCHEMA_PATH = Path("schemas/process_party_link.schema.json")
COUNSEL_SCHEMA_PATH = Path("schemas/process_counsel_link.schema.json")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_PARTY_OUTPUT_PATH = Path("data/curated/process_party_link.jsonl")
DEFAULT_COUNSEL_OUTPUT_PATH = Path("data/curated/process_counsel_link.jsonl")
COUNSEL_SOURCE_FIELDS = (
    "juris_advogados",
    "juris_procuradores",
    "counsel_raw",
)


def build_process_party_link_records(process_path: Path = DEFAULT_PROCESS_PATH) -> list[dict[str, Any]]:
    processes = read_jsonl_records(process_path)
    timestamp = utc_now_iso()
    records: list[dict[str, Any]] = []

    for process in processes:
        process_id = process["process_id"]
        records_by_link_id: dict[str, dict[str, Any]] = {}
        for role, party_name in party_entries_from_juris_partes(process.get("juris_partes")):
            normalized = normalize_entity_name(party_name)
            if normalized is None:
                continue
            canonical = canonicalize_entity_name(normalized)
            identity_key = build_identity_key(normalized, canonical_name=canonical)
            if identity_key is None:
                continue
            party_id = stable_id("party_", normalized)
            link_id = stable_id("ppl_", f"{process_id}:{party_id}")
            records_by_link_id.setdefault(
                link_id,
                {
                    "link_id": link_id,
                    "process_id": process_id,
                    "party_id": party_id,
                    "role_in_case": role,
                    "source_id": SOURCE_ID,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            )
        records.extend(records_by_link_id.values())

    validate_records(records, PARTY_SCHEMA_PATH)
    return records


def build_process_counsel_link_records(process_path: Path = DEFAULT_PROCESS_PATH) -> list[dict[str, Any]]:
    processes = read_jsonl_records(process_path)
    timestamp = utc_now_iso()
    records: list[dict[str, Any]] = []

    for process in processes:
        process_id = process["process_id"]
        records_by_link_id: dict[str, dict[str, Any]] = {}

        def register_record(*, counsel_id: str, side_in_case: str | None) -> None:
            link_id = stable_id("pcl_", f"{process_id}:{counsel_id}")
            existing = records_by_link_id.get(link_id)
            if existing is None:
                records_by_link_id[link_id] = {
                    "link_id": link_id,
                    "process_id": process_id,
                    "counsel_id": counsel_id,
                    "side_in_case": side_in_case,
                    "source_id": SOURCE_ID,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                }
                return
            if existing["side_in_case"] is None and side_in_case is not None:
                existing["side_in_case"] = side_in_case

        for _, counsel_name, party_role in counsel_entries_from_juris_partes(process.get("juris_partes")):
            normalized = normalize_entity_name(counsel_name)
            if normalized is None:
                continue
            canonical = canonicalize_entity_name(normalized)
            identity_key = build_identity_key(normalized, canonical_name=canonical)
            if identity_key is None:
                continue
            counsel_id = stable_id("csl_", normalized)
            register_record(counsel_id=counsel_id, side_in_case=party_role)
        for field in COUNSEL_SOURCE_FIELDS:
            for counsel_name in split_name_list(process.get(field)):
                normalized = normalize_entity_name(counsel_name)
                if normalized is None:
                    continue
                canonical = canonicalize_entity_name(normalized)
                identity_key = build_identity_key(normalized, canonical_name=canonical)
                if identity_key is None:
                    continue
                counsel_id = stable_id("csl_", normalized)
                register_record(counsel_id=counsel_id, side_in_case=None)
        records.extend(records_by_link_id.values())

    validate_records(records, COUNSEL_SCHEMA_PATH)
    return records


def build_process_links_jsonl(
    process_path: Path = DEFAULT_PROCESS_PATH,
    party_output_path: Path = DEFAULT_PARTY_OUTPUT_PATH,
    counsel_output_path: Path = DEFAULT_COUNSEL_OUTPUT_PATH,
) -> tuple[Path, Path]:
    party_records = build_process_party_link_records(process_path=process_path)
    counsel_records = build_process_counsel_link_records(process_path=process_path)
    return (
        write_jsonl(party_records, party_output_path),
        write_jsonl(counsel_records, counsel_output_path),
    )
