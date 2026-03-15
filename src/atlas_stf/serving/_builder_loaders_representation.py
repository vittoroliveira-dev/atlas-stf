"""Loaders for representation network entities and edges."""

from __future__ import annotations

import json
from pathlib import Path

from ._builder_utils import (
    _coerce_float,
    _coerce_int,
    _dedupe_records_by_key,
    _parse_date,
    _parse_datetime,
    _read_jsonl,
)
from ._models_representation import (
    ServingLawFirmEntity,
    ServingLawyerEntity,
    ServingProcessLawyer,
    ServingRepresentationEdge,
    ServingRepresentationEvent,
)


def load_lawyer_entities(curated_dir: Path) -> list[ServingLawyerEntity]:
    path = curated_dir / "lawyer_entity.jsonl"
    if not path.exists():
        return []
    results: list[ServingLawyerEntity] = []
    for record in _read_jsonl(path):
        results.append(
            ServingLawyerEntity(
                lawyer_id=str(record.get("lawyer_id", "")),
                lawyer_name_raw=str(record.get("lawyer_name_raw", "")),
                lawyer_name_normalized=record.get("lawyer_name_normalized"),
                canonical_name_normalized=record.get("canonical_name_normalized"),
                oab_number=record.get("oab_number"),
                oab_state=record.get("oab_state"),
                oab_status=record.get("oab_status"),
                oab_source=record.get("oab_source"),
                oab_validation_method=record.get("oab_validation_method"),
                oab_last_checked_at=_parse_datetime(record.get("oab_last_checked_at")),
                entity_tax_id=record.get("entity_tax_id"),
                identity_key=record.get("identity_key"),
                identity_strategy=record.get("identity_strategy"),
                source_systems_json=json.dumps(record.get("source_systems", []), ensure_ascii=False),
                firm_id=record.get("firm_id"),
                notes=record.get("notes"),
                process_count=_coerce_int(record.get("process_count")),
                event_count=_coerce_int(record.get("event_count")),
                first_seen_date=_parse_date(record.get("first_seen_date")),
                last_seen_date=_parse_date(record.get("last_seen_date")),
            )
        )
    return results


def load_law_firm_entities(curated_dir: Path) -> list[ServingLawFirmEntity]:
    path = curated_dir / "law_firm_entity.jsonl"
    if not path.exists():
        return []
    results: list[ServingLawFirmEntity] = []
    for record in _read_jsonl(path):
        results.append(
            ServingLawFirmEntity(
                firm_id=str(record.get("firm_id", "")),
                firm_name_raw=str(record.get("firm_name_raw", "")),
                firm_name_normalized=record.get("firm_name_normalized"),
                canonical_name_normalized=record.get("canonical_name_normalized"),
                cnpj=record.get("cnpj"),
                cnpj_valid=record.get("cnpj_valid"),
                cnsa_number=record.get("cnsa_number"),
                identity_key=record.get("identity_key"),
                identity_strategy=record.get("identity_strategy"),
                source_systems_json=json.dumps(record.get("source_systems", []), ensure_ascii=False),
                member_lawyer_ids_json=json.dumps(record.get("member_lawyer_ids", []), ensure_ascii=False),
                member_count=_coerce_int(record.get("member_count")),
                process_count=_coerce_int(record.get("process_count")),
                first_seen_date=_parse_date(record.get("first_seen_date")),
                last_seen_date=_parse_date(record.get("last_seen_date")),
            )
        )
    return results


def load_process_lawyers(curated_dir: Path) -> list[ServingProcessLawyer]:
    path = curated_dir / "process_lawyer_link.jsonl"
    if not path.exists():
        path = curated_dir / "process_counsel_link.jsonl"
    if not path.exists():
        return []
    results: list[ServingProcessLawyer] = []
    for record in _dedupe_records_by_key(_read_jsonl(path), "link_id"):
        results.append(
            ServingProcessLawyer(
                link_id=str(record.get("link_id", "")),
                process_id=str(record.get("process_id", "")),
                lawyer_id=str(record.get("lawyer_id") or record.get("counsel_id") or ""),
                side_in_case=record.get("side_in_case"),
                source_id=record.get("source_id"),
            )
        )
    return results


def load_representation_edges(curated_dir: Path) -> list[ServingRepresentationEdge]:
    path = curated_dir / "representation_edge.jsonl"
    if not path.exists():
        return []
    results: list[ServingRepresentationEdge] = []
    seen: set[str] = set()
    for record in _read_jsonl(path):
        eid = str(record.get("edge_id", ""))
        if eid in seen:
            continue
        seen.add(eid)
        results.append(
            ServingRepresentationEdge(
                edge_id=eid,
                process_id=str(record.get("process_id", "")),
                representative_entity_id=str(record.get("representative_entity_id", "")),
                representative_kind=record.get("representative_kind"),
                role_type=record.get("role_type"),
                lawyer_id=record.get("lawyer_id"),
                firm_id=record.get("firm_id"),
                party_id=record.get("party_id"),
                event_count=_coerce_int(
                    record.get("event_count") if record.get("event_count") is not None
                    else record.get("evidence_count")
                ),
                start_date=_parse_date(record.get("start_date")),
                end_date=_parse_date(record.get("end_date")),
                confidence=_coerce_float(record.get("confidence")),
                source_systems_json=json.dumps(record.get("source_systems", []), ensure_ascii=False),
            )
        )
    return results


def load_representation_events(curated_dir: Path) -> list[ServingRepresentationEvent]:
    path = curated_dir / "representation_event.jsonl"
    if not path.exists():
        return []
    results: list[ServingRepresentationEvent] = []
    seen: set[str] = set()
    for record in _read_jsonl(path):
        eid = str(record.get("event_id", ""))
        if eid in seen:
            continue
        seen.add(eid)
        results.append(
            ServingRepresentationEvent(
                event_id=eid,
                process_id=str(record.get("process_id", "")),
                edge_id=record.get("edge_id"),
                lawyer_id=record.get("lawyer_id"),
                firm_id=record.get("firm_id"),
                event_type=record.get("event_type"),
                event_date=_parse_date(record.get("event_date")),
                event_description=record.get("event_description"),
                protocol_number=record.get("protocol_number"),
                document_type=record.get("document_type"),
                source_system=record.get("source_system"),
                source_url=record.get("source_url"),
                confidence=_coerce_float(record.get("confidence")),
            )
        )
    return results
