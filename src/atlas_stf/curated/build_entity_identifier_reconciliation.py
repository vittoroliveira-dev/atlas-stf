"""Reconcile extracted STF tax identifiers against process-local party/counsel entities."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.identity import (
    canonicalize_entity_name,
    jaccard_similarity,
    levenshtein_distance,
    normalize_entity_name,
    stable_id,
)
from ..schema_validate import validate_records
from .common import read_jsonl_records, utc_now_iso, write_jsonl

SCHEMA_PATH = Path("schemas/entity_identifier_reconciliation.schema.json")
DEFAULT_ENTITY_IDENTIFIER_PATH = Path("data/curated/entity_identifier.jsonl")
DEFAULT_PARTY_PATH = Path("data/curated/party.jsonl")
DEFAULT_COUNSEL_PATH = Path("data/curated/counsel.jsonl")
DEFAULT_PROCESS_PARTY_LINK_PATH = Path("data/curated/process_party_link.jsonl")
DEFAULT_PROCESS_COUNSEL_LINK_PATH = Path("data/curated/process_counsel_link.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/curated/entity_identifier_reconciliation.jsonl")


def _build_entity_map(
    records: list[dict[str, Any]],
    *,
    id_field: str,
    name_field: str,
    raw_name_field: str,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for record in records:
        entity_id = record.get(id_field)
        if not entity_id:
            continue
        result[str(entity_id)] = {
            **record,
            "_name_normalized": normalize_entity_name(record.get(name_field) or record.get(raw_name_field)),
            "_canonical_name": canonicalize_entity_name(
                record.get("canonical_name_normalized") or record.get(name_field)
            ),
        }
    return result


def _index_links_by_process(
    links: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for link in links:
        pid = link.get("process_id")
        if pid:
            index.setdefault(pid, []).append(link)
    return index


def _build_process_entity_candidates(
    *,
    process_id: str | None,
    party_links_by_process: dict[str, list[dict[str, Any]]],
    counsel_links_by_process: dict[str, list[dict[str, Any]]],
    party_map: dict[str, dict[str, Any]],
    counsel_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if not process_id:
        return []

    candidates: list[dict[str, Any]] = []
    for link in party_links_by_process.get(process_id, []):
        party = party_map.get(str(link.get("party_id")))
        if party is None:
            continue
        candidates.append(
            {
                "entity_kind": "party",
                "entity_id": party["party_id"],
                "entity_name_normalized": party.get("party_name_normalized"),
                "canonical_name_normalized": party.get("canonical_name_normalized") or party.get("_canonical_name"),
                "_name_normalized": party.get("_name_normalized"),
                "_canonical_name": party.get("_canonical_name"),
            }
        )

    for link in counsel_links_by_process.get(process_id, []):
        counsel = counsel_map.get(str(link.get("counsel_id")))
        if counsel is None:
            continue
        candidates.append(
            {
                "entity_kind": "counsel",
                "entity_id": counsel["counsel_id"],
                "entity_name_normalized": counsel.get("counsel_name_normalized"),
                "canonical_name_normalized": counsel.get("canonical_name_normalized") or counsel.get("_canonical_name"),
                "_name_normalized": counsel.get("_name_normalized"),
                "_canonical_name": counsel.get("_canonical_name"),
            }
        )
    return candidates


def _score_candidate(entity_name_hint: str, candidate: dict[str, Any]) -> tuple[int, float, str] | None:
    hint_norm = normalize_entity_name(entity_name_hint)
    hint_canonical = canonicalize_entity_name(entity_name_hint)
    candidate_norm = candidate.get("_name_normalized")
    candidate_canonical = candidate.get("_canonical_name")
    if hint_norm is None or hint_canonical is None or candidate_norm is None or candidate_canonical is None:
        return None

    if hint_norm == candidate_norm:
        return (4, 1.0, "exact")
    if hint_canonical == candidate_canonical:
        return (3, 1.0, "canonical_name")

    jaccard = jaccard_similarity(hint_canonical, candidate_canonical)
    if jaccard >= 0.8:
        return (2, jaccard, "jaccard")

    distance = levenshtein_distance(hint_canonical, candidate_canonical)
    if distance <= 2:
        return (1, 1.0 / (1 + distance), "levenshtein")

    return None


def build_entity_identifier_reconciliation_records(
    *,
    entity_identifier_path: Path = DEFAULT_ENTITY_IDENTIFIER_PATH,
    party_path: Path = DEFAULT_PARTY_PATH,
    counsel_path: Path = DEFAULT_COUNSEL_PATH,
    process_party_link_path: Path = DEFAULT_PROCESS_PARTY_LINK_PATH,
    process_counsel_link_path: Path = DEFAULT_PROCESS_COUNSEL_LINK_PATH,
) -> list[dict[str, Any]]:
    identifier_rows = read_jsonl_records(entity_identifier_path)
    party_map = _build_entity_map(
        read_jsonl_records(party_path),
        id_field="party_id",
        name_field="party_name_normalized",
        raw_name_field="party_name_raw",
    )
    counsel_map = _build_entity_map(
        read_jsonl_records(counsel_path),
        id_field="counsel_id",
        name_field="counsel_name_normalized",
        raw_name_field="counsel_name_raw",
    )
    party_links_by_process = _index_links_by_process(read_jsonl_records(process_party_link_path))
    counsel_links_by_process = _index_links_by_process(read_jsonl_records(process_counsel_link_path))

    created_at = utc_now_iso()
    records: list[dict[str, Any]] = []
    for identifier in identifier_rows:
        entity_name_hint = identifier.get("entity_name_hint")
        candidates = _build_process_entity_candidates(
            process_id=identifier.get("process_id"),
            party_links_by_process=party_links_by_process,
            counsel_links_by_process=counsel_links_by_process,
            party_map=party_map,
            counsel_map=counsel_map,
        )

        scored: list[tuple[tuple[int, float, str], dict[str, Any]]] = []
        if isinstance(entity_name_hint, str) and entity_name_hint.strip():
            for candidate in candidates:
                score = _score_candidate(entity_name_hint, candidate)
                if score is not None:
                    scored.append((score, candidate))

        proposal_status = "unresolved"
        entity_kind: str | None = None
        entity_id: str | None = None
        entity_name_normalized: str | None = None
        proposal_strategy: str | None = None
        proposal_score: float | None = None
        candidate_count = 0
        uncertainty_note: str | None = None

        if not entity_name_hint:
            uncertainty_note = "missing_entity_name_hint"
        elif not scored:
            uncertainty_note = "no_process_local_match"
        else:
            scored.sort(key=lambda item: (item[0][0], item[0][1]), reverse=True)
            best_priority, best_score, best_strategy = scored[0][0]
            top_candidates = [
                candidate
                for (priority, score, _), candidate in scored
                if priority == best_priority and score == best_score
            ]
            candidate_count = len(top_candidates)
            if len(top_candidates) == 1:
                proposal_status = "proposed"
                top_candidate = top_candidates[0]
                entity_kind = str(top_candidate["entity_kind"])
                entity_id = str(top_candidate["entity_id"])
                entity_name_normalized = top_candidate.get("entity_name_normalized")
                proposal_strategy = best_strategy
                proposal_score = best_score
            else:
                proposal_status = "ambiguous"
                uncertainty_note = "multiple_process_local_candidates"

        reconciliation_id = stable_id(
            "eir_",
            f"{identifier.get('identifier_occurrence_id')}:{proposal_status}:{entity_id or 'none'}",
        )
        records.append(
            {
                "reconciliation_id": reconciliation_id,
                "identifier_occurrence_id": identifier.get("identifier_occurrence_id"),
                "process_id": identifier.get("process_id"),
                "process_number": identifier.get("process_number"),
                "identifier_kind": identifier.get("identifier_kind"),
                "identifier_value_normalized": identifier.get("identifier_value_normalized"),
                "entity_kind": entity_kind,
                "entity_id": entity_id,
                "entity_name_normalized": entity_name_normalized,
                "proposal_strategy": proposal_strategy,
                "proposal_score": proposal_score,
                "candidate_count": candidate_count,
                "proposal_status": proposal_status,
                "uncertainty_note": uncertainty_note,
                "created_at": created_at,
            }
        )

    validate_records(records, SCHEMA_PATH)
    return records


def build_entity_identifier_reconciliation_jsonl(
    *,
    entity_identifier_path: Path = DEFAULT_ENTITY_IDENTIFIER_PATH,
    party_path: Path = DEFAULT_PARTY_PATH,
    counsel_path: Path = DEFAULT_COUNSEL_PATH,
    process_party_link_path: Path = DEFAULT_PROCESS_PARTY_LINK_PATH,
    process_counsel_link_path: Path = DEFAULT_PROCESS_COUNSEL_LINK_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    records = build_entity_identifier_reconciliation_records(
        entity_identifier_path=entity_identifier_path,
        party_path=party_path,
        counsel_path=counsel_path,
        process_party_link_path=process_party_link_path,
        process_counsel_link_path=process_counsel_link_path,
    )
    return write_jsonl(records, output_path)
