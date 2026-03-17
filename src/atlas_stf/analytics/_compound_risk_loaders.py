"""Data loading and indexing helpers for compound risk analytics."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from ._compound_risk_evidence import PairEvidence
from ._match_io import read_jsonl

__all__ = [
    "_counsel_name_map",
    "_evidence_for",
    "_load_rows",
    "_pair_process_index",
    "_pair_process_map",
    "_party_name_map",
    "_process_context",
    "_process_entity_maps",
    "_qualifies_as_signal",
    "_required_inputs_exist",
]


def _required_inputs_exist(curated_dir: Path) -> bool:
    required = [
        curated_dir / "party.jsonl",
        curated_dir / "counsel.jsonl",
        curated_dir / "process_party_link.jsonl",
        curated_dir / "process_counsel_link.jsonl",
        curated_dir / "decision_event.jsonl",
    ]
    return all(path.exists() for path in required)


def _party_name_map(curated_dir: Path) -> dict[str, str]:
    return {
        str(record.get("party_id")): str(record.get("party_name_normalized") or record.get("party_name_raw") or "")
        for record in read_jsonl(curated_dir / "party.jsonl")
        if record.get("party_id")
    }


def _counsel_name_map(curated_dir: Path) -> dict[str, str]:
    return {
        str(record.get("counsel_id")): str(
            record.get("counsel_name_normalized") or record.get("counsel_name_raw") or ""
        )
        for record in read_jsonl(curated_dir / "counsel.jsonl")
        if record.get("counsel_id")
    }


def _process_entity_maps(
    curated_dir: Path,
    party_names: dict[str, str],
    counsel_names: dict[str, str],
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, list[tuple[str, str]]]]:
    process_parties: dict[str, list[tuple[str, str]]] = defaultdict(list)
    process_counsels: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for record in read_jsonl(curated_dir / "process_party_link.jsonl"):
        process_id = record.get("process_id")
        party_id = record.get("party_id")
        if process_id and party_id and party_id in party_names:
            process_parties[str(process_id)].append((str(party_id), party_names[str(party_id)]))

    for record in read_jsonl(curated_dir / "process_counsel_link.jsonl"):
        process_id = record.get("process_id")
        counsel_id = record.get("counsel_id")
        if process_id and counsel_id and counsel_id in counsel_names:
            process_counsels[str(process_id)].append((str(counsel_id), counsel_names[str(counsel_id)]))

    return dict(process_parties), dict(process_counsels)


def _process_context(
    curated_dir: Path,
) -> tuple[dict[str, set[str]], dict[str, tuple[str, str]], dict[str, tuple[int, int]]]:
    process_ministers: dict[str, set[str]] = defaultdict(set)
    decision_event_context: dict[str, tuple[str, str]] = {}
    _year_min: dict[str, int] = {}
    _year_max: dict[str, int] = {}

    for record in read_jsonl(curated_dir / "decision_event.jsonl"):
        process_id = record.get("process_id")
        minister_name = record.get("current_rapporteur")
        decision_event_id = record.get("decision_event_id")
        if process_id and minister_name:
            pid = str(process_id)
            process_ministers[pid].add(str(minister_name))
            if decision_event_id:
                decision_event_context[str(decision_event_id)] = (pid, str(minister_name))
            decision_date = record.get("decision_date")
            if decision_date and isinstance(decision_date, str) and len(decision_date) >= 4:
                try:
                    year = int(decision_date[:4])
                    if 1900 <= year <= 2100:
                        if pid not in _year_min or year < _year_min[pid]:
                            _year_min[pid] = year
                        if pid not in _year_max or year > _year_max[pid]:
                            _year_max[pid] = year
                except ValueError:
                    pass

    process_years = {pid: (_year_min[pid], _year_max[pid]) for pid in _year_min}
    return dict(process_ministers), decision_event_context, process_years


def _pair_process_map(
    process_ministers: dict[str, set[str]],
    process_parties: dict[str, list[tuple[str, str]]],
    process_counsels: dict[str, list[tuple[str, str]]],
) -> dict[tuple[str, str, str], set[str]]:
    pair_processes: dict[tuple[str, str, str], set[str]] = defaultdict(set)

    for process_id, ministers in process_ministers.items():
        for minister_name in ministers:
            for entity_id, _entity_name in process_parties.get(process_id, []):
                pair_processes[(minister_name, "party", entity_id)].add(process_id)
            for entity_id, _entity_name in process_counsels.get(process_id, []):
                pair_processes[(minister_name, "counsel", entity_id)].add(process_id)

    return dict(pair_processes)


def _pair_process_index(
    pair_processes: dict[tuple[str, str, str], set[str]],
) -> tuple[dict[str, list[tuple[str, set[str]]]], dict[str, list[tuple[str, set[str]]]]]:
    party_pairs: dict[str, list[tuple[str, set[str]]]] = defaultdict(list)
    counsel_pairs: dict[str, list[tuple[str, set[str]]]] = defaultdict(list)

    for (minister_name, entity_type, entity_id), process_ids in pair_processes.items():
        if entity_type == "party":
            party_pairs[entity_id].append((minister_name, process_ids))
        elif entity_type == "counsel":
            counsel_pairs[entity_id].append((minister_name, process_ids))

    return dict(party_pairs), dict(counsel_pairs)


def _qualifies_as_signal(row: dict[str, Any]) -> bool:
    """Decide if an analytics row qualifies as a compound risk signal.

    When ``red_flag_substantive`` is present in the row (even as False or
    None), it is the sole authority.  The legacy ``red_flag`` field only
    governs when the substantive field is absent — i.e. for old data or
    analytics sources that do not compute it (e.g. rapporteur_change).
    """
    if "red_flag_substantive" in row:
        return row["red_flag_substantive"] is True
    return bool(row.get("red_flag"))


def _load_rows(path: Path, *, red_flag_only: bool = False) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = read_jsonl(path)
    if red_flag_only:
        return [row for row in rows if _qualifies_as_signal(row)]
    return rows


def _evidence_for(
    pairs: dict[tuple[str, str, str], PairEvidence],
    minister_name: str,
    entity_type: str,
    entity_id: str,
    entity_name: str,
) -> PairEvidence:
    key = (minister_name, entity_type, entity_id)
    if key not in pairs:
        pairs[key] = PairEvidence(
            minister_name=minister_name,
            entity_type=entity_type,
            entity_id=entity_id,
            entity_name=entity_name,
        )
    return pairs[key]
