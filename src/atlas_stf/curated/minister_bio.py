"""Minister biographical profile — curated seed data validation and coverage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_BIO_PATH = Path("data/curated/minister_bio.json")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")

REQUIRED_FIELDS = {"minister_name", "appointment_date", "appointing_president"}
OPTIONAL_FIELDS = {
    "civil_name",
    "political_party_history",
    "known_connections",
    "news_references",
    "birth_date",
    "birth_state",
    "career_summary",
}
ALL_FIELDS = REQUIRED_FIELDS | OPTIONAL_FIELDS


@dataclass(frozen=True)
class BioValidationResult:
    total_ministers_in_bio: int
    total_ministers_in_data: int
    covered_count: int
    missing_from_bio: list[str]
    schema_errors: list[str]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _validate_entry(name: str, entry: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in REQUIRED_FIELDS:
        if not entry.get(field):
            errors.append(f"{name}: campo obrigatório ausente: {field}")
    if not isinstance(entry.get("minister_name"), str):
        errors.append(f"{name}: minister_name deve ser string")
    if entry.get("political_party_history") is not None and not isinstance(entry["political_party_history"], list):
        errors.append(f"{name}: political_party_history deve ser lista ou null")
    if entry.get("known_connections") is not None and not isinstance(entry["known_connections"], list):
        errors.append(f"{name}: known_connections deve ser lista ou null")
    if entry.get("news_references") is not None and not isinstance(entry["news_references"], list):
        errors.append(f"{name}: news_references deve ser lista ou null")
    return errors


def build_minister_bio_index(
    *,
    bio_path: Path = DEFAULT_BIO_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
) -> BioValidationResult:
    if not bio_path.exists():
        raise FileNotFoundError(f"Arquivo de perfis biográficos não encontrado: {bio_path}")

    bio_data: dict[str, Any] = json.loads(bio_path.read_text(encoding="utf-8"))

    schema_errors: list[str] = []
    for name, entry in bio_data.items():
        schema_errors.extend(_validate_entry(name, entry))

    bio_names = {name.upper() for name in bio_data}

    data_rapporteurs: set[str] = set()
    if decision_event_path.exists():
        for record in _read_jsonl(decision_event_path):
            rap = record.get("current_rapporteur")
            if rap and isinstance(rap, str) and rap.strip():
                data_rapporteurs.add(rap.strip().upper())

    covered = bio_names & data_rapporteurs
    missing = sorted(data_rapporteurs - bio_names)

    return BioValidationResult(
        total_ministers_in_bio=len(bio_data),
        total_ministers_in_data=len(data_rapporteurs),
        covered_count=len(covered),
        missing_from_bio=missing,
        schema_errors=schema_errors,
    )
