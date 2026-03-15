from __future__ import annotations

from pathlib import Path

from ._builder_utils import _read_jsonl
from .models import ServingMovement, ServingSessionEvent


def load_movements(curated_dir: Path) -> list[ServingMovement]:
    path = curated_dir / "movement.jsonl"
    if not path.exists():
        return []
    return [
        ServingMovement(
            movement_id=str(record["movement_id"]),
            process_id=str(record["process_id"]),
            source_system=str(record.get("source_system", "stf_portal")),
            tpu_code=record.get("tpu_code"),
            tpu_name=record.get("tpu_name"),
            movement_category=record.get("movement_category"),
            movement_raw_description=record.get("movement_raw_description"),
            movement_date=record.get("movement_date"),
            movement_detail=record.get("movement_detail"),
            rapporteur_at_event=record.get("rapporteur_at_event"),
            tpu_match_confidence=record.get("tpu_match_confidence"),
            normalization_method=record.get("normalization_method"),
        )
        for record in _read_jsonl(path)
    ]


def load_session_events(curated_dir: Path) -> list[ServingSessionEvent]:
    path = curated_dir / "session_event.jsonl"
    if not path.exists():
        return []
    return [
        ServingSessionEvent(
            session_event_id=str(record["session_event_id"]),
            process_id=str(record["process_id"]),
            movement_id=record.get("movement_id"),
            source_system=str(record.get("source_system", "stf_portal")),
            session_type=record.get("session_type"),
            event_type=record.get("event_type"),
            event_date=record.get("event_date"),
            rapporteur_at_event=record.get("rapporteur_at_event"),
            vista_duration_days=record.get("vista_duration_days"),
        )
        for record in _read_jsonl(path)
    ]
