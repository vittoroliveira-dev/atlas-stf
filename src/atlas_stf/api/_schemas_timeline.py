from __future__ import annotations

from pydantic import BaseModel


class MovementResponse(BaseModel):
    movement_id: str
    process_id: str
    source_system: str
    tpu_code: int | None = None
    tpu_name: str | None = None
    movement_category: str | None = None
    movement_raw_description: str | None = None
    movement_date: str | None = None
    movement_detail: str | None = None
    rapporteur_at_event: str | None = None
    tpu_match_confidence: str | None = None
    normalization_method: str | None = None


class SessionEventResponse(BaseModel):
    session_event_id: str
    process_id: str
    movement_id: str | None = None
    source_system: str
    session_type: str | None = None
    event_type: str | None = None
    event_date: str | None = None
    rapporteur_at_event: str | None = None
    vista_duration_days: int | None = None


class TimelineResponse(BaseModel):
    process_id: str
    movements: list[MovementResponse]
    session_events: list[SessionEventResponse]
    total_movements: int
    total_session_events: int
