from __future__ import annotations

from fastapi import FastAPI

from ._schemas_timeline import MovementResponse, SessionEventResponse, TimelineResponse
from ._service_timeline import get_process_sessions, get_process_timeline


def register_timeline_routes(
    app: FastAPI,
    factory: ...,
    build_filters: ...,
    get_base_filters: ...,
) -> None:
    @app.get("/caso/{process_id}/timeline", response_model=TimelineResponse)
    def case_timeline(process_id: str) -> TimelineResponse:
        with factory() as session:
            movements, session_events = get_process_timeline(session, process_id)
        return TimelineResponse(
            process_id=process_id,
            movements=[
                MovementResponse(
                    movement_id=m.movement_id,
                    process_id=m.process_id,
                    source_system=m.source_system,
                    tpu_code=m.tpu_code,
                    tpu_name=m.tpu_name,
                    movement_category=m.movement_category,
                    movement_raw_description=m.movement_raw_description,
                    movement_date=m.movement_date,
                    movement_detail=m.movement_detail,
                    rapporteur_at_event=m.rapporteur_at_event,
                    tpu_match_confidence=m.tpu_match_confidence,
                    normalization_method=m.normalization_method,
                )
                for m in movements
            ],
            session_events=[
                SessionEventResponse(
                    session_event_id=s.session_event_id,
                    process_id=s.process_id,
                    movement_id=s.movement_id,
                    source_system=s.source_system,
                    session_type=s.session_type,
                    event_type=s.event_type,
                    event_date=s.event_date,
                    rapporteur_at_event=s.rapporteur_at_event,
                    vista_duration_days=s.vista_duration_days,
                )
                for s in session_events
            ],
            total_movements=len(movements),
            total_session_events=len(session_events),
        )

    @app.get("/caso/{process_id}/sessions", response_model=list[SessionEventResponse])
    def case_sessions(process_id: str) -> list[SessionEventResponse]:
        with factory() as session:
            events = get_process_sessions(session, process_id)
        return [
            SessionEventResponse(
                session_event_id=s.session_event_id,
                process_id=s.process_id,
                movement_id=s.movement_id,
                source_system=s.source_system,
                session_type=s.session_type,
                event_type=s.event_type,
                event_date=s.event_date,
                rapporteur_at_event=s.rapporteur_at_event,
                vista_duration_days=s.vista_duration_days,
            )
            for s in events
        ]
