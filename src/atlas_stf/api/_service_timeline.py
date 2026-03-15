from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..serving.models import ServingMovement, ServingSessionEvent


def get_process_timeline(session: Session, process_id: str) -> tuple[list[ServingMovement], list[ServingSessionEvent]]:
    movements = list(
        session.scalars(
            select(ServingMovement)
            .where(ServingMovement.process_id == process_id)
            .order_by(ServingMovement.movement_date)
        ).all()
    )
    session_events = list(
        session.scalars(
            select(ServingSessionEvent)
            .where(ServingSessionEvent.process_id == process_id)
            .order_by(ServingSessionEvent.event_date)
        ).all()
    )
    return movements, session_events


def get_process_sessions(session: Session, process_id: str) -> list[ServingSessionEvent]:
    return list(
        session.scalars(
            select(ServingSessionEvent)
            .where(ServingSessionEvent.process_id == process_id)
            .order_by(ServingSessionEvent.event_date)
        ).all()
    )
