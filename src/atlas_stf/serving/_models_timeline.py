from __future__ import annotations

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .models import Base


class ServingMovement(Base):
    __tablename__ = "serving_movement"

    movement_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    source_system: Mapped[str] = mapped_column(String(32))
    tpu_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tpu_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    movement_category: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    movement_raw_description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    movement_date: Mapped[str | None] = mapped_column(String(10), index=True, nullable=True)
    movement_detail: Mapped[str | None] = mapped_column(Text(), nullable=True)
    rapporteur_at_event: Mapped[str | None] = mapped_column(String(256), nullable=True)
    tpu_match_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)
    normalization_method: Mapped[str | None] = mapped_column(String(32), nullable=True)


class ServingSessionEvent(Base):
    __tablename__ = "serving_session_event"

    session_event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    movement_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_system: Mapped[str] = mapped_column(String(32))
    session_type: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    event_date: Mapped[str | None] = mapped_column(String(10), index=True, nullable=True)
    rapporteur_at_event: Mapped[str | None] = mapped_column(String(256), nullable=True)
    vista_duration_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
