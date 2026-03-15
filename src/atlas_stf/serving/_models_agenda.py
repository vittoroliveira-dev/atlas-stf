from __future__ import annotations

from datetime import date, datetime, time

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Text, Time
from sqlalchemy.orm import Mapped, mapped_column

from .models import Base


class ServingAgendaEvent(Base):
    __tablename__ = "agenda_event"
    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    minister_slug: Mapped[str] = mapped_column(String(64), index=True)
    minister_name: Mapped[str] = mapped_column(String(256))
    owner_scope: Mapped[str] = mapped_column(String(32), index=True)
    owner_role: Mapped[str] = mapped_column(String(32))
    event_date: Mapped[date] = mapped_column(Date, index=True)
    event_time_local: Mapped[time | None] = mapped_column(Time)
    event_datetime_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_time_raw: Mapped[str | None] = mapped_column(String(32))
    event_title: Mapped[str] = mapped_column(String(512))
    event_description: Mapped[str | None] = mapped_column(Text)
    event_category: Mapped[str] = mapped_column(String(64), index=True)
    meeting_nature: Mapped[str] = mapped_column(String(64), index=True)
    has_process_ref: Mapped[bool] = mapped_column(Boolean)
    contains_public_actor: Mapped[bool] = mapped_column(Boolean)
    contains_private_actor: Mapped[bool] = mapped_column(Boolean)
    actor_count: Mapped[int] = mapped_column(Integer)
    classification_confidence: Mapped[float] = mapped_column(Float)
    relevance_track: Mapped[str] = mapped_column(String(8))
    process_refs_json: Mapped[str | None] = mapped_column(Text)
    participants_json: Mapped[str | None] = mapped_column(Text)
    participant_resolution_confidence: Mapped[float | None] = mapped_column(Float)
    organizations_json: Mapped[str | None] = mapped_column(Text)
    process_id: Mapped[str | None] = mapped_column(String(64), index=True)
    process_class: Mapped[str | None] = mapped_column(String(64))
    is_own_process: Mapped[bool | None] = mapped_column(Boolean)
    minister_case_role: Mapped[str | None] = mapped_column(String(32))
    institutional_role_bias_flag: Mapped[bool] = mapped_column(Boolean)


class ServingAgendaCoverage(Base):
    __tablename__ = "agenda_coverage"
    coverage_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    minister_slug: Mapped[str] = mapped_column(String(64), index=True)
    minister_name: Mapped[str] = mapped_column(String(256))
    owner_scope: Mapped[str] = mapped_column(String(32))
    year: Mapped[int] = mapped_column(Integer)
    month: Mapped[int] = mapped_column(Integer)
    publication_observed: Mapped[bool] = mapped_column(Boolean)
    event_count: Mapped[int] = mapped_column(Integer)
    days_with_events: Mapped[int] = mapped_column(Integer)
    business_days_in_month: Mapped[int] = mapped_column(Integer)
    coverage_ratio: Mapped[float] = mapped_column(Float)
    institutional_core_count: Mapped[int] = mapped_column(Integer)
    institutional_external_actor_count: Mapped[int] = mapped_column(Integer)
    private_advocacy_count: Mapped[int] = mapped_column(Integer)
    unclear_count: Mapped[int] = mapped_column(Integer)
    track_a_count: Mapped[int] = mapped_column(Integer)
    track_b_count: Mapped[int] = mapped_column(Integer)
    court_recess_flag: Mapped[bool] = mapped_column(Boolean)
    vacation_or_leave_flag: Mapped[bool] = mapped_column(Boolean)
    publication_gap_flag: Mapped[bool] = mapped_column(Boolean)
    comparability_tier: Mapped[str] = mapped_column(String(16))
    coverage_quality_note: Mapped[str | None] = mapped_column(Text)


class ServingAgendaExposure(Base):
    __tablename__ = "agenda_exposure"
    exposure_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    agenda_event_id: Mapped[str] = mapped_column(String(64), index=True)
    minister_slug: Mapped[str] = mapped_column(String(64), index=True)
    process_id: Mapped[str | None] = mapped_column(String(64), index=True)
    process_class: Mapped[str | None] = mapped_column(String(64))
    agenda_date: Mapped[date] = mapped_column(Date, index=True)
    decision_date: Mapped[date | None] = mapped_column(Date)
    days_between: Mapped[int | None] = mapped_column(Integer)
    window: Mapped[str] = mapped_column(String(8), index=True)
    is_own_process: Mapped[bool] = mapped_column(Boolean)
    minister_case_role: Mapped[str | None] = mapped_column(String(32))
    event_category: Mapped[str] = mapped_column(String(64))
    meeting_nature: Mapped[str] = mapped_column(String(64))
    event_title: Mapped[str | None] = mapped_column(String(512))
    decision_type: Mapped[str | None] = mapped_column(String(64))
    baseline_rate: Mapped[float | None] = mapped_column(Float)
    rate_ratio: Mapped[float | None] = mapped_column(Float)
    priority_score: Mapped[float] = mapped_column(Float)
    priority_tier: Mapped[str] = mapped_column(String(16), index=True)
    priority_tier_override_reason: Mapped[str | None] = mapped_column(String(64))
    coverage_comparability: Mapped[str] = mapped_column(String(16))
