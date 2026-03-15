"""Pydantic schemas for agenda endpoints."""

from __future__ import annotations

from pydantic import BaseModel


class AgendaEventItem(BaseModel):
    event_id: str
    minister_slug: str
    minister_name: str
    owner_scope: str
    owner_role: str
    event_date: str
    event_time_local: str | None = None
    source_time_raw: str | None = None
    event_title: str
    event_description: str | None = None
    event_category: str
    meeting_nature: str
    has_process_ref: bool = False
    classification_confidence: float = 0.0
    relevance_track: str = "none"
    process_refs: list[dict] = []
    process_id: str | None = None
    process_class: str | None = None
    is_own_process: bool | None = None
    minister_case_role: str | None = None
    contains_public_actor: bool = False
    contains_private_actor: bool = False
    actor_count: int = 0
    institutional_role_bias_flag: bool = False


class AgendaCoverageItem(BaseModel):
    coverage_id: str
    minister_slug: str
    minister_name: str
    year: int
    month: int
    event_count: int = 0
    days_with_events: int = 0
    coverage_ratio: float = 0.0
    comparability_tier: str = "low"
    court_recess_flag: bool = False
    publication_gap_flag: bool = False


class AgendaExposureItem(BaseModel):
    exposure_id: str
    agenda_event_id: str
    minister_slug: str
    process_id: str | None = None
    process_class: str | None = None
    agenda_date: str
    decision_date: str | None = None
    days_between: int | None = None
    window: str
    is_own_process: bool = False
    event_category: str = ""
    meeting_nature: str = ""
    event_title: str | None = None
    decision_type: str | None = None
    priority_score: float = 0.0
    priority_tier: str = "low"
    priority_tier_override_reason: str | None = None
    coverage_comparability: str = "low"


class AgendaMinisterSummary(BaseModel):
    minister_slug: str
    minister_name: str
    total_events: int = 0
    private_advocacy_count: int = 0
    track_a_count: int = 0
    coverage_months: int = 0
    avg_coverage_ratio: float = 0.0


class PaginatedAgendaEventsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[AgendaEventItem]


class PaginatedAgendaExposuresResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[AgendaExposureItem]


class AgendaSummaryResponse(BaseModel):
    total_events: int = 0
    total_ministerial_events: int = 0
    total_private_advocacy: int = 0
    total_with_process_ref: int = 0
    ministers_covered: int = 0
    total_exposures: int = 0
    high_priority_exposures: int = 0
    coverage_scope: str = "public_agenda_partial"
    methodology_note: str = ""
    disclaimer: str = ""
