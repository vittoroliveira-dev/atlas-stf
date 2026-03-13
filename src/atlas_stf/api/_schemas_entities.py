from __future__ import annotations

from pydantic import BaseModel

from ._schemas_core import (
    CaseSummaryItem,
    EntitySummaryItem,
    FilterOptionsResponse,
    RelationLevel,
    SourceAuditItem,
)


class MinisterCorrelationItem(BaseModel):
    minister: str
    associated_event_count: int
    distinct_process_count: int
    relation_level: RelationLevel
    role_labels: list[str] = []


class EntityDetailResponse(BaseModel):
    filters: FilterOptionsResponse
    entity: EntitySummaryItem
    ministers: list[MinisterCorrelationItem]
    cases: list[CaseSummaryItem]
    source_files: list[SourceAuditItem]


class PaginatedEntitiesResponse(BaseModel):
    filters: FilterOptionsResponse
    total: int
    page: int
    page_size: int
    items: list[EntitySummaryItem]


class RapporteurProfileResponse(BaseModel):
    rapporteur: str
    process_class: str
    thematic_key: str
    decision_year: int
    event_count: int
    chi2_statistic: float | None = None
    p_value_approx: float | None = None
    deviation_flag: bool
    deviation_direction: str | None = None
    progress_distribution: dict[str, int] = {}
    group_progress_distribution: dict[str, int] = {}


class SequentialAnalysisResponse(BaseModel):
    rapporteur: str
    decision_year: int
    n_decisions: int
    autocorrelation_lag1: float
    streak_effect_3: float | None = None
    streak_effect_5: float | None = None
    base_favorable_rate: float
    post_streak_favorable_rate_3: float | None = None
    post_streak_favorable_rate_5: float | None = None
    sequential_bias_flag: bool


class AssignmentAuditResponse(BaseModel):
    process_class: str
    decision_year: int
    rapporteur_count: int
    event_count: int
    chi2_statistic: float
    p_value_approx: float
    uniformity_flag: bool
    most_overrepresented_rapporteur: str | None = None
    most_underrepresented_rapporteur: str | None = None
    rapporteur_distribution: dict[str, int] = {}


class MinisterBioResponse(BaseModel):
    minister_name: str
    appointment_date: str | None = None
    appointing_president: str | None = None
    birth_date: str | None = None
    birth_state: str | None = None
    career_summary: str | None = None
    political_party_history: list[str] | None = None
    known_connections: list[str] | None = None
    news_references: list[str] | None = None


class OriginContextItem(BaseModel):
    origin_index: str
    tribunal_label: str
    state: str
    datajud_total_processes: int
    stf_process_count: int
    stf_share_pct: float
    top_assuntos: list[dict[str, str | int]] = []
    top_orgaos_julgadores: list[dict[str, str | int]] = []
    class_distribution: list[dict[str, str | int]] = []


class OriginContextResponse(BaseModel):
    items: list[OriginContextItem]
    total: int
