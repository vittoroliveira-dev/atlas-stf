from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    pass

CollegiateFilter = Literal["all", "colegiado", "monocratico"]
RelationLevel = Literal["process_level", "decision_derived", "incerto"]
HeroStatus = Literal["ok", "empty", "inconclusivo"]


class AppliedFilters(BaseModel):
    minister: str | None = None
    period: str | None = None
    collegiate: CollegiateFilter
    judging_body: str | None = None
    process_class: str | None = None


class FilterOptionsResponse(BaseModel):
    ministers: list[str]
    periods: list[str]
    collegiates: list[CollegiateFilter] = Field(default_factory=lambda: ["all", "colegiado", "monocratico"])
    judging_bodies: list[str]
    process_classes: list[str]
    applied: AppliedFilters


class SourceAuditItem(BaseModel):
    label: str
    category: str
    path: str
    checksum: str
    updated_at: datetime


class MetricsSummary(BaseModel):
    alert_count: int
    valid_group_count: int
    baseline_count: int
    average_alert_score: float
    selected_events: int
    selected_processes: int


class DistributionItem(BaseModel):
    label: str
    value: int


class DailyPoint(BaseModel):
    date: str
    event_count: int
    delta_vs_historical_average: float
    ratio_vs_historical_average: float


class SegmentFlowItem(BaseModel):
    segment_value: str
    event_count: int
    process_count: int
    active_day_count: int
    historical_event_count: int
    historical_active_day_count: int
    historical_average_events_per_active_day: float
    daily_counts: list[DailyPoint]


class MinisterFlowResponse(BaseModel):
    minister_query: str
    minister_match_mode: str = "contains_casefold"
    minister_reference: str | None = None
    period: str
    status: Literal["ok", "empty"]
    collegiate_filter: CollegiateFilter
    event_count: int
    process_count: int
    active_day_count: int
    first_decision_date: date | None = None
    last_decision_date: date | None = None
    historical_reference_period_start: date | None = None
    historical_reference_period_end: date | None = None
    historical_event_count: int
    historical_active_day_count: int
    historical_average_events_per_active_day: float
    linked_alert_count: int
    thematic_key_rule: str
    thematic_source_distribution: dict[str, int]
    historical_thematic_source_distribution: dict[str, int]
    thematic_flow_interpretation_status: Literal["comparativo", "inconclusivo"]
    thematic_flow_interpretation_reasons: list[str]
    decision_type_distribution: dict[str, int]
    decision_progress_distribution: dict[str, int]
    judging_body_distribution: dict[str, int]
    collegiate_distribution: dict[str, int]
    process_class_distribution: dict[str, int]
    thematic_distribution: dict[str, int]
    daily_counts: list[DailyPoint]
    decision_type_flow: list[SegmentFlowItem]
    judging_body_flow: list[SegmentFlowItem]
    decision_progress_flow: list[SegmentFlowItem]
    process_class_flow: list[SegmentFlowItem]
    thematic_flow: list[SegmentFlowItem]


class MinisterProfileItem(BaseModel):
    minister: str
    period: str
    collegiate: CollegiateFilter
    event_count: int
    historical_average: float
    linked_alert_count: int
    process_classes: list[str]
    themes: list[str]


class EntitySummaryItem(BaseModel):
    id: str
    name_raw: str
    name_normalized: str
    associated_event_count: int
    distinct_process_count: int
    relation_level: RelationLevel
    role_labels: list[str] = []


class CaseSummaryItem(BaseModel):
    process_id: str
    process_number: str
    process_class: str
    decision_event_id: str
    decision_date: str
    decision_type: str
    decision_progress: str
    judging_body: str
    collegiate_label: str
    branch_of_law: str
    first_subject: str
    inteiro_teor_url: str | None = None
    doc_count_label: str
    acordao_label: str
    monocratic_decision_label: str
    origin_description: str
    decision_note_snippet: str


class AlertSummaryItem(BaseModel):
    alert_id: str
    process_id: str
    decision_event_id: str
    comparison_group_id: str
    alert_type: str
    alert_score: float
    ensemble_score: float | None = None
    expected_pattern: str
    observed_pattern: str
    evidence_summary: str
    uncertainty_note: str | None = None
    status: str
    risk_signal_count: int = 0
    risk_signals: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    process_number: str
    process_class: str
    decision_date: str
    decision_type: str
    decision_progress: str
    judging_body: str
    collegiate_label: str
    inteiro_teor_url: str | None = None
    doc_count_label: str
    acordao_label: str
    monocratic_decision_label: str
    branch_of_law: str
    first_subject: str
    origin_description: str
    decision_note_snippet: str


class MlOutlierScoreResponse(BaseModel):
    decision_event_id: str
    comparison_group_id: str
    ml_anomaly_score: float
    ml_rarity_score: float
    ensemble_score: float | None = None
    n_features: int
    n_samples: int
    generated_at: datetime | None = None


class DashboardResponse(BaseModel):
    filters: FilterOptionsResponse
    flow: MinisterFlowResponse
    kpis: MetricsSummary
    source_files: list[SourceAuditItem]
    minister_profiles: list[MinisterProfileItem]
    top_alerts: list[AlertSummaryItem]
    case_rows: list[CaseSummaryItem]
    top_counsels: list[EntitySummaryItem]
    top_parties: list[EntitySummaryItem]


class PaginatedAlertsResponse(BaseModel):
    filters: FilterOptionsResponse
    flow: MinisterFlowResponse
    source_files: list[SourceAuditItem]
    total: int
    page: int
    page_size: int
    items: list[AlertSummaryItem]
    top_counsels: list[EntitySummaryItem]
    top_parties: list[EntitySummaryItem]


class CaseDetailResponse(BaseModel):
    filters: FilterOptionsResponse
    flow: MinisterFlowResponse
    source_files: list[SourceAuditItem]
    case_item: CaseSummaryItem | None = None
    ml_outlier_analysis: MlOutlierScoreResponse | None = None
    related_alerts: list[AlertSummaryItem]
    counsels: list[EntitySummaryItem]
    parties: list[EntitySummaryItem]


class PaginatedCasesResponse(BaseModel):
    filters: FilterOptionsResponse
    flow: MinisterFlowResponse
    source_files: list[SourceAuditItem]
    total: int
    page: int
    page_size: int
    items: list[CaseSummaryItem]
