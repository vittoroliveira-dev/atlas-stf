from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class TemporalOverviewSummary(BaseModel):
    total_records: int
    counts_by_kind: dict[str, int]
    ministers_covered: int
    events_covered: int
    rolling_window_months: int
    event_window_days: int


class TemporalMinisterSummary(BaseModel):
    rapporteur: str
    record_count: int
    breakpoint_count: int
    latest_decision_month: str | None = None
    latest_breakpoint_month: str | None = None


class TemporalMonthlyItem(BaseModel):
    record_id: str
    rapporteur: str | None = None
    decision_month: str | None = None
    decision_year: int | None = None
    decision_count: int
    favorable_count: int
    unfavorable_count: int
    favorable_rate: float | None = None
    rolling_favorable_rate_6m: float | None = None
    breakpoint_score: float | None = None
    breakpoint_flag: bool | None = None
    generated_at: datetime | None = None


class TemporalYoyItem(BaseModel):
    record_id: str
    rapporteur: str | None = None
    process_class: str | None = None
    decision_year: int | None = None
    decision_count: int
    favorable_count: int
    unfavorable_count: int
    current_favorable_rate: float | None = None
    favorable_rate: float | None = None
    prior_decision_count: int | None = None
    prior_favorable_rate: float | None = None
    delta_vs_prior_year: float | None = None
    generated_at: datetime | None = None


class TemporalSeasonalityItem(BaseModel):
    record_id: str
    rapporteur: str | None = None
    month_of_year: int | None = None
    decision_count: int
    favorable_count: int
    unfavorable_count: int
    favorable_rate: float | None = None
    delta_vs_overall: float | None = None
    generated_at: datetime | None = None


class TemporalEventItem(BaseModel):
    record_id: str
    rapporteur: str | None = None
    event_id: str | None = None
    event_type: str | None = None
    event_scope: str | None = None
    event_date: date | None = None
    event_title: str | None = None
    source: str | None = None
    source_url: str | None = None
    status: str | None = None
    before_decision_count: int | None = None
    before_favorable_rate: float | None = None
    after_decision_count: int | None = None
    after_favorable_rate: float | None = None
    delta_before_after: float | None = None
    decision_count: int
    favorable_count: int
    unfavorable_count: int
    generated_at: datetime | None = None


class TemporalCorporateLinkItem(BaseModel):
    record_id: str
    rapporteur: str | None = None
    linked_entity_type: str | None = None
    linked_entity_id: str | None = None
    linked_entity_name: str | None = None
    company_cnpj_basico: str | None = None
    company_name: str | None = None
    link_degree: int | None = None
    link_chain: str | None = None
    link_start_date: date | None = None
    link_status: str | None = None
    decision_count: int
    favorable_count: int
    unfavorable_count: int
    favorable_rate: float | None = None
    generated_at: datetime | None = None


class TemporalAnalysisOverviewResponse(BaseModel):
    summary: TemporalOverviewSummary
    minister_summaries: list[TemporalMinisterSummary]
    breakpoints: list[TemporalMonthlyItem]
    seasonality: list[TemporalSeasonalityItem]
    events: list[TemporalEventItem]


class TemporalAnalysisMinisterResponse(BaseModel):
    minister: str
    rapporteur: str | None = None
    monthly: list[TemporalMonthlyItem]
    yoy: list[TemporalYoyItem]
    seasonality: list[TemporalSeasonalityItem]
    events: list[TemporalEventItem]
    corporate_links: list[TemporalCorporateLinkItem]
