from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from ._schemas_core import SourceAuditItem


class SanctionMatchItem(BaseModel):
    match_id: str
    entity_type: str = "party"
    party_id: str
    counsel_id: str | None = None
    party_name_normalized: str
    sanction_source: str
    sanction_id: str
    sanctioning_body: str | None = None
    sanction_type: str | None = None
    sanction_start_date: str | None = None
    sanction_end_date: str | None = None
    sanction_description: str | None = None
    stf_case_count: int
    favorable_rate: float | None = None
    baseline_favorable_rate: float | None = None
    favorable_rate_delta: float | None = None
    red_flag: bool
    match_strategy: str | None = None
    match_score: float | None = None
    match_confidence: str | None = None


class CounselSanctionProfileItem(BaseModel):
    counsel_id: str
    counsel_name_normalized: str
    sanctioned_client_count: int
    total_client_count: int
    sanctioned_client_rate: float
    sanctioned_favorable_rate: float | None = None
    overall_favorable_rate: float | None = None
    red_flag: bool


class PaginatedSanctionsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[SanctionMatchItem]


class SanctionRedFlagsResponse(BaseModel):
    party_flags: list[SanctionMatchItem]
    counsel_flags: list[CounselSanctionProfileItem]
    total_party_flags: int
    total_counsel_flags: int


class DonationMatchItem(BaseModel):
    match_id: str
    entity_type: str = "party"
    party_id: str
    counsel_id: str | None = None
    party_name_normalized: str
    donor_cpf_cnpj: str
    total_donated_brl: float
    donation_count: int
    election_years: list[int] = []
    parties_donated_to: list[str] = []
    candidates_donated_to: list[str] = []
    positions_donated_to: list[str] = []
    stf_case_count: int
    favorable_rate: float | None = None
    baseline_favorable_rate: float | None = None
    favorable_rate_delta: float | None = None
    red_flag: bool
    match_strategy: str | None = None
    match_score: float | None = None
    match_confidence: str | None = None


class CounselDonationProfileItem(BaseModel):
    counsel_id: str
    counsel_name_normalized: str
    donor_client_count: int
    total_client_count: int
    donor_client_rate: float
    donor_client_favorable_rate: float | None = None
    overall_favorable_rate: float | None = None
    red_flag: bool


class PaginatedDonationsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[DonationMatchItem]


class DonationRedFlagsResponse(BaseModel):
    party_flags: list[DonationMatchItem]
    counsel_flags: list[CounselDonationProfileItem]
    total_party_flags: int
    total_counsel_flags: int


class CorporateConflictItem(BaseModel):
    conflict_id: str
    minister_name: str
    company_cnpj_basico: str
    company_name: str
    minister_qualification: str | None = None
    linked_entity_type: str
    linked_entity_id: str
    linked_entity_name: str
    entity_qualification: str | None = None
    shared_process_ids: list[str] = []
    shared_process_count: int
    favorable_rate: float | None = None
    baseline_favorable_rate: float | None = None
    favorable_rate_delta: float | None = None
    risk_score: float | None = None
    decay_factor: float | None = None
    red_flag: bool
    link_chain: str | None = None
    link_degree: int = Field(default=1, ge=1)


class PaginatedCorporateConflictsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[CorporateConflictItem]


class CorporateConflictRedFlagsResponse(BaseModel):
    items: list[CorporateConflictItem]
    total: int


class CounselAffinityItem(BaseModel):
    affinity_id: str
    rapporteur: str
    counsel_id: str
    counsel_name_normalized: str
    shared_case_count: int
    favorable_count: int
    unfavorable_count: int
    pair_favorable_rate: float | None = None
    minister_baseline_favorable_rate: float | None = None
    counsel_baseline_favorable_rate: float | None = None
    pair_delta_vs_minister: float | None = None
    pair_delta_vs_counsel: float | None = None
    red_flag: bool
    top_process_classes: list[str] = []


class PaginatedCounselAffinityResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[CounselAffinityItem]


class CounselAffinityRedFlagsResponse(BaseModel):
    items: list[CounselAffinityItem]
    total: int


class CompoundRiskCompanyItem(BaseModel):
    company_cnpj_basico: str
    company_name: str
    link_degree: int


class CompoundRiskItem(BaseModel):
    pair_id: str
    minister_name: str
    entity_type: str
    entity_id: str
    entity_name: str
    signal_count: int
    signals: list[str] = []
    red_flag: bool
    shared_process_count: int
    shared_process_ids: list[str] = []
    alert_count: int
    alert_ids: list[str] = []
    max_alert_score: float | None = None
    max_rate_delta: float | None = None
    sanction_match_count: int
    sanction_sources: list[str] = []
    donation_match_count: int
    donation_total_brl: float | None = None
    corporate_conflict_count: int
    corporate_conflict_ids: list[str] = []
    corporate_companies: list[CompoundRiskCompanyItem] = []
    affinity_count: int
    affinity_ids: list[str] = []
    top_process_classes: list[str] = []
    supporting_party_ids: list[str] = []
    supporting_party_names: list[str] = []
    signal_details: dict[str, dict[str, Any]] | None = None
    earliest_year: int | None = None
    latest_year: int | None = None


class PaginatedCompoundRiskResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[CompoundRiskItem]


class CompoundRiskRedFlagsResponse(BaseModel):
    items: list[CompoundRiskItem]
    total: int


class CompoundRiskHeatmapEntity(BaseModel):
    entity_type: str
    entity_id: str
    entity_name: str


class CompoundRiskHeatmapCell(BaseModel):
    pair_id: str
    minister_name: str
    entity_type: str
    entity_id: str
    signal_count: int
    signals: list[str] = []
    red_flag: bool
    max_alert_score: float | None = None
    max_rate_delta: float | None = None


class CompoundRiskHeatmapResponse(BaseModel):
    pair_count: int
    display_limit: int
    ministers: list[str]
    entities: list[CompoundRiskHeatmapEntity]
    cells: list[CompoundRiskHeatmapCell]


class HealthResponse(BaseModel):
    status: str
    database_backend: str


class SourcesAuditResponse(BaseModel):
    source_files: list[SourceAuditItem]
    metrics: dict[str, int | float]
