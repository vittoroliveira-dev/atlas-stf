from __future__ import annotations

from typing import Literal

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
    red_flag_power: float | None = None
    red_flag_confidence: Literal["high", "moderate", "low"] | None = None
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
    entity_id: str = ""
    party_id: str
    counsel_id: str | None = None
    party_name_normalized: str
    donor_cpf_cnpj: str
    donor_name_normalized: str | None = None
    donor_name_originator: str | None = None
    total_donated_brl: float = Field(
        description="Total de doações do doador em TODAS as eleições e partidos (global).",
    )
    donation_count: int = Field(
        description="Contagem global de registros de doação do doador.",
    )
    matched_events_total_brl: float | None = Field(
        default=None,
        description="Soma real dos eventos vinculados a este match específico (subtotal contextual).",
    )
    matched_events_count: int | None = Field(
        default=None,
        description="Contagem dos eventos vinculados a este match específico.",
    )
    donation_scope: Literal["donor_global"] = Field(
        default="donor_global",
        description=(
            "Escopo de total_donated_brl e donation_count: "
            "'donor_global' = total do doador em todas as eleições/partidos."
        ),
    )
    election_years: list[int] = []
    parties_donated_to: list[str] = []
    candidates_donated_to: list[str] = []
    positions_donated_to: list[str] = []
    stf_case_count: int
    favorable_rate: float | None = None
    favorable_rate_substantive: float | None = None
    substantive_decision_count: int | None = None
    baseline_favorable_rate: float | None = None
    favorable_rate_delta: float | None = None
    red_flag: bool
    red_flag_substantive: bool | None = None
    red_flag_power: float | None = None
    red_flag_confidence: Literal["high", "moderate", "low"] | None = None
    match_strategy: str | None = None
    match_score: float | None = None
    match_confidence: str | None = None
    matched_alias: str | None = None
    matched_tax_id: str | None = None
    uncertainty_note: str | None = None
    donor_identity_key: str | None = None
    # Corporate enrichment
    donor_document_type: str | None = None
    donor_tax_id_normalized: str | None = None
    donor_cnpj_basico: str | None = None
    donor_company_name: str | None = None
    economic_group_id: str | None = None
    economic_group_member_count: int | None = None
    is_law_firm_group: bool | None = None
    donor_group_has_minister_partner: bool | None = None
    donor_group_has_party_partner: bool | None = None
    donor_group_has_counsel_partner: bool | None = None
    min_link_degree_to_minister: int | None = None
    corporate_link_red_flag: bool | None = None
    resource_types_observed: list[str] = Field(default_factory=list)
    # Temporal / concentration metrics
    first_donation_date: str | None = None
    last_donation_date: str | None = None
    active_election_year_count: int = 0
    max_single_donation_brl: float = 0.0
    avg_donation_brl: float = 0.0
    top_candidate_share: float | None = None
    top_party_share: float | None = None
    top_state_share: float | None = None
    donation_year_span: int | None = None
    recent_donation_flag: bool = False


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


class DonationEventItem(BaseModel):
    event_id: str
    match_id: str
    election_year: int | None = None
    donation_date: str | None = None
    donation_amount: float = 0.0
    candidate_name: str | None = None
    party_abbrev: str | None = None
    position: str | None = None
    state: str | None = None
    donor_name: str | None = None
    donor_name_originator: str | None = None
    donor_cpf_cnpj: str | None = None
    donation_description: str | None = None
    donor_identity_key: str | None = None
    resource_type_category: str | None = None
    resource_type_subtype: str | None = None
    resource_classification_confidence: str | None = None
    resource_classification_rule: str | None = None
    source_file: str | None = None
    collected_at: str | None = None
    source_url: str | None = None
    ingest_run_id: str | None = None
    record_hash: str | None = None


class PaginatedDonationEventsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[DonationEventItem]


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


class HealthResponse(BaseModel):
    status: str
    database_backend: str


class SourcesAuditResponse(BaseModel):
    source_files: list[SourceAuditItem]
    metrics: dict[str, int | float]
