from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class EstablishmentSummary(BaseModel):
    cnpj_full: str = ""
    matriz_filial: str = ""
    nome_fantasia: str = ""
    uf: str = ""
    municipio_label: str = ""
    cnae_fiscal: str = ""
    cnae_label: str = ""
    situacao_cadastral: str = ""
    data_inicio_atividade: str = ""


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
    # Decoded labels
    minister_qualification_label: str | None = None
    entity_qualification_label: str | None = None
    company_natureza_juridica_label: str | None = None
    # Multi-establishment
    establishment_count: int | None = None
    active_establishment_count: int | None = None
    headquarters_uf: str | None = None
    headquarters_municipio_label: str | None = None
    headquarters_cnae_fiscal: str | None = None
    headquarters_cnae_label: str | None = None
    headquarters_situacao_cadastral: str | None = None
    headquarters_motivo_situacao_label: str | None = None
    establishment_ufs: list[str] = []
    establishment_cnaes: list[str] = []
    establishment_cnae_labels: list[str] = []
    key_establishments: list[EstablishmentSummary] = []
    # Economic group
    economic_group_id: str | None = None
    economic_group_member_count: int | None = None
    economic_group_razoes_sociais: list[str] = []
    # Provenance
    evidence_type: str | None = None
    source_dataset: str | None = None
    source_snapshot: str | None = None
    evidence_strength: str | None = None
    # Substantive rate
    favorable_rate_substantive: float | None = None
    substantive_decision_count: int | None = None
    red_flag_substantive: bool | None = None
    red_flag_power: float | None = None
    red_flag_confidence: Literal["high", "moderate", "low"] | None = None


class PaginatedCorporateConflictsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[CorporateConflictItem]


class CorporateConflictRedFlagsResponse(BaseModel):
    items: list[CorporateConflictItem]
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
    donation_total_brl: float | None = Field(
        default=None,
        description=(
            "Soma dos totais globais dos doadores vinculados a este par. "
            "Cada parcela é o total do doador em TODAS as eleições e partidos, "
            "não o subtotal das doações relevantes para este match. "
            "Usar donation_total_scope para confirmar a semântica."
        ),
    )
    donation_total_scope: Literal["donor_global_sum"] = Field(
        default="donor_global_sum",
        description=(
            "Escopo semântico de donation_total_brl: "
            "'donor_global_sum' = soma de totais globais por doador."
        ),
    )
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
    sanction_corporate_link_count: int = 0
    sanction_corporate_link_ids: list[str] = Field(default_factory=list)
    sanction_corporate_min_degree: int | None = None
    adjusted_rate_delta: float | None = None
    has_law_firm_group: bool = False
    donor_group_has_minister_partner: bool = False
    donor_group_has_party_partner: bool = False
    donor_group_has_counsel_partner: bool = False
    min_link_degree_to_minister: int | None = None


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
    adjusted_rate_delta: float | None = None


class CompoundRiskHeatmapResponse(BaseModel):
    pair_count: int
    display_limit: int
    ministers: list[str]
    entities: list[CompoundRiskHeatmapEntity]
    cells: list[CompoundRiskHeatmapCell]


class EconomicGroupItem(BaseModel):
    group_id: str
    member_cnpjs: list[str] = []
    razoes_sociais: list[str] = []
    member_count: int
    total_capital_social: float | None = None
    cnae_labels: list[str] = []
    ufs: list[str] = []
    active_establishment_count: int = 0
    total_establishment_count: int = 0
    is_law_firm_group: bool = False
    law_firm_member_count: int = 0
    law_firm_member_ratio: float = 0.0
    has_minister_partner: bool = False
    has_party_partner: bool = False
    has_counsel_partner: bool = False


class PaginatedEconomicGroupResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[EconomicGroupItem]


class PaymentCounterpartyItem(BaseModel):
    counterparty_id: str
    counterparty_identity_key: str
    identity_basis: str = ""
    counterparty_name: str
    counterparty_tax_id: str | None = None
    counterparty_tax_id_normalized: str | None = None
    counterparty_document_type: str = ""
    total_received_brl: float
    payment_count: int
    election_years: list[int] = Field(default_factory=list)
    payer_parties: list[str] = Field(default_factory=list)
    payer_actor_type: str = "party_org"
    first_payment_date: str | None = None
    last_payment_date: str | None = None
    states: list[str] = Field(default_factory=list)
    cnae_codes: list[str] = Field(default_factory=list)
    provenance: dict[str, Any] | None = Field(default=None)


class PaginatedPaymentCounterpartiesResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[PaymentCounterpartyItem] = Field(default_factory=list)


class SanctionCorporateLinkItem(BaseModel):
    link_id: str
    sanction_id: str
    sanction_source: str
    sanction_entity_name: str
    sanction_entity_tax_id: str | None = None
    sanction_type: str | None = None
    bridge_company_cnpj_basico: str
    bridge_company_name: str | None = None
    bridge_link_basis: str
    bridge_confidence: str = "deterministic"
    bridge_partner_role: str | None = None
    bridge_qualification_code: str | None = None
    bridge_qualification_label: str | None = None
    economic_group_id: str | None = None
    economic_group_member_count: int | None = None
    is_law_firm_group: bool | None = None
    stf_entity_type: str
    stf_entity_id: str
    stf_entity_name: str
    stf_match_strategy: str | None = None
    stf_match_score: float | None = None
    stf_match_confidence: str | None = None
    matched_alias: str | None = None
    matched_tax_id: str | None = None
    uncertainty_note: str | None = None
    link_degree: int = Field(default=2, ge=2)
    stf_process_count: int = 0
    favorable_rate: float | None = None
    baseline_favorable_rate: float | None = None
    favorable_rate_delta: float | None = None
    risk_score: float | None = None
    red_flag: bool = False
    red_flag_power: float | None = None
    red_flag_confidence: Literal["high", "moderate", "low"] | None = None
    evidence_chain: list[str] = Field(default_factory=list)
    source_datasets: list[str] = Field(default_factory=list)


class PaginatedSanctionCorporateLinksResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[SanctionCorporateLinkItem]


class SanctionCorporateLinkRedFlagsResponse(BaseModel):
    items: list[SanctionCorporateLinkItem]
    total: int
