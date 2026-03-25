from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .models import Base

__all__ = [
    "ServingSanctionMatch",
    "ServingDonationMatch",
    "ServingDonationEvent",
    "ServingCorporateConflict",
    "ServingCompoundRisk",
    "ServingDecisionVelocity",
    "ServingRapporteurChange",
    "ServingPaymentCounterparty",
    "ServingEconomicGroup",
    "ServingSanctionCorporateLink",
]


class ServingSanctionMatch(Base):
    __tablename__ = "serving_sanction_match"

    match_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(16), default="party", index=True)
    party_id: Mapped[str] = mapped_column(String(64), index=True)
    party_name_normalized: Mapped[str] = mapped_column(String(512))
    sanction_source: Mapped[str] = mapped_column(String(16), index=True)
    sanction_id: Mapped[str] = mapped_column(String(128))
    sanctioning_body: Mapped[str | None] = mapped_column(String(512))
    sanction_type: Mapped[str | None] = mapped_column(String(256))
    sanction_start_date: Mapped[str | None] = mapped_column(String(10))
    sanction_end_date: Mapped[str | None] = mapped_column(String(10))
    sanction_description: Mapped[str | None] = mapped_column(Text())
    stf_case_count: Mapped[int] = mapped_column(Integer, default=0)
    favorable_rate: Mapped[float | None] = mapped_column(Float)
    baseline_favorable_rate: Mapped[float | None] = mapped_column(Float)
    favorable_rate_delta: Mapped[float | None] = mapped_column(Float)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    red_flag_power: Mapped[float | None] = mapped_column(Float)
    red_flag_confidence: Mapped[str | None] = mapped_column(String(16))
    match_strategy: Mapped[str | None] = mapped_column(String(64))
    match_score: Mapped[float | None] = mapped_column(Float)
    match_confidence: Mapped[str | None] = mapped_column(String(64))
    matched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingDonationMatch(Base):
    __tablename__ = "serving_donation_match"

    match_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(16), default="party", index=True)
    entity_id: Mapped[str] = mapped_column(String(64), index=True, default="")
    party_id: Mapped[str] = mapped_column(String(64), index=True)
    party_name_normalized: Mapped[str] = mapped_column(String(512))
    donor_cpf_cnpj: Mapped[str] = mapped_column(String(20))
    donor_name_normalized: Mapped[str | None] = mapped_column(String(512))
    donor_name_originator: Mapped[str | None] = mapped_column(String(512))
    total_donated_brl: Mapped[float] = mapped_column(Float)
    donation_count: Mapped[int] = mapped_column(Integer)
    election_years_json: Mapped[str | None] = mapped_column(Text())
    parties_donated_to_json: Mapped[str | None] = mapped_column(Text())
    candidates_donated_to_json: Mapped[str | None] = mapped_column(Text())
    positions_donated_to_json: Mapped[str | None] = mapped_column(Text())
    stf_case_count: Mapped[int] = mapped_column(Integer, default=0)
    favorable_rate: Mapped[float | None] = mapped_column(Float)
    favorable_rate_substantive: Mapped[float | None] = mapped_column(Float)
    substantive_decision_count: Mapped[int | None] = mapped_column(Integer)
    baseline_favorable_rate: Mapped[float | None] = mapped_column(Float)
    favorable_rate_delta: Mapped[float | None] = mapped_column(Float)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    red_flag_substantive: Mapped[bool | None] = mapped_column(Boolean)
    red_flag_power: Mapped[float | None] = mapped_column(Float)
    red_flag_confidence: Mapped[str | None] = mapped_column(String(16))
    match_strategy: Mapped[str | None] = mapped_column(String(64))
    match_score: Mapped[float | None] = mapped_column(Float)
    match_confidence: Mapped[str | None] = mapped_column(String(64))
    matched_alias: Mapped[str | None] = mapped_column(String(512))
    matched_tax_id: Mapped[str | None] = mapped_column(String(20))
    uncertainty_note: Mapped[str | None] = mapped_column(String(256))
    matched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    donor_identity_key: Mapped[str | None] = mapped_column(String(512), index=True)
    # Corporate enrichment fields
    donor_document_type: Mapped[str | None] = mapped_column(String(16))
    donor_tax_id_normalized: Mapped[str | None] = mapped_column(String(20))
    donor_cnpj_basico: Mapped[str | None] = mapped_column(String(8))
    donor_company_name: Mapped[str | None] = mapped_column(String(512))
    economic_group_id: Mapped[str | None] = mapped_column(String(64), index=True)
    economic_group_member_count: Mapped[int | None] = mapped_column(Integer)
    is_law_firm_group: Mapped[bool | None] = mapped_column(Boolean)
    donor_group_has_minister_partner: Mapped[bool | None] = mapped_column(Boolean)
    donor_group_has_party_partner: Mapped[bool | None] = mapped_column(Boolean)
    donor_group_has_counsel_partner: Mapped[bool | None] = mapped_column(Boolean)
    min_link_degree_to_minister: Mapped[int | None] = mapped_column(Integer)
    corporate_link_red_flag: Mapped[bool | None] = mapped_column(Boolean)
    resource_types_observed_json: Mapped[str | None] = mapped_column(Text())
    # Temporal / concentration metrics
    first_donation_date: Mapped[str | None] = mapped_column(String(10))
    last_donation_date: Mapped[str | None] = mapped_column(String(10))
    active_election_year_count: Mapped[int] = mapped_column(Integer, default=0)
    max_single_donation_brl: Mapped[float] = mapped_column(Float, default=0.0)
    avg_donation_brl: Mapped[float] = mapped_column(Float, default=0.0)
    top_candidate_share: Mapped[float | None] = mapped_column(Float)
    top_party_share: Mapped[float | None] = mapped_column(Float)
    top_state_share: Mapped[float | None] = mapped_column(Float)
    donation_year_span: Mapped[int | None] = mapped_column(Integer)
    recent_donation_flag: Mapped[bool] = mapped_column(Boolean, default=False)


class ServingDonationEvent(Base):
    __tablename__ = "serving_donation_event"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    match_id: Mapped[str] = mapped_column(String(64), index=True)
    election_year: Mapped[int | None] = mapped_column(Integer, index=True)
    donation_date: Mapped[date | None] = mapped_column(Date)
    donation_amount: Mapped[float] = mapped_column(Float, default=0.0)
    candidate_name: Mapped[str | None] = mapped_column(String(512))
    party_abbrev: Mapped[str | None] = mapped_column(String(32))
    position: Mapped[str | None] = mapped_column(String(128))
    state: Mapped[str | None] = mapped_column(String(4))
    donor_name: Mapped[str | None] = mapped_column(String(512))
    donor_name_originator: Mapped[str | None] = mapped_column(String(512))
    donor_cpf_cnpj: Mapped[str | None] = mapped_column(String(20))
    donation_description: Mapped[str | None] = mapped_column(String(512))
    donor_identity_key: Mapped[str | None] = mapped_column(String(512), index=True)
    resource_type_category: Mapped[str | None] = mapped_column(String(32))
    resource_type_subtype: Mapped[str | None] = mapped_column(String(64))
    resource_classification_confidence: Mapped[str | None] = mapped_column(String(8))
    resource_classification_rule: Mapped[str | None] = mapped_column(String(128))
    source_file: Mapped[str | None] = mapped_column(String(512))
    collected_at: Mapped[str | None] = mapped_column(String(32))
    source_url: Mapped[str | None] = mapped_column(Text())
    ingest_run_id: Mapped[str | None] = mapped_column(String(36))
    record_hash: Mapped[str | None] = mapped_column(String(64))


class ServingCorporateConflict(Base):
    __tablename__ = "serving_corporate_conflict"

    conflict_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    minister_name: Mapped[str] = mapped_column(String(256), index=True)
    company_cnpj_basico: Mapped[str] = mapped_column(String(8))
    company_name: Mapped[str] = mapped_column(String(512))
    minister_qualification: Mapped[str | None] = mapped_column(String(64))
    linked_entity_type: Mapped[str] = mapped_column(String(16), index=True)
    linked_entity_id: Mapped[str] = mapped_column(String(64), index=True)
    linked_entity_name: Mapped[str] = mapped_column(String(512))
    entity_qualification: Mapped[str | None] = mapped_column(String(64))
    shared_process_ids_json: Mapped[str | None] = mapped_column(Text())
    shared_process_count: Mapped[int] = mapped_column(Integer, default=0)
    favorable_rate: Mapped[float | None] = mapped_column(Float)
    baseline_favorable_rate: Mapped[float | None] = mapped_column(Float)
    favorable_rate_delta: Mapped[float | None] = mapped_column(Float)
    risk_score: Mapped[float | None] = mapped_column(Float, index=True)
    decay_factor: Mapped[float | None] = mapped_column(Float)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    link_chain: Mapped[str | None] = mapped_column(Text())
    link_degree: Mapped[int] = mapped_column(Integer, default=1)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Decoded labels
    minister_qualification_label: Mapped[str | None] = mapped_column(String(256))
    entity_qualification_label: Mapped[str | None] = mapped_column(String(256))
    company_natureza_juridica_label: Mapped[str | None] = mapped_column(String(256))
    # Multi-establishment
    establishment_count: Mapped[int | None] = mapped_column(Integer)
    active_establishment_count: Mapped[int | None] = mapped_column(Integer)
    headquarters_uf: Mapped[str | None] = mapped_column(String(2))
    headquarters_municipio_label: Mapped[str | None] = mapped_column(String(256))
    headquarters_cnae_fiscal: Mapped[str | None] = mapped_column(String(16))
    headquarters_cnae_label: Mapped[str | None] = mapped_column(String(512))
    headquarters_situacao_cadastral: Mapped[str | None] = mapped_column(String(2))
    headquarters_motivo_situacao_label: Mapped[str | None] = mapped_column(String(512))
    establishment_ufs_json: Mapped[str | None] = mapped_column(Text())
    establishment_cnaes_json: Mapped[str | None] = mapped_column(Text())
    establishment_cnae_labels_json: Mapped[str | None] = mapped_column(Text())
    key_establishments_json: Mapped[str | None] = mapped_column(Text())
    # Economic group
    economic_group_id: Mapped[str | None] = mapped_column(String(64))
    economic_group_member_count: Mapped[int | None] = mapped_column(Integer)
    economic_group_razoes_sociais_json: Mapped[str | None] = mapped_column(Text())
    # Provenance
    evidence_type: Mapped[str | None] = mapped_column(String(32))
    source_dataset: Mapped[str | None] = mapped_column(String(32))
    source_snapshot: Mapped[str | None] = mapped_column(String(7))
    evidence_strength: Mapped[str | None] = mapped_column(String(16))
    # Substantive rate
    favorable_rate_substantive: Mapped[float | None] = mapped_column(Float)
    substantive_decision_count: Mapped[int | None] = mapped_column(Integer)
    red_flag_substantive: Mapped[bool | None] = mapped_column(Boolean)
    red_flag_power: Mapped[float | None] = mapped_column(Float)
    red_flag_confidence: Mapped[str | None] = mapped_column(String(16))


class ServingCompoundRisk(Base):
    __tablename__ = "serving_compound_risk"

    pair_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    minister_name: Mapped[str] = mapped_column(String(256), index=True)
    entity_type: Mapped[str] = mapped_column(String(16), index=True)
    entity_id: Mapped[str] = mapped_column(String(64), index=True)
    entity_name: Mapped[str] = mapped_column(String(512))
    signal_count: Mapped[int] = mapped_column(Integer, index=True)
    signals_json: Mapped[str | None] = mapped_column(Text())
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    shared_process_count: Mapped[int] = mapped_column(Integer, default=0)
    shared_process_ids_json: Mapped[str | None] = mapped_column(Text())
    alert_count: Mapped[int] = mapped_column(Integer, default=0)
    alert_ids_json: Mapped[str | None] = mapped_column(Text())
    max_alert_score: Mapped[float | None] = mapped_column(Float, index=True)
    max_rate_delta: Mapped[float | None] = mapped_column(Float, index=True)
    sanction_match_count: Mapped[int] = mapped_column(Integer, default=0)
    sanction_sources_json: Mapped[str | None] = mapped_column(Text())
    donation_match_count: Mapped[int] = mapped_column(Integer, default=0)
    donation_total_brl: Mapped[float | None] = mapped_column(Float)
    corporate_conflict_count: Mapped[int] = mapped_column(Integer, default=0)
    corporate_conflict_ids_json: Mapped[str | None] = mapped_column(Text())
    corporate_companies_json: Mapped[str | None] = mapped_column(Text())
    affinity_count: Mapped[int] = mapped_column(Integer, default=0)
    affinity_ids_json: Mapped[str | None] = mapped_column(Text())
    top_process_classes_json: Mapped[str | None] = mapped_column(Text())
    supporting_party_ids_json: Mapped[str | None] = mapped_column(Text())
    supporting_party_names_json: Mapped[str | None] = mapped_column(Text())
    signal_details_json: Mapped[str | None] = mapped_column(Text())
    earliest_year: Mapped[int | None] = mapped_column(Integer)
    latest_year: Mapped[int | None] = mapped_column(Integer)
    sanction_corporate_link_count: Mapped[int] = mapped_column(Integer, default=0)
    sanction_corporate_link_ids_json: Mapped[str | None] = mapped_column(Text())
    sanction_corporate_min_degree: Mapped[int | None] = mapped_column(Integer)
    adjusted_rate_delta: Mapped[float | None] = mapped_column(Float, index=True)
    has_law_firm_group: Mapped[bool] = mapped_column(Boolean, default=False)
    donor_group_has_minister_partner: Mapped[bool] = mapped_column(Boolean, default=False)
    donor_group_has_party_partner: Mapped[bool] = mapped_column(Boolean, default=False)
    donor_group_has_counsel_partner: Mapped[bool] = mapped_column(Boolean, default=False)
    min_link_degree_to_minister: Mapped[int | None] = mapped_column(Integer)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)


class ServingDecisionVelocity(Base):
    __tablename__ = "serving_decision_velocity"

    velocity_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    decision_event_id: Mapped[str] = mapped_column(String(64), index=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    current_rapporteur: Mapped[str | None] = mapped_column(String(256), index=True)
    decision_date: Mapped[str | None] = mapped_column(String(10))
    filing_date: Mapped[str | None] = mapped_column(String(10))
    days_to_decision: Mapped[int] = mapped_column(Integer)
    process_class: Mapped[str | None] = mapped_column(String(64), index=True)
    thematic_key: Mapped[str | None] = mapped_column(String(256))
    decision_year: Mapped[int | None] = mapped_column(Integer, index=True)
    group_size: Mapped[int | None] = mapped_column(Integer)
    p5_days: Mapped[float | None] = mapped_column(Float)
    p10_days: Mapped[float | None] = mapped_column(Float)
    median_days: Mapped[float | None] = mapped_column(Float)
    p90_days: Mapped[float | None] = mapped_column(Float)
    p95_days: Mapped[float | None] = mapped_column(Float)
    velocity_flag: Mapped[str | None] = mapped_column(String(32), index=True)
    velocity_z_score: Mapped[float | None] = mapped_column(Float)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingRapporteurChange(Base):
    __tablename__ = "serving_rapporteur_change"

    change_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    process_class: Mapped[str | None] = mapped_column(String(64), index=True)
    previous_rapporteur: Mapped[str] = mapped_column(String(256), index=True)
    new_rapporteur: Mapped[str] = mapped_column(String(256), index=True)
    change_date: Mapped[str | None] = mapped_column(String(10))
    decision_event_id: Mapped[str | None] = mapped_column(String(64))
    post_change_decision_count: Mapped[int] = mapped_column(Integer, default=0)
    post_change_favorable_rate: Mapped[float | None] = mapped_column(Float)
    new_rapporteur_baseline_rate: Mapped[float | None] = mapped_column(Float)
    delta_vs_baseline: Mapped[float | None] = mapped_column(Float)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingPaymentCounterparty(Base):
    __tablename__ = "serving_payment_counterparty"

    counterparty_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    counterparty_identity_key: Mapped[str] = mapped_column(String(512), index=True)
    identity_basis: Mapped[str] = mapped_column(String(16), default="")
    counterparty_name: Mapped[str] = mapped_column(String(512))
    counterparty_tax_id: Mapped[str | None] = mapped_column(String(20))
    counterparty_tax_id_normalized: Mapped[str | None] = mapped_column(String(14))
    counterparty_document_type: Mapped[str] = mapped_column(String(8), default="")
    total_received_brl: Mapped[float] = mapped_column(Float, index=True)
    payment_count: Mapped[int] = mapped_column(Integer)
    election_years_json: Mapped[str | None] = mapped_column(Text())
    payer_parties_json: Mapped[str | None] = mapped_column(Text())
    payer_actor_type: Mapped[str] = mapped_column(String(32), default="party_org")
    first_payment_date: Mapped[str | None] = mapped_column(String(10))
    last_payment_date: Mapped[str | None] = mapped_column(String(10))
    states_json: Mapped[str | None] = mapped_column(Text())
    cnae_codes_json: Mapped[str | None] = mapped_column(Text())
    provenance_json: Mapped[str | None] = mapped_column(Text())
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingEconomicGroup(Base):
    __tablename__ = "serving_economic_group"

    group_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    member_cnpjs_json: Mapped[str | None] = mapped_column(Text())
    razoes_sociais_json: Mapped[str | None] = mapped_column(Text())
    member_count: Mapped[int] = mapped_column(Integer)
    total_capital_social: Mapped[float | None] = mapped_column(Float)
    cnae_labels_json: Mapped[str | None] = mapped_column(Text())
    ufs_json: Mapped[str | None] = mapped_column(Text())
    active_establishment_count: Mapped[int] = mapped_column(Integer, default=0)
    total_establishment_count: Mapped[int] = mapped_column(Integer, default=0)
    is_law_firm_group: Mapped[bool] = mapped_column(Boolean, default=False)
    law_firm_member_count: Mapped[int] = mapped_column(Integer, default=0)
    law_firm_member_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    has_minister_partner: Mapped[bool] = mapped_column(Boolean, default=False)
    has_party_partner: Mapped[bool] = mapped_column(Boolean, default=False)
    has_counsel_partner: Mapped[bool] = mapped_column(Boolean, default=False)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingSanctionCorporateLink(Base):
    __tablename__ = "serving_sanction_corporate_link"

    link_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    sanction_id: Mapped[str] = mapped_column(String(128))
    sanction_source: Mapped[str] = mapped_column(String(16), index=True)
    sanction_entity_name: Mapped[str] = mapped_column(String(512))
    sanction_entity_tax_id: Mapped[str | None] = mapped_column(String(20))
    sanction_type: Mapped[str | None] = mapped_column(String(256))
    bridge_company_cnpj_basico: Mapped[str] = mapped_column(String(8))
    bridge_company_name: Mapped[str | None] = mapped_column(String(512))
    bridge_link_basis: Mapped[str] = mapped_column(String(32))
    bridge_confidence: Mapped[str] = mapped_column(String(16), default="deterministic")
    bridge_partner_role: Mapped[str | None] = mapped_column(String(256))
    bridge_qualification_code: Mapped[str | None] = mapped_column(String(8))
    bridge_qualification_label: Mapped[str | None] = mapped_column(String(256))
    economic_group_id: Mapped[str | None] = mapped_column(String(64), index=True)
    economic_group_member_count: Mapped[int | None] = mapped_column(Integer)
    is_law_firm_group: Mapped[bool | None] = mapped_column(Boolean)
    stf_entity_type: Mapped[str] = mapped_column(String(16), index=True)
    stf_entity_id: Mapped[str] = mapped_column(String(64), index=True)
    stf_entity_name: Mapped[str] = mapped_column(String(512))
    stf_match_strategy: Mapped[str | None] = mapped_column(String(64))
    stf_match_score: Mapped[float | None] = mapped_column(Float)
    stf_match_confidence: Mapped[str | None] = mapped_column(String(64))
    matched_alias: Mapped[str | None] = mapped_column(String(512))
    matched_tax_id: Mapped[str | None] = mapped_column(String(20))
    uncertainty_note: Mapped[str | None] = mapped_column(String(256))
    link_degree: Mapped[int] = mapped_column(Integer, index=True, default=2)
    stf_process_count: Mapped[int] = mapped_column(Integer, default=0)
    favorable_rate: Mapped[float | None] = mapped_column(Float)
    baseline_favorable_rate: Mapped[float | None] = mapped_column(Float)
    favorable_rate_delta: Mapped[float | None] = mapped_column(Float)
    risk_score: Mapped[float | None] = mapped_column(Float, index=True)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    red_flag_power: Mapped[float | None] = mapped_column(Float)
    red_flag_confidence: Mapped[str | None] = mapped_column(String(16))
    evidence_chain_json: Mapped[str | None] = mapped_column(Text())
    source_datasets_json: Mapped[str | None] = mapped_column(Text())
    record_hash: Mapped[str | None] = mapped_column(String(64))
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
