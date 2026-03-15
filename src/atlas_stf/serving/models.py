from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ServingCase(Base):
    __tablename__ = "serving_case"

    decision_event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    process_number: Mapped[str | None] = mapped_column(String(128), index=True)
    process_class: Mapped[str | None] = mapped_column(String(64), index=True)
    branch_of_law: Mapped[str | None] = mapped_column(String(256))
    thematic_key: Mapped[str | None] = mapped_column(String(256), index=True)
    origin_description: Mapped[str | None] = mapped_column(String(256))
    inteiro_teor_url: Mapped[str | None] = mapped_column(Text())
    juris_doc_count: Mapped[int] = mapped_column(Integer, default=0)
    juris_has_acordao: Mapped[bool] = mapped_column(Boolean, default=False)
    juris_has_decisao_monocratica: Mapped[bool] = mapped_column(Boolean, default=False)
    decision_date: Mapped[date | None] = mapped_column(Date, index=True)
    period: Mapped[str | None] = mapped_column(String(7), index=True)
    current_rapporteur: Mapped[str | None] = mapped_column(String(256), index=True)
    decision_type: Mapped[str | None] = mapped_column(String(256), index=True)
    decision_progress: Mapped[str | None] = mapped_column(String(256), index=True)
    decision_origin: Mapped[str | None] = mapped_column(String(256))
    judging_body: Mapped[str | None] = mapped_column(String(256), index=True)
    is_collegiate: Mapped[bool | None] = mapped_column(Boolean, index=True)
    decision_note: Mapped[str | None] = mapped_column(Text())


Index(
    "ix_serving_case_filter_bundle",
    ServingCase.current_rapporteur,
    ServingCase.period,
    ServingCase.is_collegiate,
    ServingCase.judging_body,
    ServingCase.process_class,
)


class ServingAlert(Base):
    __tablename__ = "serving_alert"

    alert_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    decision_event_id: Mapped[str] = mapped_column(String(64), index=True)
    comparison_group_id: Mapped[str] = mapped_column(String(64), index=True)
    alert_type: Mapped[str] = mapped_column(String(64), index=True)
    alert_score: Mapped[float] = mapped_column(Float, index=True)
    expected_pattern: Mapped[str] = mapped_column(Text())
    observed_pattern: Mapped[str] = mapped_column(Text())
    evidence_summary: Mapped[str] = mapped_column(Text())
    uncertainty_note: Mapped[str | None] = mapped_column(Text())
    status: Mapped[str] = mapped_column(String(32), index=True)
    risk_signal_count: Mapped[int] = mapped_column(Integer, default=0)
    risk_signals_json: Mapped[str | None] = mapped_column(Text())
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingCounsel(Base):
    __tablename__ = "serving_counsel"

    counsel_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    counsel_name_raw: Mapped[str] = mapped_column(Text())
    counsel_name_normalized: Mapped[str] = mapped_column(String(512), index=True)
    notes: Mapped[str | None] = mapped_column(String(128))


class ServingProcessCounsel(Base):
    __tablename__ = "serving_process_counsel"

    link_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    counsel_id: Mapped[str] = mapped_column(String(64), index=True)
    side_in_case: Mapped[str | None] = mapped_column(String(128), index=True)
    source_id: Mapped[str | None] = mapped_column(String(128))


class ServingParty(Base):
    __tablename__ = "serving_party"

    party_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    party_name_raw: Mapped[str] = mapped_column(Text())
    party_name_normalized: Mapped[str] = mapped_column(String(512), index=True)
    notes: Mapped[str | None] = mapped_column(String(128))


class ServingProcessParty(Base):
    __tablename__ = "serving_process_party"

    link_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    party_id: Mapped[str] = mapped_column(String(64), index=True)
    role_in_case: Mapped[str | None] = mapped_column(String(128), index=True)
    source_id: Mapped[str | None] = mapped_column(String(128))


class ServingSourceAudit(Base):
    __tablename__ = "serving_source_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(String(64), index=True)
    category: Mapped[str] = mapped_column(String(32), index=True)
    relative_path: Mapped[str] = mapped_column(String(512), unique=True)
    checksum: Mapped[str] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ServingRapporteurProfile(Base):
    __tablename__ = "serving_rapporteur_profile"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rapporteur: Mapped[str] = mapped_column(String(256), index=True)
    process_class: Mapped[str] = mapped_column(String(64), index=True)
    thematic_key: Mapped[str] = mapped_column(String(256))
    decision_year: Mapped[int] = mapped_column(Integer, index=True)
    event_count: Mapped[int] = mapped_column(Integer)
    chi2_statistic: Mapped[float | None] = mapped_column(Float)
    p_value_approx: Mapped[float | None] = mapped_column(Float)
    deviation_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    deviation_direction: Mapped[str | None] = mapped_column(String(256))
    progress_distribution_json: Mapped[str | None] = mapped_column(Text())
    group_progress_distribution_json: Mapped[str | None] = mapped_column(Text())
    monocratic_event_count: Mapped[int] = mapped_column(Integer, default=0)
    monocratic_favorable_rate: Mapped[float | None] = mapped_column(Float)
    collegiate_event_count: Mapped[int] = mapped_column(Integer, default=0)
    collegiate_favorable_rate: Mapped[float | None] = mapped_column(Float)
    monocratic_blocking_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)


class ServingSequentialAnalysis(Base):
    __tablename__ = "serving_sequential_analysis"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rapporteur: Mapped[str] = mapped_column(String(256), index=True)
    decision_year: Mapped[int] = mapped_column(Integer, index=True)
    n_decisions: Mapped[int] = mapped_column(Integer)
    autocorrelation_lag1: Mapped[float] = mapped_column(Float)
    streak_effect_3: Mapped[float | None] = mapped_column(Float)
    streak_effect_5: Mapped[float | None] = mapped_column(Float)
    base_favorable_rate: Mapped[float] = mapped_column(Float)
    post_streak_favorable_rate_3: Mapped[float | None] = mapped_column(Float)
    post_streak_favorable_rate_5: Mapped[float | None] = mapped_column(Float)
    sequential_bias_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)


class ServingTemporalAnalysis(Base):
    __tablename__ = "serving_temporal_analysis"

    record_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    analysis_kind: Mapped[str] = mapped_column(String(64), index=True)
    rapporteur: Mapped[str | None] = mapped_column(String(256), index=True)
    decision_month: Mapped[str | None] = mapped_column(String(7), index=True)
    decision_year: Mapped[int | None] = mapped_column(Integer, index=True)
    month_of_year: Mapped[int | None] = mapped_column(Integer, index=True)
    process_class: Mapped[str | None] = mapped_column(String(64), index=True)
    decision_count: Mapped[int] = mapped_column(Integer, default=0)
    favorable_count: Mapped[int] = mapped_column(Integer, default=0)
    unfavorable_count: Mapped[int] = mapped_column(Integer, default=0)
    favorable_rate: Mapped[float | None] = mapped_column(Float)
    rolling_favorable_rate_6m: Mapped[float | None] = mapped_column(Float)
    breakpoint_score: Mapped[float | None] = mapped_column(Float)
    breakpoint_flag: Mapped[bool | None] = mapped_column(Boolean, index=True)
    current_favorable_rate: Mapped[float | None] = mapped_column(Float)
    prior_decision_count: Mapped[int | None] = mapped_column(Integer)
    prior_favorable_rate: Mapped[float | None] = mapped_column(Float)
    delta_vs_prior_year: Mapped[float | None] = mapped_column(Float)
    delta_vs_overall: Mapped[float | None] = mapped_column(Float)
    event_id: Mapped[str | None] = mapped_column(String(128), index=True)
    event_type: Mapped[str | None] = mapped_column(String(64), index=True)
    event_scope: Mapped[str | None] = mapped_column(String(32))
    event_date: Mapped[date | None] = mapped_column(Date, index=True)
    event_title: Mapped[str | None] = mapped_column(String(512))
    source: Mapped[str | None] = mapped_column(String(256))
    source_url: Mapped[str | None] = mapped_column(Text())
    status: Mapped[str | None] = mapped_column(String(32), index=True)
    before_decision_count: Mapped[int | None] = mapped_column(Integer)
    before_favorable_rate: Mapped[float | None] = mapped_column(Float)
    after_decision_count: Mapped[int | None] = mapped_column(Integer)
    after_favorable_rate: Mapped[float | None] = mapped_column(Float)
    delta_before_after: Mapped[float | None] = mapped_column(Float)
    linked_entity_type: Mapped[str | None] = mapped_column(String(32), index=True)
    linked_entity_id: Mapped[str | None] = mapped_column(String(64), index=True)
    linked_entity_name: Mapped[str | None] = mapped_column(String(512))
    company_cnpj_basico: Mapped[str | None] = mapped_column(String(16), index=True)
    company_name: Mapped[str | None] = mapped_column(String(512))
    link_degree: Mapped[int | None] = mapped_column(Integer, index=True)
    link_chain: Mapped[str | None] = mapped_column(Text())
    link_start_date: Mapped[date | None] = mapped_column(Date, index=True)
    link_status: Mapped[str | None] = mapped_column(String(64), index=True)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)


class ServingAssignmentAudit(Base):
    __tablename__ = "serving_assignment_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    process_class: Mapped[str] = mapped_column(String(64), index=True)
    decision_year: Mapped[int] = mapped_column(Integer, index=True)
    rapporteur_count: Mapped[int] = mapped_column(Integer)
    event_count: Mapped[int] = mapped_column(Integer)
    chi2_statistic: Mapped[float] = mapped_column(Float)
    p_value_approx: Mapped[float] = mapped_column(Float)
    uniformity_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    most_overrepresented_rapporteur: Mapped[str | None] = mapped_column(String(256))
    most_underrepresented_rapporteur: Mapped[str | None] = mapped_column(String(256))
    rapporteur_distribution_json: Mapped[str | None] = mapped_column(Text())


class ServingMlOutlierScore(Base):
    __tablename__ = "serving_ml_outlier_score"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_event_id: Mapped[str] = mapped_column(String(64), index=True)
    comparison_group_id: Mapped[str] = mapped_column(String(64), index=True)
    ml_anomaly_score: Mapped[float] = mapped_column(Float)
    ml_rarity_score: Mapped[float] = mapped_column(Float, index=True)
    ensemble_score: Mapped[float | None] = mapped_column(Float, index=True)
    n_features: Mapped[int] = mapped_column(Integer)
    n_samples: Mapped[int] = mapped_column(Integer)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingMinisterBio(Base):
    __tablename__ = "serving_minister_bio"

    minister_name: Mapped[str] = mapped_column(String(256), primary_key=True)
    appointment_date: Mapped[str | None] = mapped_column(String(10))
    appointing_president: Mapped[str | None] = mapped_column(String(256))
    birth_date: Mapped[str | None] = mapped_column(String(10))
    birth_state: Mapped[str | None] = mapped_column(String(128))
    career_summary: Mapped[str | None] = mapped_column(Text())
    political_party_history_json: Mapped[str | None] = mapped_column(Text())
    known_connections_json: Mapped[str | None] = mapped_column(Text())
    news_references_json: Mapped[str | None] = mapped_column(Text())


class ServingMinisterFlow(Base):
    __tablename__ = "serving_minister_flow"

    flow_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    minister_name: Mapped[str | None] = mapped_column(String(256), index=True)
    period: Mapped[str] = mapped_column(String(7), index=True)
    collegiate_filter: Mapped[str] = mapped_column(String(16), index=True)
    judging_body: Mapped[str | None] = mapped_column(String(256), index=True)
    process_class: Mapped[str | None] = mapped_column(String(64), index=True)
    minister_query: Mapped[str] = mapped_column(String(256), default="")
    minister_match_mode: Mapped[str] = mapped_column(String(32), default="contains_casefold")
    minister_reference: Mapped[str | None] = mapped_column(String(256))
    status: Mapped[str] = mapped_column(String(16), index=True)
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    process_count: Mapped[int] = mapped_column(Integer, default=0)
    active_day_count: Mapped[int] = mapped_column(Integer, default=0)
    first_decision_date: Mapped[date | None] = mapped_column(Date)
    last_decision_date: Mapped[date | None] = mapped_column(Date)
    historical_reference_period_start: Mapped[date | None] = mapped_column(Date)
    historical_reference_period_end: Mapped[date | None] = mapped_column(Date)
    historical_event_count: Mapped[int] = mapped_column(Integer, default=0)
    historical_active_day_count: Mapped[int] = mapped_column(Integer, default=0)
    historical_average_events_per_active_day: Mapped[float] = mapped_column(Float, default=0.0)
    linked_alert_count: Mapped[int] = mapped_column(Integer, default=0)
    thematic_key_rule: Mapped[str] = mapped_column(String(128))
    thematic_flow_interpretation_status: Mapped[str] = mapped_column(String(16))
    thematic_source_distribution_json: Mapped[str | None] = mapped_column(Text())
    historical_thematic_source_distribution_json: Mapped[str | None] = mapped_column(Text())
    thematic_flow_interpretation_reasons_json: Mapped[str | None] = mapped_column(Text())
    decision_type_distribution_json: Mapped[str | None] = mapped_column(Text())
    decision_progress_distribution_json: Mapped[str | None] = mapped_column(Text())
    judging_body_distribution_json: Mapped[str | None] = mapped_column(Text())
    collegiate_distribution_json: Mapped[str | None] = mapped_column(Text())
    process_class_distribution_json: Mapped[str | None] = mapped_column(Text())
    thematic_distribution_json: Mapped[str | None] = mapped_column(Text())
    daily_counts_json: Mapped[str | None] = mapped_column(Text())
    decision_type_flow_json: Mapped[str | None] = mapped_column(Text())
    judging_body_flow_json: Mapped[str | None] = mapped_column(Text())
    decision_progress_flow_json: Mapped[str | None] = mapped_column(Text())
    process_class_flow_json: Mapped[str | None] = mapped_column(Text())
    thematic_flow_json: Mapped[str | None] = mapped_column(Text())


Index(
    "ix_serving_minister_flow_lookup",
    ServingMinisterFlow.period,
    ServingMinisterFlow.collegiate_filter,
    ServingMinisterFlow.minister_name,
    ServingMinisterFlow.judging_body,
    ServingMinisterFlow.process_class,
)


class ServingOriginContext(Base):
    __tablename__ = "serving_origin_context"

    origin_index: Mapped[str] = mapped_column(String(128), primary_key=True)
    tribunal_label: Mapped[str] = mapped_column(String(64), index=True)
    state: Mapped[str] = mapped_column(String(2), index=True)
    datajud_total_processes: Mapped[int] = mapped_column(Integer)
    stf_process_count: Mapped[int] = mapped_column(Integer)
    stf_share_pct: Mapped[float] = mapped_column(Float)
    top_assuntos_json: Mapped[str | None] = mapped_column(Text())
    top_orgaos_julgadores_json: Mapped[str | None] = mapped_column(Text())
    class_distribution_json: Mapped[str | None] = mapped_column(Text())
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


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
    match_strategy: Mapped[str | None] = mapped_column(String(64))
    match_score: Mapped[float | None] = mapped_column(Float)
    match_confidence: Mapped[str | None] = mapped_column(String(64))
    matched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingCounselSanctionProfile(Base):
    __tablename__ = "serving_counsel_sanction_profile"

    counsel_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    counsel_name_normalized: Mapped[str] = mapped_column(String(512))
    sanctioned_client_count: Mapped[int] = mapped_column(Integer)
    total_client_count: Mapped[int] = mapped_column(Integer)
    sanctioned_client_rate: Mapped[float] = mapped_column(Float)
    sanctioned_favorable_rate: Mapped[float | None] = mapped_column(Float)
    overall_favorable_rate: Mapped[float | None] = mapped_column(Float)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)


class ServingDonationMatch(Base):
    __tablename__ = "serving_donation_match"

    match_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(16), default="party", index=True)
    party_id: Mapped[str] = mapped_column(String(64), index=True)
    party_name_normalized: Mapped[str] = mapped_column(String(512))
    donor_cpf_cnpj: Mapped[str] = mapped_column(String(20))
    total_donated_brl: Mapped[float] = mapped_column(Float)
    donation_count: Mapped[int] = mapped_column(Integer)
    election_years_json: Mapped[str | None] = mapped_column(Text())
    parties_donated_to_json: Mapped[str | None] = mapped_column(Text())
    candidates_donated_to_json: Mapped[str | None] = mapped_column(Text())
    positions_donated_to_json: Mapped[str | None] = mapped_column(Text())
    stf_case_count: Mapped[int] = mapped_column(Integer, default=0)
    favorable_rate: Mapped[float | None] = mapped_column(Float)
    baseline_favorable_rate: Mapped[float | None] = mapped_column(Float)
    favorable_rate_delta: Mapped[float | None] = mapped_column(Float)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    match_strategy: Mapped[str | None] = mapped_column(String(64))
    match_score: Mapped[float | None] = mapped_column(Float)
    match_confidence: Mapped[str | None] = mapped_column(String(64))
    matched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingCounselDonationProfile(Base):
    __tablename__ = "serving_counsel_donation_profile"

    counsel_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    counsel_name_normalized: Mapped[str] = mapped_column(String(512))
    donor_client_count: Mapped[int] = mapped_column(Integer)
    total_client_count: Mapped[int] = mapped_column(Integer)
    donor_client_rate: Mapped[float] = mapped_column(Float)
    donor_client_favorable_rate: Mapped[float | None] = mapped_column(Float)
    overall_favorable_rate: Mapped[float | None] = mapped_column(Float)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)


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


class ServingCounselAffinity(Base):
    __tablename__ = "serving_counsel_affinity"

    affinity_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    rapporteur: Mapped[str] = mapped_column(String(256), index=True)
    counsel_id: Mapped[str] = mapped_column(String(64), index=True)
    counsel_name_normalized: Mapped[str] = mapped_column(String(512))
    shared_case_count: Mapped[int] = mapped_column(Integer)
    favorable_count: Mapped[int] = mapped_column(Integer)
    unfavorable_count: Mapped[int] = mapped_column(Integer)
    pair_favorable_rate: Mapped[float | None] = mapped_column(Float)
    minister_baseline_favorable_rate: Mapped[float | None] = mapped_column(Float)
    counsel_baseline_favorable_rate: Mapped[float | None] = mapped_column(Float)
    pair_delta_vs_minister: Mapped[float | None] = mapped_column(Float)
    pair_delta_vs_counsel: Mapped[float | None] = mapped_column(Float)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    top_process_classes_json: Mapped[str | None] = mapped_column(Text())
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


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


class ServingCounselNetworkCluster(Base):
    __tablename__ = "serving_counsel_network_cluster"

    cluster_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    counsel_ids_json: Mapped[str | None] = mapped_column(Text())
    counsel_names_json: Mapped[str | None] = mapped_column(Text())
    cluster_size: Mapped[int] = mapped_column(Integer)
    shared_client_count: Mapped[int] = mapped_column(Integer, default=0)
    shared_process_count: Mapped[int] = mapped_column(Integer, default=0)
    minister_names_json: Mapped[str | None] = mapped_column(Text())
    cluster_favorable_rate: Mapped[float | None] = mapped_column(Float)
    cluster_case_count: Mapped[int] = mapped_column(Integer, default=0)
    red_flag: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingMetric(Base):
    __tablename__ = "serving_metric"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value_integer: Mapped[int | None] = mapped_column(Integer)
    value_float: Mapped[float | None] = mapped_column(Float)
    value_text: Mapped[str | None] = mapped_column(String(256))


class ServingSchemaMeta(Base):
    __tablename__ = "serving_schema_meta"

    singleton_key: Mapped[str] = mapped_column(String(32), primary_key=True, default="serving")
    schema_version: Mapped[int] = mapped_column(Integer)
    schema_fingerprint: Mapped[str] = mapped_column(String(64))
    built_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
