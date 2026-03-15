from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session

from ._builder_flow import FLOW_SHAPES as FLOW_SHAPES  # re-export
from ._builder_flow import _materialize_minister_flows
from ._builder_loaders import (
    build_source_audits,
    load_alerts,
    load_assignment_audits,
    load_cases,
    load_compound_risks,
    load_corporate_conflicts,
    load_counsel_affinities,
    load_counsel_network_clusters,
    load_counsels,
    load_decision_velocities,
    load_donation_matches,
    load_metrics,
    load_minister_bios,
    load_ml_outlier_scores,
    load_movements,
    load_origin_contexts,
    load_parties,
    load_rapporteur_changes,
    load_rapporteur_profiles,
    load_sanction_matches,
    load_sequential_analyses,
    load_session_events,
    load_temporal_analyses,
)
from ._builder_schema import (
    SERVING_SCHEMA_SINGLETON_KEY,
    SERVING_SCHEMA_VERSION,
    _ensure_compatible_schema,
    _serving_schema_fingerprint,
)
from ._builder_utils import ServingBuildResult, SourceFile
from .models import (
    ServingAlert,
    ServingAssignmentAudit,
    ServingCase,
    ServingCompoundRisk,
    ServingCorporateConflict,
    ServingCounsel,
    ServingCounselAffinity,
    ServingCounselDonationProfile,
    ServingCounselNetworkCluster,
    ServingCounselSanctionProfile,
    ServingDecisionVelocity,
    ServingDonationMatch,
    ServingMetric,
    ServingMinisterBio,
    ServingMinisterFlow,
    ServingMlOutlierScore,
    ServingMovement,
    ServingOriginContext,
    ServingParty,
    ServingProcessCounsel,
    ServingProcessParty,
    ServingRapporteurChange,
    ServingRapporteurProfile,
    ServingSanctionMatch,
    ServingSchemaMeta,
    ServingSequentialAnalysis,
    ServingSessionEvent,
    ServingSourceAudit,
    ServingTemporalAnalysis,
)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")


def build_serving_database(
    *,
    database_url: str,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    analytics_dir: Path = DEFAULT_ANALYTICS_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> ServingBuildResult:
    engine = create_engine(database_url)
    try:
        _ensure_compatible_schema(engine)
        source_files = [
            SourceFile("process", "curated", curated_dir / "process.jsonl"),
            SourceFile("decision_event", "curated", curated_dir / "decision_event.jsonl"),
            SourceFile("party", "curated", curated_dir / "party.jsonl"),
            SourceFile("process_party_link", "curated", curated_dir / "process_party_link.jsonl"),
            SourceFile("counsel", "curated", curated_dir / "counsel.jsonl"),
            SourceFile("process_counsel_link", "curated", curated_dir / "process_counsel_link.jsonl"),
            SourceFile("outlier_alert", "analytics", analytics_dir / "outlier_alert.jsonl"),
            SourceFile("outlier_alert_summary", "analytics", analytics_dir / "outlier_alert_summary.json"),
            SourceFile("comparison_group_summary", "analytics", analytics_dir / "comparison_group_summary.json"),
            SourceFile("baseline_summary", "analytics", analytics_dir / "baseline_summary.json"),
        ]
        optional_source_files = [
            SourceFile("rapporteur_profile", "analytics", analytics_dir / "rapporteur_profile.jsonl"),
            SourceFile("rapporteur_profile_summary", "analytics", analytics_dir / "rapporteur_profile_summary.json"),
            SourceFile("sequential_analysis", "analytics", analytics_dir / "sequential_analysis.jsonl"),
            SourceFile("sequential_analysis_summary", "analytics", analytics_dir / "sequential_analysis_summary.json"),
            SourceFile("assignment_audit", "analytics", analytics_dir / "assignment_audit.jsonl"),
            SourceFile("assignment_audit_summary", "analytics", analytics_dir / "assignment_audit_summary.json"),
            SourceFile("ml_outlier_score", "analytics", analytics_dir / "ml_outlier_score.jsonl"),
            SourceFile("ml_outlier_score_summary", "analytics", analytics_dir / "ml_outlier_score_summary.json"),
            SourceFile("sanction_match", "analytics", analytics_dir / "sanction_match.jsonl"),
            SourceFile("sanction_match_summary", "analytics", analytics_dir / "sanction_match_summary.json"),
            SourceFile("counsel_sanction_profile", "analytics", analytics_dir / "counsel_sanction_profile.jsonl"),
            SourceFile("donation_match", "analytics", analytics_dir / "donation_match.jsonl"),
            SourceFile("donation_match_summary", "analytics", analytics_dir / "donation_match_summary.json"),
            SourceFile("counsel_donation_profile", "analytics", analytics_dir / "counsel_donation_profile.jsonl"),
            SourceFile("corporate_network", "analytics", analytics_dir / "corporate_network.jsonl"),
            SourceFile("corporate_network_summary", "analytics", analytics_dir / "corporate_network_summary.json"),
            SourceFile("counsel_affinity", "analytics", analytics_dir / "counsel_affinity.jsonl"),
            SourceFile("counsel_affinity_summary", "analytics", analytics_dir / "counsel_affinity_summary.json"),
            SourceFile("compound_risk", "analytics", analytics_dir / "compound_risk.jsonl"),
            SourceFile("compound_risk_summary", "analytics", analytics_dir / "compound_risk_summary.json"),
            SourceFile("temporal_analysis", "analytics", analytics_dir / "temporal_analysis.jsonl"),
            SourceFile("temporal_analysis_summary", "analytics", analytics_dir / "temporal_analysis_summary.json"),
            SourceFile("decision_velocity", "analytics", analytics_dir / "decision_velocity.jsonl"),
            SourceFile("decision_velocity_summary", "analytics", analytics_dir / "decision_velocity_summary.json"),
            SourceFile("rapporteur_change", "analytics", analytics_dir / "rapporteur_change.jsonl"),
            SourceFile("rapporteur_change_summary", "analytics", analytics_dir / "rapporteur_change_summary.json"),
            SourceFile("counsel_network_cluster", "analytics", analytics_dir / "counsel_network_cluster.jsonl"),
            SourceFile(
                "counsel_network_cluster_summary",
                "analytics",
                analytics_dir / "counsel_network_cluster_summary.json",
            ),
        ]
        source_files.extend(source for source in optional_source_files if source.path.exists())

        missing = [source.path for source in source_files if not source.path.exists()]
        if missing:
            missing_text = ", ".join(str(path) for path in missing)
            raise FileNotFoundError(f"Serving build requires existing artifacts: {missing_text}")

        _total = 28  # 23 loaders + 5 DB phases
        _step = 0

        def _tick(desc: str) -> None:
            nonlocal _step
            if on_progress:
                on_progress(_step, _total, desc)
            _step += 1

        _tick("Serving: Carregando processos...")
        cases = load_cases(curated_dir)
        _tick("Serving: Carregando alertas...")
        alerts = load_alerts(analytics_dir)
        _tick("Serving: Carregando advogados...")
        counsels, process_counsels = load_counsels(curated_dir)
        _tick("Serving: Carregando partes...")
        parties, process_parties = load_parties(curated_dir)
        _tick("Serving: Carregando métricas...")
        metrics = load_metrics(analytics_dir)
        _tick("Serving: Carregando perfis relatores...")
        rapporteur_profiles = load_rapporteur_profiles(analytics_dir)
        _tick("Serving: Carregando análises sequenciais...")
        sequential_analyses = load_sequential_analyses(analytics_dir)
        _tick("Serving: Carregando análises temporais...")
        temporal_analyses = load_temporal_analyses(analytics_dir)
        _tick("Serving: Carregando auditoria distribuição...")
        assignment_audits = load_assignment_audits(analytics_dir)
        _tick("Serving: Carregando ML outliers...")
        ml_outlier_scores = load_ml_outlier_scores(analytics_dir)
        _tick("Serving: Carregando contexto origem...")
        origin_contexts = load_origin_contexts(analytics_dir)
        _tick("Serving: Carregando sanções...")
        sanction_matches, counsel_sanction_profiles = load_sanction_matches(analytics_dir)
        _tick("Serving: Carregando doações...")
        donation_matches, counsel_donation_profiles = load_donation_matches(analytics_dir)
        _tick("Serving: Carregando vínculos societários...")
        corporate_conflicts = load_corporate_conflicts(analytics_dir)
        _tick("Serving: Carregando afinidade advogado...")
        counsel_affinities = load_counsel_affinities(analytics_dir)
        _tick("Serving: Carregando risco composto...")
        compound_risks = load_compound_risks(analytics_dir)
        _tick("Serving: Carregando velocidade decisória...")
        decision_velocities = load_decision_velocities(analytics_dir)
        _tick("Serving: Carregando mudanças de relatoria...")
        rapporteur_changes = load_rapporteur_changes(analytics_dir)
        _tick("Serving: Carregando clusters de advogados...")
        counsel_network_clusters = load_counsel_network_clusters(analytics_dir)
        _tick("Serving: Carregando biografias ministros...")
        minister_bios = load_minister_bios(curated_dir)
        _tick("Serving: Carregando movimentações...")
        movements = load_movements(curated_dir)
        _tick("Serving: Carregando sessões...")
        session_events = load_session_events(curated_dir)
        _tick("Serving: Calculando auditorias...")
        audits = build_source_audits(source_files)

        _tick("Serving: Limpando banco...")
        with Session(engine) as session:
            with session.begin():
                for model in (
                    ServingSourceAudit,
                    ServingMetric,
                    ServingSchemaMeta,
                    ServingSessionEvent,
                    ServingMovement,
                    ServingCounselNetworkCluster,
                    ServingRapporteurChange,
                    ServingDecisionVelocity,
                    ServingCounselAffinity,
                    ServingCompoundRisk,
                    ServingCorporateConflict,
                    ServingCounselDonationProfile,
                    ServingDonationMatch,
                    ServingCounselSanctionProfile,
                    ServingSanctionMatch,
                    ServingOriginContext,
                    ServingMinisterBio,
                    ServingMinisterFlow,
                    ServingAssignmentAudit,
                    ServingTemporalAnalysis,
                    ServingSequentialAnalysis,
                    ServingRapporteurProfile,
                    ServingMlOutlierScore,
                    ServingProcessParty,
                    ServingParty,
                    ServingProcessCounsel,
                    ServingCounsel,
                    ServingAlert,
                    ServingCase,
                ):
                    session.execute(delete(model))
                _tick("Serving: Inserindo processos e alertas...")
                session.add_all(cases)
                session.add_all(alerts)
                session.flush()
                _tick("Serving: Materializando fluxo ministros...")
                minister_flows = _materialize_minister_flows(session)
                _tick("Serving: Inserindo entidades e analytics...")
                session.add_all(counsels)
                session.add_all(process_counsels)
                session.add_all(parties)
                session.add_all(process_parties)
                session.add_all(rapporteur_profiles)
                session.add_all(temporal_analyses)
                session.add_all(sequential_analyses)
                session.add_all(assignment_audits)
                session.add_all(ml_outlier_scores)
                session.add_all(origin_contexts)
                session.add_all(sanction_matches)
                session.add_all(counsel_sanction_profiles)
                session.add_all(donation_matches)
                session.add_all(counsel_donation_profiles)
                session.add_all(corporate_conflicts)
                session.add_all(counsel_affinities)
                session.add_all(compound_risks)
                session.add_all(decision_velocities)
                session.add_all(rapporteur_changes)
                session.add_all(counsel_network_clusters)
                session.add_all(minister_bios)
                session.add_all(minister_flows)
                session.add_all(movements)
                session.add_all(session_events)
                session.add_all(metrics)
                session.add_all(audits)
                _tick("Serving: Finalizando metadados...")
                session.add(
                    ServingSchemaMeta(
                        singleton_key=SERVING_SCHEMA_SINGLETON_KEY,
                        schema_version=SERVING_SCHEMA_VERSION,
                        schema_fingerprint=_serving_schema_fingerprint(),
                        built_at=datetime.now().astimezone(),
                    )
                )

        if on_progress:
            on_progress(_total, _total, "Serving: Concluído")

        return ServingBuildResult(
            database_url=database_url,
            case_count=len(cases),
            alert_count=len(alerts),
            counsel_count=len(counsels),
            party_count=len(parties),
            source_count=len(audits),
        )
    finally:
        engine.dispose()
