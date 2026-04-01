from __future__ import annotations

from pathlib import Path

from ._builder_utils import SourceFile

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")


def _collect_source_files(curated_dir: Path, analytics_dir: Path) -> list[SourceFile]:
    required = [
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
    optional = [
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
        SourceFile("economic_group", "analytics", analytics_dir / "economic_group.jsonl"),
        SourceFile("lawyer_entity", "curated", curated_dir / "lawyer_entity.jsonl"),
        SourceFile("law_firm_entity", "curated", curated_dir / "law_firm_entity.jsonl"),
        SourceFile("representation_edge", "curated", curated_dir / "representation_edge.jsonl"),
        SourceFile("representation_event", "curated", curated_dir / "representation_event.jsonl"),
        SourceFile("agenda_event", "curated", curated_dir / "agenda_event.jsonl"),
        SourceFile("agenda_coverage", "curated", curated_dir / "agenda_coverage.jsonl"),
        SourceFile("agenda_exposure", "analytics", analytics_dir / "agenda_exposure.jsonl"),
        SourceFile("payment_counterparty", "analytics", analytics_dir / "payment_counterparty.jsonl"),
        SourceFile("sanction_corporate_link", "analytics", analytics_dir / "sanction_corporate_link.jsonl"),
    ]
    result = list(required)
    result.extend(source for source in optional if source.path.exists())

    missing = [source.path for source in result if not source.path.exists()]
    if missing:
        missing_text = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"Serving build requires existing artifacts: {missing_text}")
    return result
