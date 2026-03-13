from __future__ import annotations

from pathlib import Path

from ..core.rules import derive_thematic_key
from ._builder_loaders_analytics import (
    load_assignment_audits as load_assignment_audits,
)
from ._builder_loaders_analytics import (
    load_compound_risks as load_compound_risks,
)
from ._builder_loaders_analytics import (
    load_corporate_conflicts as load_corporate_conflicts,
)
from ._builder_loaders_analytics import (
    load_counsel_affinities as load_counsel_affinities,
)
from ._builder_loaders_analytics import (
    load_counsel_network_clusters as load_counsel_network_clusters,
)
from ._builder_loaders_analytics import (
    load_decision_velocities as load_decision_velocities,
)
from ._builder_loaders_analytics import (
    load_donation_matches as load_donation_matches,
)
from ._builder_loaders_analytics import (
    load_minister_bios as load_minister_bios,
)
from ._builder_loaders_analytics import (
    load_ml_outlier_scores as load_ml_outlier_scores,
)
from ._builder_loaders_analytics import (
    load_origin_contexts as load_origin_contexts,
)
from ._builder_loaders_analytics import (
    load_rapporteur_changes as load_rapporteur_changes,
)
from ._builder_loaders_analytics import (
    load_rapporteur_profiles as load_rapporteur_profiles,
)
from ._builder_loaders_analytics import (
    load_sanction_matches as load_sanction_matches,
)
from ._builder_loaders_analytics import (
    load_sequential_analyses as load_sequential_analyses,
)
from ._builder_loaders_analytics import (
    load_temporal_analyses as load_temporal_analyses,
)
from ._builder_utils import (
    SourceFile,
    _coerce_bool,
    _coerce_int,
    _dedupe_records_by_key,
    _parse_date,
    _parse_datetime,
    _read_json,
    _read_jsonl,
    _source_checksum,
    _source_updated_at,
)
from .models import (
    ServingAlert,
    ServingCase,
    ServingCounsel,
    ServingMetric,
    ServingParty,
    ServingProcessCounsel,
    ServingProcessParty,
    ServingSourceAudit,
)


def load_cases(curated_dir: Path) -> list[ServingCase]:
    process_by_id = {
        record["process_id"]: record
        for record in _read_jsonl(curated_dir / "process.jsonl")
        if isinstance(record.get("process_id"), str)
    }
    cases: list[ServingCase] = []
    for record in _read_jsonl(curated_dir / "decision_event.jsonl"):
        decision_event_id = record.get("decision_event_id")
        process_id = record.get("process_id")
        if not isinstance(decision_event_id, str) or not decision_event_id.strip():
            continue
        if not isinstance(process_id, str):
            continue
        process = process_by_id.get(process_id, {})
        decision_date = _parse_date(record.get("decision_date"))
        period = decision_date.strftime("%Y-%m") if decision_date else None
        cases.append(
            ServingCase(
                decision_event_id=decision_event_id,
                process_id=process_id,
                process_number=process.get("process_number"),
                process_class=process.get("process_class"),
                branch_of_law=process.get("branch_of_law"),
                thematic_key=derive_thematic_key(process.get("subjects_normalized"), process.get("branch_of_law")),
                origin_description=process.get("origin_description"),
                inteiro_teor_url=process.get("juris_inteiro_teor_url"),
                juris_doc_count=_coerce_int(process.get("juris_doc_count")),
                juris_has_acordao=_coerce_bool(process.get("juris_has_acordao")),
                juris_has_decisao_monocratica=_coerce_bool(process.get("juris_has_decisao_monocratica")),
                decision_date=decision_date,
                period=period,
                current_rapporteur=record.get("current_rapporteur"),
                decision_type=record.get("decision_type"),
                decision_progress=record.get("decision_progress"),
                decision_origin=record.get("decision_origin"),
                judging_body=record.get("judging_body"),
                is_collegiate=record.get("is_collegiate"),
                decision_note=record.get("decision_note"),
            )
        )
    return cases


def load_alerts(analytics_dir: Path) -> list[ServingAlert]:
    import json as _json

    return [
        ServingAlert(
            alert_id=str(record.get("alert_id")),
            process_id=str(record.get("process_id")),
            decision_event_id=str(record.get("decision_event_id")),
            comparison_group_id=str(record.get("comparison_group_id")),
            alert_type=str(record.get("alert_type")),
            alert_score=float(record.get("alert_score", 0.0)),
            expected_pattern=str(record.get("expected_pattern", "")),
            observed_pattern=str(record.get("observed_pattern", "")),
            evidence_summary=str(record.get("evidence_summary", "")),
            uncertainty_note=record.get("uncertainty_note"),
            status=str(record.get("status", "novo")),
            risk_signal_count=int(record.get("risk_signal_count", 0)),
            risk_signals_json=_json.dumps(record["risk_signals"]) if record.get("risk_signals") else None,
            created_at=_parse_datetime(record.get("created_at")),
            updated_at=_parse_datetime(record.get("updated_at")),
        )
        for record in _read_jsonl(analytics_dir / "outlier_alert.jsonl")
    ]


def load_counsels(curated_dir: Path) -> tuple[list[ServingCounsel], list[ServingProcessCounsel]]:
    counsels = [
        ServingCounsel(
            counsel_id=str(record.get("counsel_id")),
            counsel_name_raw=str(record.get("counsel_name_raw", "")),
            counsel_name_normalized=str(record.get("counsel_name_normalized", "")),
            notes=record.get("notes"),
        )
        for record in _read_jsonl(curated_dir / "counsel.jsonl")
    ]
    process_counsels = [
        ServingProcessCounsel(
            link_id=str(record.get("link_id")),
            process_id=str(record.get("process_id")),
            counsel_id=str(record.get("counsel_id")),
            side_in_case=record.get("side_in_case"),
            source_id=record.get("source_id"),
        )
        for record in _dedupe_records_by_key(_read_jsonl(curated_dir / "process_counsel_link.jsonl"), "link_id")
    ]
    return counsels, process_counsels


def load_parties(curated_dir: Path) -> tuple[list[ServingParty], list[ServingProcessParty]]:
    parties = [
        ServingParty(
            party_id=str(record.get("party_id")),
            party_name_raw=str(record.get("party_name_raw", "")),
            party_name_normalized=str(record.get("party_name_normalized", "")),
            notes=record.get("notes"),
        )
        for record in _read_jsonl(curated_dir / "party.jsonl")
    ]
    process_parties = [
        ServingProcessParty(
            link_id=str(record.get("link_id")),
            process_id=str(record.get("process_id")),
            party_id=str(record.get("party_id")),
            role_in_case=record.get("role_in_case"),
            source_id=record.get("source_id"),
        )
        for record in _dedupe_records_by_key(_read_jsonl(curated_dir / "process_party_link.jsonl"), "link_id")
    ]
    return parties, process_parties


def load_metrics(analytics_dir: Path) -> list[ServingMetric]:
    alert_summary = _read_json(analytics_dir / "outlier_alert_summary.json")
    comparison_summary = _read_json(analytics_dir / "comparison_group_summary.json")
    baseline_summary = _read_json(analytics_dir / "baseline_summary.json")
    return [
        ServingMetric(key="alert_count", value_integer=_coerce_int(alert_summary.get("alert_count"))),
        ServingMetric(key="avg_alert_score", value_float=float(alert_summary.get("avg_score", 0.0))),
        ServingMetric(key="valid_group_count", value_integer=_coerce_int(comparison_summary.get("valid_group_count"))),
        ServingMetric(key="baseline_count", value_integer=_coerce_int(baseline_summary.get("baseline_count"))),
    ]


def build_source_audits(source_files: list[SourceFile]) -> list[ServingSourceAudit]:
    return [
        ServingSourceAudit(
            label=source.label,
            category=source.category,
            relative_path=(
                str(source.path.relative_to(Path.cwd())) if source.path.is_relative_to(Path.cwd()) else str(source.path)
            ),
            checksum=_source_checksum(source.path),
            updated_at=_source_updated_at(source.path),
        )
        for source in source_files
    ]
