from __future__ import annotations

import json
from pathlib import Path

from ._builder_utils import _coerce_bool, _coerce_float, _coerce_int, _parse_datetime, _read_json, _read_jsonl
from .models import (
    ServingCompoundRisk,
    ServingCounselAffinity,
    ServingCounselNetworkCluster,
    ServingDecisionVelocity,
    ServingMinisterBio,
    ServingPaymentCounterparty,
    ServingRapporteurChange,
)


def load_counsel_affinities(analytics_dir: Path) -> list[ServingCounselAffinity]:
    ca_path = analytics_dir / "counsel_affinity.jsonl"
    if not ca_path.exists():
        return []
    results: list[ServingCounselAffinity] = []
    seen: set[str] = set()
    for record in _read_jsonl(ca_path):
        aid = str(record.get("affinity_id", ""))
        if aid in seen:
            continue
        seen.add(aid)
        results.append(
            ServingCounselAffinity(
                affinity_id=aid,
                rapporteur=str(record.get("rapporteur", "")),
                counsel_id=str(record.get("counsel_id", "")),
                counsel_name_normalized=str(record.get("counsel_name_normalized", "")),
                shared_case_count=_coerce_int(record.get("shared_case_count")),
                favorable_count=_coerce_int(record.get("favorable_count")),
                unfavorable_count=_coerce_int(record.get("unfavorable_count")),
                pair_favorable_rate=record.get("pair_favorable_rate"),
                minister_baseline_favorable_rate=record.get("minister_baseline_favorable_rate"),
                counsel_baseline_favorable_rate=record.get("counsel_baseline_favorable_rate"),
                pair_delta_vs_minister=record.get("pair_delta_vs_minister"),
                pair_delta_vs_counsel=record.get("pair_delta_vs_counsel"),
                red_flag=_coerce_bool(record.get("red_flag")),
                institutional=_coerce_bool(record.get("institutional")),
                institutional_source=str(record.get("institutional_source", "private")),
                institutional_confidence=record.get("institutional_confidence"),
                top_process_classes_json=json.dumps(record.get("top_process_classes", []), ensure_ascii=False),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_compound_risks(analytics_dir: Path) -> list[ServingCompoundRisk]:
    cr_path = analytics_dir / "compound_risk.jsonl"
    if not cr_path.exists():
        return []
    results: list[ServingCompoundRisk] = []
    seen: set[str] = set()
    for record in _read_jsonl(cr_path):
        pair_id = str(record.get("pair_id", ""))
        if pair_id in seen:
            continue
        seen.add(pair_id)
        results.append(
            ServingCompoundRisk(
                pair_id=pair_id,
                minister_name=str(record.get("minister_name", "")),
                entity_type=str(record.get("entity_type", "")),
                entity_id=str(record.get("entity_id", "")),
                entity_name=str(record.get("entity_name", "")),
                signal_count=_coerce_int(record.get("signal_count")),
                signals_json=json.dumps(record.get("signals", []), ensure_ascii=False),
                red_flag=_coerce_bool(record.get("red_flag")),
                shared_process_count=_coerce_int(record.get("shared_process_count")),
                shared_process_ids_json=json.dumps(record.get("shared_process_ids", []), ensure_ascii=False),
                alert_count=_coerce_int(record.get("alert_count")),
                alert_ids_json=json.dumps(record.get("alert_ids", []), ensure_ascii=False),
                max_alert_score=_coerce_float(record.get("max_alert_score")),
                max_rate_delta=_coerce_float(record.get("max_rate_delta")),
                sanction_match_count=_coerce_int(record.get("sanction_match_count")),
                sanction_sources_json=json.dumps(record.get("sanction_sources", []), ensure_ascii=False),
                donation_match_count=_coerce_int(record.get("donation_match_count")),
                donation_total_brl=_coerce_float(record.get("donation_total_brl")),
                corporate_conflict_count=_coerce_int(record.get("corporate_conflict_count")),
                corporate_conflict_ids_json=json.dumps(record.get("corporate_conflict_ids", []), ensure_ascii=False),
                corporate_companies_json=json.dumps(record.get("corporate_companies", []), ensure_ascii=False),
                affinity_count=_coerce_int(record.get("affinity_count")),
                affinity_ids_json=json.dumps(record.get("affinity_ids", []), ensure_ascii=False),
                top_process_classes_json=json.dumps(record.get("top_process_classes", []), ensure_ascii=False),
                supporting_party_ids_json=json.dumps(record.get("supporting_party_ids", []), ensure_ascii=False),
                supporting_party_names_json=json.dumps(record.get("supporting_party_names", []), ensure_ascii=False),
                signal_details_json=json.dumps(record.get("signal_details", {}), ensure_ascii=False),
                earliest_year=record.get("earliest_year"),
                latest_year=record.get("latest_year"),
                sanction_corporate_link_count=_coerce_int(record.get("sanction_corporate_link_count")),
                sanction_corporate_link_ids_json=json.dumps(
                    record.get("sanction_corporate_link_ids", []), ensure_ascii=False
                ),
                sanction_corporate_min_degree=_coerce_int(record.get("sanction_corporate_min_degree"))
                if record.get("sanction_corporate_min_degree") is not None
                else None,
                adjusted_rate_delta=_coerce_float(record.get("adjusted_rate_delta")),
                has_law_firm_group=_coerce_bool(record.get("has_law_firm_group")),
                donor_group_has_minister_partner=_coerce_bool(record.get("donor_group_has_minister_partner")),
                donor_group_has_party_partner=_coerce_bool(record.get("donor_group_has_party_partner")),
                donor_group_has_counsel_partner=_coerce_bool(record.get("donor_group_has_counsel_partner")),
                min_link_degree_to_minister=_coerce_int(record.get("min_link_degree_to_minister"))
                if record.get("min_link_degree_to_minister") is not None
                else None,
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_decision_velocities(analytics_dir: Path) -> list[ServingDecisionVelocity]:
    dv_path = analytics_dir / "decision_velocity.jsonl"
    if not dv_path.exists():
        return []
    results: list[ServingDecisionVelocity] = []
    seen: set[str] = set()
    for record in _read_jsonl(dv_path):
        vid = str(record.get("velocity_id", ""))
        if vid in seen:
            continue
        seen.add(vid)
        results.append(
            ServingDecisionVelocity(
                velocity_id=vid,
                decision_event_id=str(record.get("decision_event_id", "")),
                process_id=str(record.get("process_id", "")),
                current_rapporteur=record.get("current_rapporteur"),
                decision_date=record.get("decision_date"),
                filing_date=record.get("filing_date"),
                days_to_decision=_coerce_int(record.get("days_to_decision")),
                process_class=record.get("process_class"),
                thematic_key=record.get("thematic_key"),
                decision_year=record.get("decision_year"),
                group_size=record.get("group_size"),
                p5_days=_coerce_float(record.get("p5_days")),
                p10_days=_coerce_float(record.get("p10_days")),
                median_days=_coerce_float(record.get("median_days")),
                p90_days=_coerce_float(record.get("p90_days")),
                p95_days=_coerce_float(record.get("p95_days")),
                velocity_flag=record.get("velocity_flag"),
                velocity_z_score=_coerce_float(record.get("velocity_z_score")),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_rapporteur_changes(analytics_dir: Path) -> list[ServingRapporteurChange]:
    rc_path = analytics_dir / "rapporteur_change.jsonl"
    if not rc_path.exists():
        return []
    results: list[ServingRapporteurChange] = []
    seen: set[str] = set()
    for record in _read_jsonl(rc_path):
        cid = str(record.get("change_id", ""))
        if cid in seen:
            continue
        seen.add(cid)
        results.append(
            ServingRapporteurChange(
                change_id=cid,
                process_id=str(record.get("process_id", "")),
                process_class=record.get("process_class"),
                previous_rapporteur=str(record.get("previous_rapporteur", "")),
                new_rapporteur=str(record.get("new_rapporteur", "")),
                change_date=record.get("change_date"),
                decision_event_id=record.get("decision_event_id"),
                post_change_decision_count=_coerce_int(record.get("post_change_decision_count")),
                post_change_favorable_rate=_coerce_float(record.get("post_change_favorable_rate")),
                new_rapporteur_baseline_rate=_coerce_float(record.get("new_rapporteur_baseline_rate")),
                delta_vs_baseline=_coerce_float(record.get("delta_vs_baseline")),
                red_flag=_coerce_bool(record.get("red_flag")),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_counsel_network_clusters(analytics_dir: Path) -> list[ServingCounselNetworkCluster]:
    cn_path = analytics_dir / "counsel_network_cluster.jsonl"
    if not cn_path.exists():
        return []
    results: list[ServingCounselNetworkCluster] = []
    seen: set[str] = set()
    for record in _read_jsonl(cn_path):
        cid = str(record.get("cluster_id", ""))
        if cid in seen:
            continue
        seen.add(cid)
        results.append(
            ServingCounselNetworkCluster(
                cluster_id=cid,
                counsel_ids_json=json.dumps(record.get("counsel_ids", []), ensure_ascii=False),
                counsel_names_json=json.dumps(record.get("counsel_names", []), ensure_ascii=False),
                cluster_size=_coerce_int(record.get("cluster_size")),
                shared_client_count=_coerce_int(record.get("shared_client_count")),
                shared_process_count=_coerce_int(record.get("shared_process_count")),
                minister_names_json=json.dumps(record.get("minister_names", []), ensure_ascii=False),
                cluster_favorable_rate=_coerce_float(record.get("cluster_favorable_rate")),
                baseline_rate=_coerce_float(record.get("baseline_rate")),
                cluster_case_count=_coerce_int(record.get("cluster_case_count")),
                red_flag=_coerce_bool(record.get("red_flag")),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_payment_counterparties(analytics_dir: Path) -> list[ServingPaymentCounterparty]:
    pc_path = analytics_dir / "payment_counterparty.jsonl"
    if not pc_path.exists():
        return []
    results: list[ServingPaymentCounterparty] = []
    seen: set[str] = set()
    for record in _read_jsonl(pc_path):
        cid = str(record.get("counterparty_id", ""))
        if cid in seen:
            continue
        seen.add(cid)
        results.append(
            ServingPaymentCounterparty(
                counterparty_id=cid,
                counterparty_identity_key=str(record.get("counterparty_identity_key", "")),
                identity_basis=str(record.get("identity_basis", "")),
                counterparty_name=str(record.get("counterparty_name", "")),
                counterparty_tax_id=str(record.get("counterparty_tax_id") or ""),
                counterparty_tax_id_normalized=str(record.get("counterparty_tax_id_normalized") or ""),
                counterparty_document_type=str(record.get("counterparty_document_type", "")),
                total_received_brl=_coerce_float(record.get("total_received_brl")) or 0.0,
                payment_count=_coerce_int(record.get("payment_count")),
                election_years_json=json.dumps(record.get("election_years", []), ensure_ascii=False),
                payer_parties_json=json.dumps(record.get("payer_parties", []), ensure_ascii=False),
                payer_actor_type=str(record.get("payer_actor_type", "party_org")),
                first_payment_date=record.get("first_payment_date"),
                last_payment_date=record.get("last_payment_date"),
                states_json=json.dumps(record.get("states", []), ensure_ascii=False),
                cnae_codes_json=json.dumps(record.get("cnae_codes", []), ensure_ascii=False),
                provenance_json=json.dumps(record.get("provenance", {}), ensure_ascii=False),
                generated_at=_parse_datetime(record.get("generated_at")),
            )
        )
    return results


def load_minister_bios(curated_dir: Path) -> list[ServingMinisterBio]:
    bio_path = curated_dir / "minister_bio.json"
    if not bio_path.exists():
        return []
    bio_data = _read_json(bio_path)
    results: list[ServingMinisterBio] = []
    for _name, entry in bio_data.items():
        results.append(
            ServingMinisterBio(
                minister_name=str(entry.get("minister_name", "")),
                appointment_date=entry.get("appointment_date"),
                appointing_president=entry.get("appointing_president"),
                birth_date=entry.get("birth_date"),
                birth_state=entry.get("birth_state"),
                career_summary=entry.get("career_summary"),
                political_party_history_json=json.dumps(entry.get("political_party_history"), ensure_ascii=False),
                known_connections_json=json.dumps(entry.get("known_connections"), ensure_ascii=False),
                news_references_json=json.dumps(entry.get("news_references"), ensure_ascii=False),
            )
        )
    return results
