"""Build compound risk analytics by converging signals across analytics artifacts."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ._atomic_io import AtomicJsonlWriter
from ._compound_risk_evidence import (
    PairEvidence,
    _build_signal_details,
    _coerce_float,
    _coerce_str_list,
    _compute_adjusted_rate_delta,
    _sort_rows,
)
from ._compound_risk_loaders import (
    _counsel_name_map,
    _evidence_for,
    _load_rows,
    _pair_process_index,
    _pair_process_map,
    _party_name_map,
    _process_context,
    _process_entity_maps,
    _required_inputs_exist,
)
from ._match_helpers import build_process_class_map
from ._run_context import RunContext

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")
TOP_PAIR_LIMIT = 20


def build_compound_risk(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    analytics_dir: Path = DEFAULT_ANALYTICS_DIR,
    output_dir: Path = DEFAULT_ANALYTICS_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build minister-entity compound risk rankings from converging signals."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ctx = RunContext("compound-risk", output_dir, total_steps=4, on_progress=on_progress)

    if not _required_inputs_exist(curated_dir):
        logger.warning("Compound risk skipped: curated inputs missing under %s", curated_dir)
        ctx.finish(outputs=[])
        return output_dir

    ctx.start_step(0, "Compound Risk: Carregando dados...")
    party_names = _party_name_map(curated_dir)
    counsel_names = _counsel_name_map(curated_dir)
    process_parties, process_counsels = _process_entity_maps(curated_dir, party_names, counsel_names)
    process_ministers, decision_event_context, process_years = _process_context(curated_dir)
    pair_processes = _pair_process_map(process_ministers, process_parties, process_counsels)
    party_pair_processes, counsel_pair_processes = _pair_process_index(pair_processes)
    process_path = curated_dir / "process.jsonl"
    process_classes = build_process_class_map(process_path) if process_path.exists() else {}

    sanction_rows = _load_rows(analytics_dir / "sanction_match.jsonl", red_flag_only=True)
    donation_rows = _load_rows(analytics_dir / "donation_match.jsonl", red_flag_only=True)
    corporate_rows = _load_rows(analytics_dir / "corporate_network.jsonl", red_flag_only=True)
    affinity_rows = _load_rows(analytics_dir / "counsel_affinity.jsonl", red_flag_only=True)
    alert_rows = _load_rows(analytics_dir / "outlier_alert.jsonl")
    velocity_rows = _load_rows(analytics_dir / "decision_velocity.jsonl")
    redistribution_rows = _load_rows(analytics_dir / "rapporteur_change.jsonl", red_flag_only=True)
    scl_rows = _load_rows(analytics_dir / "sanction_corporate_link.jsonl")

    donation_by_party: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pairs: dict[tuple[str, str, str], PairEvidence] = {}
    applied_cross_entity_donations: set[tuple[str, str, str]] = set()
    # Track counsels with direct donation matches to avoid duplicate
    # propagation via cross-entity inference.
    counsels_with_direct_donation: set[str] = set()

    ctx.start_step(1, "Compound Risk: Cruzando sinais...")
    for row in sanction_rows:
        row_entity_type = str(row.get("entity_type") or "party")
        if row_entity_type == "counsel":
            counsel_id = str(row.get("entity_id") or "")
            counsel_name = str(row.get("entity_name_normalized") or counsel_names.get(counsel_id) or "")
            if not counsel_id or counsel_id not in counsel_names:
                continue
            for minister_name, process_ids in counsel_pair_processes.get(counsel_id, []):
                evidence = _evidence_for(pairs, minister_name, "counsel", counsel_id, counsel_name)
                evidence.signals.add("sanction")
                evidence.sanction_match_count += 1
                evidence.sanction_sources.add(str(row.get("sanction_source") or ""))
                evidence.update_max_rate_delta(_coerce_float(row.get("favorable_rate_delta")))
                evidence.add_process_ids(process_ids)
        else:
            party_id = str(row.get("party_id") or row.get("entity_id") or "")
            if not party_id or party_id not in party_names:
                continue
            for minister_name, process_ids in party_pair_processes.get(party_id, []):
                evidence = _evidence_for(pairs, minister_name, "party", party_id, party_names[party_id])
                evidence.signals.add("sanction")
                evidence.sanction_match_count += 1
                evidence.sanction_sources.add(str(row.get("sanction_source") or ""))
                evidence.update_max_rate_delta(_coerce_float(row.get("favorable_rate_delta")))
                evidence.add_process_ids(process_ids)

    for row in donation_rows:
        row_entity_type = str(row.get("entity_type") or "party")
        if row_entity_type == "counsel":
            counsel_id = str(row.get("entity_id") or "")
            counsel_name = str(row.get("entity_name_normalized") or counsel_names.get(counsel_id) or "")
            if not counsel_id or counsel_id not in counsel_names:
                continue
            counsels_with_direct_donation.add(counsel_id)
            for minister_name, process_ids in counsel_pair_processes.get(counsel_id, []):
                evidence = _evidence_for(pairs, minister_name, "counsel", counsel_id, counsel_name)
                evidence.signals.add("donation")
                evidence.donation_match_count += 1
                evidence.donation_total_brl += _coerce_float(row.get("total_donated_brl")) or 0.0
                evidence.update_max_rate_delta(_coerce_float(row.get("favorable_rate_delta")))
                evidence.add_process_ids(process_ids)
                evidence.accumulate_donation_enrichment(row)
        else:
            party_id = str(row.get("party_id") or row.get("entity_id") or "")
            if not party_id or party_id not in party_names:
                continue
            donation_by_party[party_id].append(row)
            for minister_name, process_ids in party_pair_processes.get(party_id, []):
                evidence = _evidence_for(pairs, minister_name, "party", party_id, party_names[party_id])
                evidence.signals.add("donation")
                evidence.donation_match_count += 1
                evidence.donation_total_brl += _coerce_float(row.get("total_donated_brl")) or 0.0
                evidence.update_max_rate_delta(_coerce_float(row.get("favorable_rate_delta")))
                evidence.add_process_ids(process_ids)
                evidence.accumulate_donation_enrichment(row)

    for row in corporate_rows:
        minister_name = str(row.get("minister_name") or "")
        entity_type = str(row.get("linked_entity_type") or "")
        entity_id = str(row.get("linked_entity_id") or "")
        entity_name = str(row.get("linked_entity_name") or "")
        if entity_type not in {"party", "counsel"} or not minister_name or not entity_id or not entity_name:
            continue
        evidence = _evidence_for(pairs, minister_name, entity_type, entity_id, entity_name)
        evidence.signals.add("corporate")
        evidence.corporate_conflict_count += 1
        evidence.corporate_conflict_ids.add(str(row.get("conflict_id") or ""))
        key = f"{row.get('company_cnpj_basico') or ''}:{row.get('company_name') or ''}"
        evidence.corporate_companies[key] = {
            "company_cnpj_basico": str(row.get("company_cnpj_basico") or ""),
            "company_name": str(row.get("company_name") or ""),
            "link_degree": int(row.get("link_degree") or 1),
        }
        rs = _coerce_float(row.get("risk_score"))
        evidence.update_max_rate_delta(rs if rs is not None else _coerce_float(row.get("favorable_rate_delta")))
        evidence.add_process_ids(set(_coerce_str_list(row.get("shared_process_ids"))))

    for row in affinity_rows:
        minister_name = str(row.get("rapporteur") or "")
        counsel_id = str(row.get("counsel_id") or "")
        counsel_name = str(row.get("counsel_name_normalized") or counsel_names.get(counsel_id) or "")
        if not minister_name or not counsel_id or not counsel_name:
            continue
        evidence = _evidence_for(pairs, minister_name, "counsel", counsel_id, counsel_name)
        evidence.signals.add("affinity")
        evidence.affinity_count += 1
        evidence.affinity_ids.add(str(row.get("affinity_id") or ""))
        evidence.top_process_classes.update(_coerce_str_list(row.get("top_process_classes")))
        delta = max(
            _coerce_float(row.get("pair_delta_vs_minister")) or 0.0,
            _coerce_float(row.get("pair_delta_vs_counsel")) or 0.0,
        )
        evidence.update_max_rate_delta(delta)
        for candidate_minister_name, process_ids in counsel_pair_processes.get(counsel_id, []):
            if candidate_minister_name == minister_name:
                evidence.add_process_ids(process_ids)
                break

    for process_id, ministers in process_ministers.items():
        parties = process_parties.get(process_id, [])
        counsels = process_counsels.get(process_id, [])
        for minister_name in ministers:
            for party_id, party_name in parties:
                if party_id in donation_by_party:
                    # Cross-entity inference: when a party has donation matches,
                    # propagate the "donation" signal to counsels representing that
                    # party in the same process, since the counsel benefits from
                    # the same potentially conflicted relationship.
                    # Skip counsels that already have a direct donation match to
                    # avoid double-counting.
                    for counsel_id, counsel_name in counsels:
                        if counsel_id in counsels_with_direct_donation:
                            continue
                        evidence = _evidence_for(pairs, minister_name, "counsel", counsel_id, counsel_name)
                        evidence.signals.add("donation")
                        evidence.supporting_parties[party_id] = party_name
                        evidence.add_process_ids({process_id})
                        cross_entity_key = (minister_name, counsel_id, party_id)
                        if cross_entity_key in applied_cross_entity_donations:
                            continue
                        applied_cross_entity_donations.add(cross_entity_key)
                        evidence.donation_match_count += len(donation_by_party[party_id])
                        evidence.donation_total_brl += sum(
                            _coerce_float(row.get("total_donated_brl")) or 0.0 for row in donation_by_party[party_id]
                        )
                        for row in donation_by_party[party_id]:
                            evidence.update_max_rate_delta(_coerce_float(row.get("favorable_rate_delta")))
                            evidence.accumulate_donation_enrichment(row)

    # Velocity signals: flag processes with queue-jump or stalled status
    velocity_flagged = [r for r in velocity_rows if r.get("velocity_flag")]
    for row in velocity_flagged:
        process_id = str(row.get("process_id") or "")
        minister_name = str(row.get("current_rapporteur") or "")
        if not process_id or not minister_name:
            continue
        for party_id, party_name in process_parties.get(process_id, []):
            evidence = _evidence_for(pairs, minister_name, "party", party_id, party_name)
            evidence.signals.add("velocity")
            evidence.add_process_ids({process_id})
        for counsel_id, counsel_name in process_counsels.get(process_id, []):
            evidence = _evidence_for(pairs, minister_name, "counsel", counsel_id, counsel_name)
            evidence.signals.add("velocity")
            evidence.add_process_ids({process_id})

    # Redistribution signals: flag processes with rapporteur change + favorable outcome
    for row in redistribution_rows:
        process_id = str(row.get("process_id") or "")
        minister_name = str(row.get("new_rapporteur") or "")
        if not process_id or not minister_name:
            continue
        for party_id, party_name in process_parties.get(process_id, []):
            evidence = _evidence_for(pairs, minister_name, "party", party_id, party_name)
            evidence.signals.add("redistribution")
            evidence.add_process_ids({process_id})
        for counsel_id, counsel_name in process_counsels.get(process_id, []):
            evidence = _evidence_for(pairs, minister_name, "counsel", counsel_id, counsel_name)
            evidence.signals.add("redistribution")
            evidence.add_process_ids({process_id})

    for row in scl_rows:
        stf_entity_type = str(row.get("stf_entity_type") or "")
        stf_entity_id = str(row.get("stf_entity_id") or "")
        stf_entity_name = str(row.get("stf_entity_name") or "")
        if stf_entity_type not in {"party", "counsel"} or not stf_entity_id:
            continue
        link_id = str(row.get("link_id") or "")
        link_degree = int(row.get("link_degree") or 2)
        if stf_entity_type == "party":
            for minister_name, process_ids in party_pair_processes.get(stf_entity_id, []):
                evidence = _evidence_for(pairs, minister_name, "party", stf_entity_id, stf_entity_name)
                evidence.sanction_corporate_link_count += 1
                evidence.add_process_ids(process_ids)
                if link_id:
                    evidence.sanction_corporate_link_ids.add(link_id)
                cur_min = evidence.sanction_corporate_min_degree
                if cur_min is None or link_degree < cur_min:
                    evidence.sanction_corporate_min_degree = link_degree
        elif stf_entity_type == "counsel":
            for minister_name, process_ids in counsel_pair_processes.get(stf_entity_id, []):
                evidence = _evidence_for(pairs, minister_name, "counsel", stf_entity_id, stf_entity_name)
                evidence.sanction_corporate_link_count += 1
                evidence.add_process_ids(process_ids)
                if link_id:
                    evidence.sanction_corporate_link_ids.add(link_id)
                cur_min = evidence.sanction_corporate_min_degree
                if cur_min is None or link_degree < cur_min:
                    evidence.sanction_corporate_min_degree = link_degree

    # SCL promotion: promote "sanction" family when SCL exists but no direct sanction
    for evidence in pairs.values():
        if evidence.sanction_corporate_link_count > 0 and "sanction" not in evidence.signals:
            evidence.signals.add("sanction")

    ctx.start_step(2, "Compound Risk: Vinculando alertas...")
    for row in alert_rows:
        alert_id = str(row.get("alert_id") or "")
        decision_event_id = str(row.get("decision_event_id") or "")
        alert_score = _coerce_float(row.get("alert_score"))
        context = decision_event_context.get(decision_event_id)
        if context is None:
            continue
        process_id, minister_name = context
        for party_id, party_name in process_parties.get(process_id, []):
            evidence = _evidence_for(pairs, minister_name, "party", party_id, party_name)
            evidence.add_alert(alert_id, alert_score)
            evidence.add_process_ids({process_id})
        for counsel_id, counsel_name in process_counsels.get(process_id, []):
            evidence = _evidence_for(pairs, minister_name, "counsel", counsel_id, counsel_name)
            evidence.add_alert(alert_id, alert_score)
            evidence.add_process_ids({process_id})

    generated_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for evidence in pairs.values():
        pair_process_classes = {
            process_classes[process_id]
            for process_id in evidence.process_ids
            if process_id in process_classes and process_classes[process_id]
        }
        pair_process_classes.update(evidence.top_process_classes)
        signal_count = len(evidence.signals)
        year_vals: list[int] = []
        for pid in evidence.process_ids:
            yr = process_years.get(pid)
            if yr is not None:
                year_vals.extend(yr)
        earliest_year = min(year_vals) if year_vals else None
        latest_year = max(year_vals) if year_vals else None
        rows.append(
            {
                "pair_id": stable_id("cr-", f"{evidence.minister_name}:{evidence.entity_type}:{evidence.entity_id}"),
                "minister_name": evidence.minister_name,
                "entity_type": evidence.entity_type,
                "entity_id": evidence.entity_id,
                "entity_name": evidence.entity_name,
                "signal_count": signal_count,
                "signals": sorted(evidence.signals),
                "red_flag": signal_count >= 2,
                "shared_process_count": len(evidence.process_ids),
                "shared_process_ids": sorted(evidence.process_ids),
                "alert_count": len(evidence.alert_ids),
                "alert_ids": sorted(evidence.alert_ids),
                "max_alert_score": evidence.max_alert_score,
                "max_rate_delta": evidence.max_rate_delta,
                "sanction_match_count": evidence.sanction_match_count,
                "sanction_sources": sorted(source for source in evidence.sanction_sources if source),
                "donation_match_count": evidence.donation_match_count,
                "donation_total_brl": round(evidence.donation_total_brl, 2),
                "corporate_conflict_count": evidence.corporate_conflict_count,
                "corporate_conflict_ids": sorted(conflict for conflict in evidence.corporate_conflict_ids if conflict),
                "corporate_companies": sorted(
                    evidence.corporate_companies.values(),
                    key=lambda item: (item["company_name"], item["company_cnpj_basico"]),
                ),
                "affinity_count": evidence.affinity_count,
                "affinity_ids": sorted(affinity for affinity in evidence.affinity_ids if affinity),
                "top_process_classes": sorted(pair_process_classes),
                "supporting_party_ids": sorted(evidence.supporting_parties),
                "supporting_party_names": [
                    evidence.supporting_parties[party_id] for party_id in sorted(evidence.supporting_parties)
                ],
                "signal_details": _build_signal_details(evidence),
                "earliest_year": earliest_year,
                "latest_year": latest_year,
                "sanction_corporate_link_count": evidence.sanction_corporate_link_count,
                "sanction_corporate_link_ids": sorted(lid for lid in evidence.sanction_corporate_link_ids if lid),
                "sanction_corporate_min_degree": evidence.sanction_corporate_min_degree,
                "adjusted_rate_delta": _compute_adjusted_rate_delta(evidence),
                "has_law_firm_group": evidence.has_law_firm_group,
                "donor_group_has_minister_partner": evidence.donor_group_has_minister_partner,
                "donor_group_has_party_partner": evidence.donor_group_has_party_partner,
                "donor_group_has_counsel_partner": evidence.donor_group_has_counsel_partner,
                "min_link_degree_to_minister": evidence.min_link_degree_to_minister,
                "generated_at": generated_at,
            }
        )

    rows = _sort_rows(rows)
    ctx.start_step(3, "Compound Risk: Gravando resultados...")
    output_path = output_dir / "compound_risk.jsonl"
    with AtomicJsonlWriter(output_path) as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = {
        "generated_at": generated_at,
        "pair_count": len(rows),
        "red_flag_count": sum(1 for row in rows if row["red_flag"]),
        "signal_frequency": {
            signal: sum(1 for row in rows if signal in row["signals"])
            for signal in ("sanction", "donation", "corporate", "affinity", "alert", "velocity", "redistribution")
        },
        "top_pairs": [
            {
                "pair_id": row["pair_id"],
                "minister_name": row["minister_name"],
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "entity_name": row["entity_name"],
                "signal_count": row["signal_count"],
                "signals": row["signals"],
                "max_alert_score": row["max_alert_score"],
                "max_rate_delta": row["max_rate_delta"],
                "adjusted_rate_delta": row["adjusted_rate_delta"],
            }
            for row in rows[:TOP_PAIR_LIMIT]
        ],
    }
    (output_dir / "compound_risk_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Built compound risk analytics: %d pairs (%d compound red flags)",
        len(rows),
        summary["red_flag_count"],
    )
    ctx.finish(outputs=[str(output_path)])
    return output_path
