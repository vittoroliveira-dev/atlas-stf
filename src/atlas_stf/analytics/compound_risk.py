"""Build compound risk analytics by converging signals across analytics artifacts."""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ._atomic_io import AtomicJsonlWriter
from ._match_helpers import build_process_class_map, read_jsonl

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")
TOP_PAIR_LIMIT = 20


@dataclass(slots=True)
class PairEvidence:
    minister_name: str
    entity_type: str
    entity_id: str
    entity_name: str
    process_ids: set[str] = field(default_factory=set)
    signals: set[str] = field(default_factory=set)
    alert_ids: set[str] = field(default_factory=set)
    sanction_sources: set[str] = field(default_factory=set)
    corporate_conflict_ids: set[str] = field(default_factory=set)
    corporate_companies: dict[str, dict[str, Any]] = field(default_factory=dict)
    affinity_ids: set[str] = field(default_factory=set)
    top_process_classes: set[str] = field(default_factory=set)
    supporting_parties: dict[str, str] = field(default_factory=dict)
    max_alert_score: float | None = None
    max_rate_delta: float | None = None
    sanction_match_count: int = 0
    donation_match_count: int = 0
    donation_total_brl: float = 0.0
    corporate_conflict_count: int = 0
    affinity_count: int = 0
    sanction_corporate_link_count: int = 0
    sanction_corporate_link_ids: set[str] = field(default_factory=set)
    sanction_corporate_min_degree: int | None = None
    has_law_firm_group: bool = False
    donor_group_has_minister_partner: bool = False
    donor_group_has_party_partner: bool = False
    donor_group_has_counsel_partner: bool = False
    min_link_degree_to_minister: int | None = None
    max_economic_group_member_count: int | None = None
    donation_enrichment_meta: dict[str, set[str]] = field(
        default_factory=lambda: {
            "economic_group_ids": set(),
            "donor_cnpj_basicos": set(),
            "donor_company_names": set(),
            "match_strategies": set(),
            "red_flag_confidences": set(),
        }
    )
    max_red_flag_power: float | None = None
    donation_has_corporate_link_red_flag: bool = False

    def add_process_ids(self, process_ids: set[str]) -> None:
        self.process_ids.update(process_ids)

    def add_alert(self, alert_id: str, alert_score: float | None) -> None:
        self.signals.add("alert")
        self.alert_ids.add(alert_id)
        if alert_score is not None and (self.max_alert_score is None or alert_score > self.max_alert_score):
            self.max_alert_score = alert_score

    def update_max_rate_delta(self, value: float | None) -> None:
        if value is None:
            return
        if self.max_rate_delta is None or value > self.max_rate_delta:
            self.max_rate_delta = value

    def accumulate_donation_enrichment(self, row: dict[str, Any]) -> None:
        if row.get("is_law_firm_group") is True:
            self.has_law_firm_group = True
        if row.get("donor_group_has_minister_partner") is True:
            self.donor_group_has_minister_partner = True
        if row.get("donor_group_has_party_partner") is True:
            self.donor_group_has_party_partner = True
        if row.get("donor_group_has_counsel_partner") is True:
            self.donor_group_has_counsel_partner = True
        degree = row.get("min_link_degree_to_minister")
        if degree is not None:
            try:
                d = int(degree)
            except (TypeError, ValueError):
                d = None  # type: ignore[assignment]
            if d is not None and (self.min_link_degree_to_minister is None or d < self.min_link_degree_to_minister):
                self.min_link_degree_to_minister = d
        member_count = row.get("economic_group_member_count")
        if member_count is not None:
            try:
                mc = int(member_count)
            except (TypeError, ValueError):
                mc = None  # type: ignore[assignment]
            if mc is not None and (
                self.max_economic_group_member_count is None or mc > self.max_economic_group_member_count
            ):
                self.max_economic_group_member_count = mc
        power = row.get("red_flag_power")
        if power is not None:
            try:
                p = float(power)
            except (TypeError, ValueError):
                p = None  # type: ignore[assignment]
            if p is not None and (self.max_red_flag_power is None or p > self.max_red_flag_power):
                self.max_red_flag_power = p
        confidence = row.get("red_flag_confidence")
        if confidence:
            self.donation_enrichment_meta["red_flag_confidences"].add(str(confidence))
        if row.get("corporate_link_red_flag") is True:
            self.donation_has_corporate_link_red_flag = True
        for meta_key, row_key in [
            ("economic_group_ids", "economic_group_id"),
            ("donor_cnpj_basicos", "donor_cnpj_basico"),
            ("donor_company_names", "donor_company_name"),
            ("match_strategies", "match_strategy"),
        ]:
            val = row.get(row_key)
            if val:
                self.donation_enrichment_meta[meta_key].add(str(val))


def _coerce_float(value: Any) -> float | None:
    try:
        result = float(value)
    except TypeError, ValueError:
        return None
    return result if math.isfinite(result) else None


def _compute_adjusted_rate_delta(evidence: PairEvidence) -> float | None:
    base = evidence.max_rate_delta
    if base is None:
        return None
    multiplier = 1.0
    if evidence.has_law_firm_group:
        multiplier *= 1.5
    if evidence.donor_group_has_minister_partner:
        multiplier *= 2.0
    if evidence.min_link_degree_to_minister is not None and evidence.min_link_degree_to_minister > 2:
        multiplier *= 0.5 ** (evidence.min_link_degree_to_minister - 2)
    result = base * multiplier
    return result if math.isfinite(result) else None


def _coerce_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item not in {None, ""}]
    return []


def _required_inputs_exist(curated_dir: Path) -> bool:
    required = [
        curated_dir / "party.jsonl",
        curated_dir / "counsel.jsonl",
        curated_dir / "process_party_link.jsonl",
        curated_dir / "process_counsel_link.jsonl",
        curated_dir / "decision_event.jsonl",
    ]
    return all(path.exists() for path in required)


def _party_name_map(curated_dir: Path) -> dict[str, str]:
    return {
        str(record.get("party_id")): str(record.get("party_name_normalized") or record.get("party_name_raw") or "")
        for record in read_jsonl(curated_dir / "party.jsonl")
        if record.get("party_id")
    }


def _counsel_name_map(curated_dir: Path) -> dict[str, str]:
    return {
        str(record.get("counsel_id")): str(
            record.get("counsel_name_normalized") or record.get("counsel_name_raw") or ""
        )
        for record in read_jsonl(curated_dir / "counsel.jsonl")
        if record.get("counsel_id")
    }


def _process_entity_maps(
    curated_dir: Path,
    party_names: dict[str, str],
    counsel_names: dict[str, str],
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, list[tuple[str, str]]]]:
    process_parties: dict[str, list[tuple[str, str]]] = defaultdict(list)
    process_counsels: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for record in read_jsonl(curated_dir / "process_party_link.jsonl"):
        process_id = record.get("process_id")
        party_id = record.get("party_id")
        if process_id and party_id and party_id in party_names:
            process_parties[str(process_id)].append((str(party_id), party_names[str(party_id)]))

    for record in read_jsonl(curated_dir / "process_counsel_link.jsonl"):
        process_id = record.get("process_id")
        counsel_id = record.get("counsel_id")
        if process_id and counsel_id and counsel_id in counsel_names:
            process_counsels[str(process_id)].append((str(counsel_id), counsel_names[str(counsel_id)]))

    return dict(process_parties), dict(process_counsels)


def _process_context(
    curated_dir: Path,
) -> tuple[dict[str, set[str]], dict[str, tuple[str, str]], dict[str, tuple[int, int]]]:
    process_ministers: dict[str, set[str]] = defaultdict(set)
    decision_event_context: dict[str, tuple[str, str]] = {}
    _year_min: dict[str, int] = {}
    _year_max: dict[str, int] = {}

    for record in read_jsonl(curated_dir / "decision_event.jsonl"):
        process_id = record.get("process_id")
        minister_name = record.get("current_rapporteur")
        decision_event_id = record.get("decision_event_id")
        if process_id and minister_name:
            pid = str(process_id)
            process_ministers[pid].add(str(minister_name))
            if decision_event_id:
                decision_event_context[str(decision_event_id)] = (pid, str(minister_name))
            decision_date = record.get("decision_date")
            if decision_date and isinstance(decision_date, str) and len(decision_date) >= 4:
                try:
                    year = int(decision_date[:4])
                    if 1900 <= year <= 2100:
                        if pid not in _year_min or year < _year_min[pid]:
                            _year_min[pid] = year
                        if pid not in _year_max or year > _year_max[pid]:
                            _year_max[pid] = year
                except ValueError:
                    pass

    process_years = {pid: (_year_min[pid], _year_max[pid]) for pid in _year_min}
    return dict(process_ministers), decision_event_context, process_years


def _pair_process_map(
    process_ministers: dict[str, set[str]],
    process_parties: dict[str, list[tuple[str, str]]],
    process_counsels: dict[str, list[tuple[str, str]]],
) -> dict[tuple[str, str, str], set[str]]:
    pair_processes: dict[tuple[str, str, str], set[str]] = defaultdict(set)

    for process_id, ministers in process_ministers.items():
        for minister_name in ministers:
            for entity_id, _entity_name in process_parties.get(process_id, []):
                pair_processes[(minister_name, "party", entity_id)].add(process_id)
            for entity_id, _entity_name in process_counsels.get(process_id, []):
                pair_processes[(minister_name, "counsel", entity_id)].add(process_id)

    return dict(pair_processes)


def _pair_process_index(
    pair_processes: dict[tuple[str, str, str], set[str]],
) -> tuple[dict[str, list[tuple[str, set[str]]]], dict[str, list[tuple[str, set[str]]]]]:
    party_pairs: dict[str, list[tuple[str, set[str]]]] = defaultdict(list)
    counsel_pairs: dict[str, list[tuple[str, set[str]]]] = defaultdict(list)

    for (minister_name, entity_type, entity_id), process_ids in pair_processes.items():
        if entity_type == "party":
            party_pairs[entity_id].append((minister_name, process_ids))
        elif entity_type == "counsel":
            counsel_pairs[entity_id].append((minister_name, process_ids))

    return dict(party_pairs), dict(counsel_pairs)


def _qualifies_as_signal(row: dict[str, Any]) -> bool:
    """Decide if an analytics row qualifies as a compound risk signal.

    When ``red_flag_substantive`` is present in the row (even as False or
    None), it is the sole authority.  The legacy ``red_flag`` field only
    governs when the substantive field is absent — i.e. for old data or
    analytics sources that do not compute it (e.g. rapporteur_change).
    """
    if "red_flag_substantive" in row:
        return row["red_flag_substantive"] is True
    return bool(row.get("red_flag"))


def _load_rows(path: Path, *, red_flag_only: bool = False) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = read_jsonl(path)
    if red_flag_only:
        return [row for row in rows if _qualifies_as_signal(row)]
    return rows


def _evidence_for(
    pairs: dict[tuple[str, str, str], PairEvidence],
    minister_name: str,
    entity_type: str,
    entity_id: str,
    entity_name: str,
) -> PairEvidence:
    key = (minister_name, entity_type, entity_id)
    if key not in pairs:
        pairs[key] = PairEvidence(
            minister_name=minister_name,
            entity_type=entity_type,
            entity_id=entity_id,
            entity_name=entity_name,
        )
    return pairs[key]


def _build_signal_details(evidence: PairEvidence) -> dict[str, dict[str, Any]]:
    """Build sparse dict of per-signal metadata from already-collected PairEvidence."""
    details: dict[str, dict[str, Any]] = {}
    if "sanction" in evidence.signals:
        details["sanction"] = {
            "count": evidence.sanction_match_count,
            "sources": sorted(s for s in evidence.sanction_sources if s),
        }
        if evidence.sanction_corporate_link_count > 0:
            details["sanction"]["scl_count"] = evidence.sanction_corporate_link_count
            details["sanction"]["scl_min_degree"] = evidence.sanction_corporate_min_degree
    if "donation" in evidence.signals:
        details["donation"] = {
            "count": evidence.donation_match_count,
            "total_brl": round(evidence.donation_total_brl, 2),
        }
        if evidence.has_law_firm_group:
            details["donation"]["is_law_firm_group"] = True
        if evidence.donor_group_has_minister_partner:
            details["donation"]["donor_group_has_minister_partner"] = True
        if evidence.donor_group_has_party_partner:
            details["donation"]["donor_group_has_party_partner"] = True
        if evidence.donor_group_has_counsel_partner:
            details["donation"]["donor_group_has_counsel_partner"] = True
        if evidence.min_link_degree_to_minister is not None:
            details["donation"]["min_link_degree_to_minister"] = evidence.min_link_degree_to_minister
        if evidence.max_economic_group_member_count is not None:
            details["donation"]["economic_group_member_count"] = evidence.max_economic_group_member_count
        if evidence.max_red_flag_power is not None:
            details["donation"]["red_flag_power"] = evidence.max_red_flag_power
        if evidence.donation_has_corporate_link_red_flag:
            details["donation"]["corporate_link_red_flag"] = True
        meta = evidence.donation_enrichment_meta
        if meta["economic_group_ids"]:
            details["donation"]["economic_group_ids"] = sorted(meta["economic_group_ids"])
        if meta["donor_cnpj_basicos"]:
            details["donation"]["donor_cnpj_basicos"] = sorted(meta["donor_cnpj_basicos"])
        if meta["donor_company_names"]:
            details["donation"]["donor_company_names"] = sorted(meta["donor_company_names"])
        if meta["match_strategies"]:
            details["donation"]["match_strategies"] = sorted(meta["match_strategies"])
        if meta["red_flag_confidences"]:
            details["donation"]["red_flag_confidences"] = sorted(meta["red_flag_confidences"])
    if "corporate" in evidence.signals:
        min_degree = min(
            (c["link_degree"] for c in evidence.corporate_companies.values()),
            default=1,
        )
        details["corporate"] = {
            "count": evidence.corporate_conflict_count,
            "company_count": len(evidence.corporate_companies),
            "min_link_degree": min_degree,
        }
    if "affinity" in evidence.signals:
        details["affinity"] = {
            "count": evidence.affinity_count,
            "affinity_ids": sorted(a for a in evidence.affinity_ids if a),
        }
    if "alert" in evidence.signals:
        details["alert"] = {
            "count": len(evidence.alert_ids),
            "max_score": evidence.max_alert_score,
        }
    if "velocity" in evidence.signals:
        details["velocity"] = {"flagged": True}
    if "redistribution" in evidence.signals:
        details["redistribution"] = {"flagged": True}
    return details


def _sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            -int(row["signal_count"]),
            -(row["adjusted_rate_delta"] or 0.0),
            -(row["max_alert_score"] or 0.0),
            -int(row["shared_process_count"]),
            row["minister_name"],
            row["entity_name"],
        ),
    )


def build_compound_risk(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    analytics_dir: Path = DEFAULT_ANALYTICS_DIR,
    output_dir: Path = DEFAULT_ANALYTICS_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build minister-entity compound risk rankings from converging signals."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if not _required_inputs_exist(curated_dir):
        logger.warning("Compound risk skipped: curated inputs missing under %s", curated_dir)
        return output_dir

    if on_progress:
        on_progress(0, 4, "Compound Risk: Carregando dados...")
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

    sanction_by_party: dict[str, list[dict[str, Any]]] = defaultdict(list)
    donation_by_party: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pairs: dict[tuple[str, str, str], PairEvidence] = {}
    applied_cross_entity_donations: set[tuple[str, str, str]] = set()
    # Track counsels with direct sanction/donation matches to avoid duplicate
    # propagation via cross-entity inference.
    counsels_with_direct_sanction: set[str] = set()
    counsels_with_direct_donation: set[str] = set()

    if on_progress:
        on_progress(1, 4, "Compound Risk: Cruzando sinais...")
    for row in sanction_rows:
        row_entity_type = str(row.get("entity_type") or "party")
        if row_entity_type == "counsel":
            counsel_id = str(row.get("entity_id") or "")
            counsel_name = str(row.get("entity_name_normalized") or counsel_names.get(counsel_id) or "")
            if not counsel_id or counsel_id not in counsel_names:
                continue
            counsels_with_direct_sanction.add(counsel_id)
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
            sanction_by_party[party_id].append(row)
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
                if link_id:
                    evidence.sanction_corporate_link_ids.add(link_id)
                cur_min = evidence.sanction_corporate_min_degree
                if cur_min is None or link_degree < cur_min:
                    evidence.sanction_corporate_min_degree = link_degree
        elif stf_entity_type == "counsel":
            for minister_name, process_ids in counsel_pair_processes.get(stf_entity_id, []):
                evidence = _evidence_for(pairs, minister_name, "counsel", stf_entity_id, stf_entity_name)
                evidence.sanction_corporate_link_count += 1
                if link_id:
                    evidence.sanction_corporate_link_ids.add(link_id)
                cur_min = evidence.sanction_corporate_min_degree
                if cur_min is None or link_degree < cur_min:
                    evidence.sanction_corporate_min_degree = link_degree

    # SCL promotion: promote "sanction" family when SCL exists but no direct sanction
    for evidence in pairs.values():
        if evidence.sanction_corporate_link_count > 0 and "sanction" not in evidence.signals:
            evidence.signals.add("sanction")

    if on_progress:
        on_progress(2, 4, "Compound Risk: Vinculando alertas...")
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
                "sanction_corporate_link_ids": sorted(
                    lid for lid in evidence.sanction_corporate_link_ids if lid
                ),
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
    if on_progress:
        on_progress(3, 4, "Compound Risk: Gravando resultados...")
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
    if on_progress:
        on_progress(4, 4, "Compound Risk: Concluído")
    return output_path
