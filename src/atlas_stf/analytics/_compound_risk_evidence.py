"""Evidence accumulation types and helpers for compound risk analytics."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "PairEvidence",
    "_build_signal_details",
    "_coerce_float",
    "_coerce_str_list",
    "_compute_adjusted_rate_delta",
    "_sort_rows",
]


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
            except TypeError, ValueError:
                d = None  # type: ignore[assignment]
            if d is not None and (self.min_link_degree_to_minister is None or d < self.min_link_degree_to_minister):
                self.min_link_degree_to_minister = d
        member_count = row.get("economic_group_member_count")
        if member_count is not None:
            try:
                mc = int(member_count)
            except TypeError, ValueError:
                mc = None  # type: ignore[assignment]
            if mc is not None and (
                self.max_economic_group_member_count is None or mc > self.max_economic_group_member_count
            ):
                self.max_economic_group_member_count = mc
        power = row.get("red_flag_power")
        if power is not None:
            try:
                p = float(power)
            except TypeError, ValueError:
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
    # has_law_firm_group: informational only, no longer amplifies risk score.
    # Previous 1.5x multiplier removed — flag was contaminated by ANY heuristic
    # on mega-components (75/69157 members ≠ "law firm group").
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
