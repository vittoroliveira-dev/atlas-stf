"""Cross-reference metrics: module inventory, rule quality, sensitivity analysis.

Standalone (zero atlas_stf imports). Reads ``data/analytics/`` summary JSON files.
Extracted from crossref_audit.py to keep each module under 500 lines.
"""

from __future__ import annotations

import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Policy tiers — derived from Prompt 4 decisions.
# ---------------------------------------------------------------------------

POLICY_TIER: dict[str, str] = {
    "tax_id": "nucleus",
    "exact": "nucleus",
    "canonical_name": "nucleus",
    "alias": "nucleus",
    "jaccard_gte_085": "nucleus",
    "jaccard_080_085": "restricted",
    "levenshtein_dist1_long": "restricted",
    "counsel_affinity": "restricted",
    "graph_scoring": "restricted",
    "fallback_name_pj": "restricted",
    "path_bc_partner": "restricted",
    "scl_degree2_deterministic": "restricted",
    "compound_risk_inferred": "restricted",
    "compound_risk_scl_promoted": "restricted",
    "red_flag_low_n": "restricted",
    "levenshtein_dist2": "enrichment",
    "fallback_name_pf": "enrichment",
    "scl_degree2_fuzzy": "enrichment",
    "counsel_network": "enrichment",
    "firm_cluster": "enrichment",
    "economic_group": "enrichment",
    "ambiguous": "review_required",
    "scl_degree3": "review_required",
    "compound_risk_inferred_only": "review_required",
    "levenshtein_short_name": "remove",
    "corporate_network_empty": "remove",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def read_json(path: Path) -> dict | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def safe_int(val: object) -> int:
    if val is None:
        return 0
    return int(str(val))


def safe_pct(num: int, den: int) -> float:
    return round(num / den * 100, 2) if den > 0 else 0.0


# ---------------------------------------------------------------------------
# Module inventory
# ---------------------------------------------------------------------------

MODULES = [
    ("donation_match", "donation_match_summary.json"),
    ("sanction_match", "sanction_match_summary.json"),
    ("donor_corporate_link", "donor_corporate_link_summary.json"),
    ("sanction_corporate_link", "sanction_corporate_link_summary.json"),
    ("economic_group", "economic_group_summary.json"),
    ("corporate_network", "corporate_network_summary.json"),
    ("counsel_affinity", "counsel_affinity_summary.json"),
    ("counsel_network", "counsel_network_cluster_summary.json"),
    ("compound_risk", "compound_risk_summary.json"),
    ("decision_velocity", "decision_velocity_summary.json"),
    ("rapporteur_change", "rapporteur_change_summary.json"),
    ("firm_cluster", "firm_cluster_summary.json"),
    ("match_calibration", "match_calibration_summary.json"),
]

_COUNT_KEYS = (
    "donation_match_count",
    "sanction_match_count",
    "pair_count",
    "total_clusters",
    "total_conflicts",
    "total_audits",
    "total_changes",
    "total_amicus_lawyers",
)

_METRIC_KEYS = frozenset(
    {
        "red_flag_count",
        "total_ambiguous_candidate_count",
        "ambiguous_candidate_count",
        "signal_frequency",
        "truncated_sanctions_count",
        "degree_counts",
    }
)


def inventory(analytics_dir: Path, module_cls: type) -> list:
    """Build module inventory. module_cls must accept (name, summary_file, exists, ...)."""
    results = []
    for name, summary_file in MODULES:
        path = analytics_dir / summary_file
        data = read_json(path)
        status = module_cls(name=name, summary_file=summary_file, exists=data is not None)
        if data:
            for key in _COUNT_KEYS:
                if key in data:
                    status.record_count = safe_int(data[key])
                    break
            status.key_metrics = {k: v for k, v in data.items() if k in _METRIC_KEYS}
        results.append(status)
    return results


# ---------------------------------------------------------------------------
# Rule quality
# ---------------------------------------------------------------------------


def rule_quality(analytics_dir: Path, rule_cls: type) -> list:
    """Compute rule quality metrics from empirical summary files."""
    rules: list = []
    empirical = read_json(analytics_dir / "donation_empirical_metrics.json")
    don_summary = read_json(analytics_dir / "donation_match_summary.json")
    scl_summary = read_json(analytics_dir / "sanction_corporate_link_summary.json")
    cr_summary = read_json(analytics_dir / "compound_risk_summary.json")
    cn_summary = read_json(analytics_dir / "corporate_network_summary.json")

    def _rq(rule: str, m: int, a: int, rf: int, conf: str, ev: str) -> object:
        return rule_cls(rule, m, a, rf, conf, POLICY_TIER.get(rule, "unknown"), ev)

    if empirical:
        mq = empirical.get("match_quality", {})
        st = mq.get("match_strategy_distribution", {})
        rf = mq.get("red_flag_by_strategy", {})
        jh = mq.get("jaccard_score_histogram", {})
        lh = mq.get("levenshtein_score_histogram", {})
        am = empirical.get("ambiguous_analysis", {})
        raw = empirical.get("raw_data_quality", {})
        j85 = sum(v for k, v in jh.items() if k != "[0.80, 0.85)")
        j80 = safe_int(jh.get("[0.80, 0.85)"))
        ta = safe_int(am.get("total_ambiguous"))
        rules.extend(
            [
                _rq("tax_id", 0, 0, 0, "high", "85.71% CPF; 0 tax_id matches in run"),
                _rq(
                    "exact",
                    safe_int(st.get("exact")),
                    0,
                    safe_int(rf.get("exact")),
                    "high",
                    f"{st.get('exact', 0)} matches, {rf.get('exact', 0)} RF",
                ),
                _rq(
                    "canonical_name",
                    safe_int(st.get("canonical_name")),
                    0,
                    safe_int(rf.get("canonical_name")),
                    "medium_high",
                    f"Low volume ({st.get('canonical_name', 0)})",
                ),
                _rq("jaccard_gte_085", j85, 0, 0, "medium_high", f"{j85} matches Jaccard>=0.85"),
                _rq(
                    "jaccard_080_085",
                    j80,
                    ta,
                    safe_int(rf.get("jaccard")),
                    "medium",
                    f"{j80} in [0.80,0.85), {ta} ambiguous",
                ),
                _rq(
                    "levenshtein_dist2",
                    safe_int(lh.get("2")),
                    0,
                    safe_int(rf.get("levenshtein")),
                    "low",
                    f"4/10 incorrect. {lh.get('2', 0)} dist=2",
                ),
                _rq("levenshtein_short_name", 35, 0, 0, "very_low", "35 total, 6/15 incorrect"),
                _rq("ambiguous", ta, ta, 0, "low", "9/10 insuff. No tiebreak."),
            ]
        )
        cpf_n = safe_int(raw.get("identity_key_cpf_count"))
        name_n = safe_int(raw.get("identity_key_name_count"))
        total_k = cpf_n + name_n
        if total_k > 0:
            nr = name_n / total_k
            mt = safe_int(don_summary.get("donation_match_count", 0)) if don_summary else 0
            rules.append(_rq("fallback_name_pf", int(mt * nr), 0, 0, "low", f"~{nr * 100:.1f}% no CPF. Homonym risk."))
    if scl_summary:
        dg = scl_summary.get("degree_counts", {})
        tr = safe_int(scl_summary.get("truncated_sanctions_count"))
        tl = safe_int(scl_summary.get("total_links"))
        rules.append(
            _rq(
                "scl_degree2_deterministic",
                safe_int(dg.get("2")),
                0,
                0,
                "medium",
                f"{dg.get('2', 0)} deg-2. Deterministic terminal.",
            )
        )
        rules.append(
            _rq("scl_degree3", safe_int(dg.get("3")), 0, 0, "very_low", f"{dg.get('3', 0)} deg-3. {tr}/{tl} trunc.")
        )
    if cn_summary:
        tc = safe_int(cn_summary.get("total_conflicts"))
        rules.append(_rq("corporate_network_empty", tc, 0, 0, "inconclusive", f"{tc} conflicts in production."))
    if cr_summary:
        sig = cr_summary.get("signal_frequency", {})
        rules.append(
            rule_cls(
                "compound_risk_all",
                safe_int(cr_summary.get("pair_count")),
                0,
                safe_int(cr_summary.get("red_flag_count")),
                "varies",
                "varies",
                f"Signals: {json.dumps(sig, sort_keys=True)}",
            )
        )
    return rules


# ---------------------------------------------------------------------------
# Sensitivity scenarios
# ---------------------------------------------------------------------------


def sensitivity(analytics_dir: Path, scenario_cls: type) -> list:
    """Compute sensitivity scenarios from empirical metrics."""
    empirical = read_json(analytics_dir / "donation_empirical_metrics.json")
    if not empirical:
        return []

    mq = empirical.get("match_quality", {})
    strategy = mq.get("match_strategy_distribution", {})
    rf_strat = mq.get("red_flag_by_strategy", {})
    jhist = mq.get("jaccard_score_histogram", {})

    total_m = sum(strategy.values())
    total_rf = sum(rf_strat.values())

    exact_m = safe_int(strategy.get("exact")) + safe_int(strategy.get("canonical_name"))
    exact_rf = safe_int(rf_strat.get("exact")) + safe_int(rf_strat.get("canonical_name"))

    j085 = sum(v for k, v in jhist.items() if k != "[0.80, 0.85)")
    inter_m = exact_m + j085
    inter_rf = exact_rf  # red flags by Jaccard score range not available; conservative

    return [
        scenario_cls(
            "strict",
            "Only exact + canonical matches (deterministic name)",
            exact_m,
            exact_rf,
            exact_m - total_m,
            exact_rf - total_rf,
            safe_pct(exact_m, total_m),
            safe_pct(exact_rf, total_rf),
        ),
        scenario_cls(
            "intermediate",
            "Strict + Jaccard >= 0.85 (high-confidence fuzzy)",
            inter_m,
            inter_rf,
            inter_m - total_m,
            inter_rf - total_rf,
            safe_pct(inter_m, total_m),
            safe_pct(inter_rf, total_rf),
        ),
        scenario_cls(
            "full",
            "All rules including Jaccard [0.80,0.85) + Levenshtein + ambiguous",
            total_m,
            total_rf,
            0,
            0,
            100.0,
            100.0,
        ),
    ]


# ---------------------------------------------------------------------------
# Quality metrics materialization
# ---------------------------------------------------------------------------


def write_quality_observed(gold_set: object, generated_at: str, path: Path) -> None:
    """Materialize observed match quality metrics as standalone artifact.

    Measures matching quality in a gold set sample — NOT downstream operational
    impact (coverage, manual review reduction, served link changes, etc.).
    """
    from dataclasses import asdict

    quality_list = getattr(gold_set, "quality_by_stratum", [])
    total = getattr(gold_set, "total", 0)
    total_c = sum(getattr(q, "correct", 0) for q in quality_list)
    total_ic = sum(getattr(q, "incorrect", 0) for q in quality_list)
    total_amb = sum(getattr(q, "ambiguous", 0) for q in quality_list)
    total_ins = sum(getattr(q, "insufficient", 0) for q in quality_list)
    definitive = total_c + total_ic
    data = {
        "description": (
            "Observed match quality in gold set sample. "
            "Does NOT measure downstream operational impact. No causal inference."
        ),
        "metric_definitions": {
            "observed_precision": "correct / (correct + incorrect). Excludes ambiguous/insufficient.",
            "false_positive_rate": "incorrect / (correct + incorrect). Same denominator.",
            "ambiguity_rate": "(ambiguous + insufficient) / total_in_stratum.",
            "resolution_rate": "(correct + incorrect) / total.",
        },
        "gold_set_total": total,
        "gold_set_labels": getattr(gold_set, "by_label", {}),
        "heuristic_override_count": getattr(gold_set, "heuristic_override_count", 0),
        "quality_by_stratum": [asdict(q) for q in quality_list],
        "aggregate": {
            "total_correct": total_c,
            "total_incorrect": total_ic,
            "total_ambiguous": total_amb,
            "total_insufficient": total_ins,
            "observed_precision": round(total_c / definitive, 4) if definitive > 0 else None,
            "false_positive_rate": round(total_ic / definitive, 4) if definitive > 0 else None,
            "ambiguity_rate": round((total_amb + total_ins) / total, 4) if total > 0 else None,
            "resolution_rate": round(definitive / total, 4) if total > 0 else None,
            "note": "Sample-based. Not generalizable without stratified weighting.",
        },
        "scope_limitations": [
            "Measures matching quality only, not operational impact.",
            "Sample size per stratum is small (10-20); confidence intervals are wide.",
            "Ambiguous records are excluded from precision/FP denominators by design.",
        ],
        "generated_at": generated_at,
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
