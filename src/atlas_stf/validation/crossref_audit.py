"""Cross-reference audit: inventory, quality metrics, sensitivity, policy checks.

Standalone (zero atlas_stf imports).  Reads ``data/analytics/`` summary files.
Run: ``uv run python -m atlas_stf.validation.crossref_audit``
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ._xref_metrics import read_json, safe_int

# ---------------------------------------------------------------------------
# Data types (public API)
# ---------------------------------------------------------------------------


@dataclass
class ModuleStatus:
    name: str
    summary_file: str
    exists: bool
    record_count: int | None = None
    key_metrics: dict[str, object] = field(default_factory=dict)


@dataclass
class RuleQuality:
    rule: str
    match_count: int
    ambiguous_count: int
    red_flag_count: int
    confidence: str
    policy_tier: str
    evidence: str


@dataclass
class SensitivityScenario:
    name: str
    description: str
    match_count: int
    red_flag_count: int
    delta_matches_vs_full: int
    delta_red_flags_vs_full: int
    pct_matches_of_full: float
    pct_red_flags_of_full: float


@dataclass
class PolicyCheck:
    check: str
    status: str
    detail: str


@dataclass
class StratumQuality:
    """Observed quality metrics for a single stratum, derived from gold set."""

    stratum: str
    gold_n: int
    correct: int
    incorrect: int
    ambiguous: int
    insufficient: int
    observed_precision: float | None  # correct / (correct + incorrect); None if 0 definitive
    false_positive_rate: float | None  # incorrect / (correct + incorrect)
    ambiguity_rate: float  # (ambiguous + insufficient) / gold_n


@dataclass
class GoldSetSummary:
    status: str  # available / not_available
    total: int
    by_stratum: dict[str, int]
    by_label: dict[str, int]
    limitations: list[str]
    quality_by_stratum: list[StratumQuality] = field(default_factory=list)
    heuristic_override_count: int = 0  # records where final_label != heuristic_label


@dataclass
class CrossrefAuditReport:
    generated_at: str
    modules: list[ModuleStatus]
    rule_quality: list[RuleQuality]
    sensitivity: list[SensitivityScenario]
    policy_checks: list[PolicyCheck]
    gold_set: GoldSetSummary


# ---------------------------------------------------------------------------
# Policy checks
# ---------------------------------------------------------------------------

MINIMUM_GOLD_SET_SIZE = 100
"""Gold set must have at least this many records for a PASS policy check."""

_REQUIRED_GOLD_STRATA = {"counsel_match", "levenshtein_dist1", "scl_degree2"}


def _policy_checks(
    modules: list[ModuleStatus],
    rules: list[RuleQuality],
    analytics_dir: Path,
) -> list[PolicyCheck]:
    checks: list[PolicyCheck] = []

    # Ambiguous matches should not be in serving score
    ambig = next((r for r in rules if r.rule == "ambiguous"), None)
    if ambig and ambig.match_count > 0:
        checks.append(
            PolicyCheck(
                "ambiguous_not_in_score",
                "WARN",
                f"{ambig.match_count} ambiguous matches exist. Policy: must not enter score. "
                f"Verify serving builder excludes strategy='ambiguous' from score computation.",
            )
        )

    # Corporate network empty
    cn = next((m for m in modules if m.name == "corporate_network"), None)
    if cn and cn.exists and (cn.record_count or 0) == 0:
        checks.append(
            PolicyCheck(
                "corporate_network_no_data",
                "WARN",
                "corporate_network builder ran but produced 0 conflicts. "
                "Policy: disable outputs until data is produced.",
            )
        )

    # Match calibration: validate presence + integrity
    mc = next((m for m in modules if m.name == "match_calibration"), None)
    _check_match_calibration(mc, analytics_dir, checks)

    # Gold set: enough ADJUDICATED records (final_label set)
    _check_gold_set(analytics_dir, checks)

    # SCL truncation rate
    scl_summary = read_json(analytics_dir / "sanction_corporate_link_summary.json")
    if scl_summary:
        trunc = safe_int(scl_summary.get("truncated_sanctions_count"))
        total = safe_int(scl_summary.get("total_links"))
        if total > 0:
            rate = trunc / total * 100
            status = "WARN" if rate > 50 else "PASS"
            detail = (
                f"SCL truncation: {trunc}/{total} ({rate:.1f}%). Mega-component affects majority of links."
                if rate > 50
                else f"SCL truncation: {trunc}/{total} ({rate:.1f}%). Within acceptable range."
            )
            checks.append(PolicyCheck("scl_truncation_rate", status, detail))

    return checks


def _check_match_calibration(mc: ModuleStatus | None, analytics_dir: Path, checks: list[PolicyCheck]) -> None:
    if not mc:
        return
    if not mc.exists:
        checks.append(
            PolicyCheck(
                "match_calibration_not_run",
                "WARN",
                "match_calibration_summary.json not found. Thresholds not empirically validated.",
            )
        )
        return
    mc_data = read_json(analytics_dir / "match_calibration_summary.json")
    missing: list[str] = []
    if mc_data:
        for f in ("git_commit", "source_dataset", "thresholds_evaluated", "execution_status"):
            if f not in mc_data:
                missing.append(f)
        if mc_data.get("execution_status") != "complete":
            missing.append(f"execution_status={mc_data.get('execution_status')!r}")
    if missing:
        checks.append(
            PolicyCheck(
                "match_calibration_incomplete",
                "WARN",
                f"Summary exists but missing/invalid: {', '.join(missing)}. Rerun: make calibrate-match",
            )
        )
    else:
        checks.append(
            PolicyCheck(
                "match_calibration_exists",
                "PASS",
                "match_calibration executed and summary is complete.",
            )
        )


def _check_gold_set(analytics_dir: Path, checks: list[PolicyCheck]) -> None:
    gold_path = analytics_dir / "gold_set_matches.jsonl"
    if not gold_path.exists() or gold_path.stat().st_size == 0:
        checks.append(
            PolicyCheck(
                "gold_set_missing",
                "FAIL",
                f"No gold set (gold_set_matches.jsonl). Minimum: {MINIMUM_GOLD_SET_SIZE} adjudicated records. "
                "Run: uv run python scripts/build_gold_set.py generate",
            )
        )
        return

    gs_total = 0
    gs_adjudicated = 0
    with open(gold_path, encoding="utf-8") as fh:
        for ln in fh:
            if not ln.strip():
                continue
            gs_total += 1
            rec = json.loads(ln)
            if rec.get("final_label") is not None:
                gs_adjudicated += 1
            elif rec.get("label") is not None and "adjudication_type" not in rec:
                gs_adjudicated += 1  # legacy single-layer schema

    if gs_adjudicated >= MINIMUM_GOLD_SET_SIZE:
        checks.append(
            PolicyCheck(
                "gold_set_available",
                "PASS",
                f"Gold set: {gs_adjudicated}/{gs_total} adjudicated records.",
            )
        )
    else:
        checks.append(
            PolicyCheck(
                "gold_set_below_minimum",
                "FAIL",
                f"Gold set: {gs_adjudicated} adjudicated records (total {gs_total}), "
                f"below minimum {MINIMUM_GOLD_SET_SIZE}. "
                "Run: uv run python scripts/build_gold_set.py review",
            )
        )


# ---------------------------------------------------------------------------
# Gold set summary
# ---------------------------------------------------------------------------


def _gold_set_summary(analytics_dir: Path) -> GoldSetSummary:
    gold_path = analytics_dir / "gold_set_matches.jsonl"
    if not gold_path.exists() or gold_path.stat().st_size == 0:
        return GoldSetSummary("not_available", 0, {}, {}, ["Gold set file does not exist."])

    by_stratum: dict[str, int] = {}
    by_label: dict[str, int] = {}
    by_adjudication: dict[str, int] = {}
    # Per-stratum label counters for quality metrics
    stratum_labels: dict[str, dict[str, int]] = {}
    total = 0
    overrides = 0

    with open(gold_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            total += 1
            stratum = rec.get("stratum", "unknown")
            by_stratum[stratum] = by_stratum.get(stratum, 0) + 1
            label = rec.get("final_label") or rec.get("heuristic_label") or rec.get("label", "unknown")
            by_label[label] = by_label.get(label, 0) + 1
            adj_type = rec.get("adjudication_type", "unknown")
            by_adjudication[adj_type] = by_adjudication.get(adj_type, 0) + 1
            # Track per-stratum final labels
            if label:
                if stratum not in stratum_labels:
                    stratum_labels[stratum] = {}
                stratum_labels[stratum][label] = stratum_labels[stratum].get(label, 0) + 1
            # Count heuristic→final overrides
            hl = rec.get("heuristic_label")
            fl = rec.get("final_label")
            if hl and fl and hl != fl:
                overrides += 1
    limitations = []
    if total < MINIMUM_GOLD_SET_SIZE:
        limitations.append(
            f"Below minimum ({total}/{MINIMUM_GOLD_SET_SIZE} records). "
            "Run: uv run python scripts/build_gold_set.py generate"
        )
    missing_strata = _REQUIRED_GOLD_STRATA - set(by_stratum)
    if missing_strata:
        limitations.append(f"Required strata missing: {', '.join(sorted(missing_strata))}")
    heuristic_only = by_adjudication.get("heuristic_provisional", 0)
    if heuristic_only > 0:
        limitations.append(
            f"{heuristic_only}/{total} records are heuristic_provisional (pending human review). "
            "Run: uv run python scripts/build_gold_set.py review"
        )
    # Compute observed quality per stratum
    quality: list[StratumQuality] = []
    for s_name in sorted(stratum_labels):
        lbl = stratum_labels[s_name]
        c = lbl.get("correct", 0)
        ic = lbl.get("incorrect", 0)
        amb = lbl.get("ambiguous", 0)
        ins = lbl.get("insufficient", 0)
        n = c + ic + amb + ins
        definitive = c + ic
        quality.append(
            StratumQuality(
                stratum=s_name,
                gold_n=n,
                correct=c,
                incorrect=ic,
                ambiguous=amb,
                insufficient=ins,
                observed_precision=round(c / definitive, 4) if definitive > 0 else None,
                false_positive_rate=round(ic / definitive, 4) if definitive > 0 else None,
                ambiguity_rate=round((amb + ins) / n, 4) if n > 0 else 0.0,
            )
        )

    return GoldSetSummary(
        "available",
        total,
        dict(sorted(by_stratum.items())),
        dict(sorted(by_label.items())),
        limitations,
        quality_by_stratum=quality,
        heuristic_override_count=overrides,
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_crossref_audit(
    *,
    analytics_dir: Path = Path("data/analytics"),
) -> CrossrefAuditReport:
    """Run cross-reference audit and return structured report."""
    from ._xref_metrics import inventory, rule_quality, sensitivity

    modules = inventory(analytics_dir, ModuleStatus)
    rules = rule_quality(analytics_dir, RuleQuality)
    sens = sensitivity(analytics_dir, SensitivityScenario)
    checks = _policy_checks(modules, rules, analytics_dir)
    gold = _gold_set_summary(analytics_dir)

    return CrossrefAuditReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        modules=modules,
        rule_quality=rules,
        sensitivity=sens,
        policy_checks=checks,
        gold_set=gold,
    )


# ---------------------------------------------------------------------------
# CLI / printing
# ---------------------------------------------------------------------------

_SYM = {"PASS": "\033[32m✓\033[0m", "WARN": "\033[33m⚠\033[0m",
        "FAIL": "\033[31m✗\033[0m", "SKIP": "\033[90m-\033[0m"}  # fmt: skip


def _print_report(rpt: CrossrefAuditReport) -> None:
    sep = "=" * 60
    print(f"\n{sep}\nCross-Reference Audit — {rpt.generated_at[:19]}\n{sep}")
    active = sum(1 for m in rpt.modules if m.exists)
    print(f"\n── Modules ({active}/{len(rpt.modules)} active) ──")
    for m in rpt.modules:
        s = "✓" if m.exists else "✗"
        c = f" ({m.record_count})" if m.record_count is not None else ""
        print(f"  [{s}] {m.name}{c}")
    print(f"\n── Rules ({len(rpt.rule_quality)}) ──")
    for r in rpt.rule_quality:
        print(
            f"  [{r.confidence:>12s}] {r.rule:<30s} m={r.match_count:>7d} "
            f"a={r.ambiguous_count:>6d} rf={r.red_flag_count:>6d} {r.policy_tier.upper()}"
        )
    if rpt.sensitivity:
        print(f"\n── Sensitivity ({len(rpt.sensitivity)}) ──")
        for s in rpt.sensitivity:
            print(
                f"  {s.name:<12s} m={s.match_count:>7d}({s.pct_matches_of_full:5.1f}%) "
                f"rf={s.red_flag_count:>6d}({s.pct_red_flags_of_full:5.1f}%) "
                f"Δm={s.delta_matches_vs_full:>+7d} Δrf={s.delta_red_flags_vs_full:>+6d}"
            )
    print(f"\n── Policy Checks ({len(rpt.policy_checks)}) ──")
    for c in rpt.policy_checks:
        print(f"  {_SYM.get(c.status, '?')} {c.check}: {c.detail}")
    gs = rpt.gold_set
    print(f"\n── Gold Set: {gs.status} ({gs.total} records) ──")
    for s, n in gs.by_stratum.items():
        print(f"    {s}: {n}")
    if gs.by_label:
        print(f"  Labels: {gs.by_label}")
    if gs.heuristic_override_count > 0:
        print(f"  Overrides: {gs.heuristic_override_count}")
    if gs.quality_by_stratum:
        print(f"\n── Observed Quality (gold set, N={gs.total}) ──")
        hdr = f"  {'Stratum':<25s} {'N':>4s} {'Prec':>6s} {'FP%':>6s} {'Amb%':>6s}"
        print(hdr)
        for q in gs.quality_by_stratum:
            p = f"{q.observed_precision:.0%}" if q.observed_precision is not None else "n/d"
            f = f"{q.false_positive_rate:.0%}" if q.false_positive_rate is not None else "n/d"
            print(f"  {q.stratum:<25s} {q.gold_n:>4d} {p:>6s} {f:>6s} {q.ambiguity_rate:>5.0%}")
    for lim in gs.limitations:
        print(f"  ⚠ {lim}")
    print(f"{sep}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Cross-reference audit for Atlas STF matching pipeline",
    )
    parser.add_argument(
        "--analytics-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Path to analytics artifacts directory",
    )
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    parser.add_argument("--output", type=Path, default=None, help="Write JSON report to file")
    args = parser.parse_args(argv)

    report = run_crossref_audit(analytics_dir=args.analytics_dir)

    if args.json:
        json.dump(asdict(report), sys.stdout, indent=2, ensure_ascii=False, default=str)
        print()
    else:
        _print_report(report)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(asdict(report), f, indent=2, ensure_ascii=False, default=str)
        from ._xref_metrics import write_quality_observed

        quality_path = args.output.with_name("match_quality_observed.json")
        write_quality_observed(report.gold_set, report.generated_at, quality_path)
        if not args.json:
            print(f"Report written to {args.output}")
            print(f"Quality metrics written to {quality_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
