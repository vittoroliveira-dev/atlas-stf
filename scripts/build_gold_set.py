"""Gold set CLI: generate, review, build, summary.

Generates a two-layer gold set separating heuristic suggestions from
adjudicated final labels. Only evidence_deterministic and human_review
records constitute the real gold set.

Usage:
    uv run python scripts/build_gold_set.py generate   # create review queue
    uv run python scripts/build_gold_set.py review      # interactive human review
    uv run python scripts/build_gold_set.py summary     # show current state
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gold_set import (
    MINIMUM_GOLD_SET_SIZE,
    adjudicated_count,
    build_summary,
    check_required_strata,
    generate_gold_set,
    print_summary,
    write_gold_set,
    write_summary_json,
)


def _cmd_generate(args: argparse.Namespace) -> int:
    """Generate gold set from production data."""
    analytics_dir = Path(args.analytics_dir)
    output = Path(args.output) if args.output else analytics_dir / "gold_set_matches.jsonl"

    records, population = generate_gold_set(analytics_dir=analytics_dir)

    # Check required strata
    violations = check_required_strata(records)
    if violations:
        for v in violations:
            print(f"ERROR: {v}", file=sys.stderr)
        return 1

    # Check minimum size
    if len(records) < MINIMUM_GOLD_SET_SIZE:
        print(f"ERROR: {len(records)} records, below minimum {MINIMUM_GOLD_SET_SIZE}.", file=sys.stderr)
        return 1

    # Write gold set
    write_gold_set(records, output)

    # Write summary
    summary = build_summary(records, str(output))
    write_summary_json(summary, output.with_name("gold_set_summary.json"))
    print_summary(summary)

    # Report adjudication status
    adj_count = adjudicated_count(records)
    pending = len(records) - adj_count
    print(f"Adjudicated (final_label set): {adj_count}/{len(records)}")
    if pending > 0:
        print(f"Pending human review: {pending} records")
        print("Run: uv run python scripts/build_gold_set.py review")

    return 0


def _cmd_review(args: argparse.Namespace) -> int:
    """Review pending records — interactive or curatorial batch."""
    analytics_dir = Path(args.analytics_dir)
    gold_path = analytics_dir / "gold_set_matches.jsonl"

    if not gold_path.exists():
        print("No gold set found. Run: uv run python scripts/build_gold_set.py generate", file=sys.stderr)
        return 1

    records: list[dict] = []
    with open(gold_path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    pending = [r for r in records if r.get("final_label") is None]
    if not pending:
        print("No pending records. All records are adjudicated.")
        return 0

    # --apply-curatorial: batch apply versionned curatorial decisions
    if args.apply_curatorial:
        return _apply_curatorial(records, gold_path)

    # Interactive review
    print(f"\n{len(pending)} records pending review.\n")
    print("For each record, enter:")
    print("  c = confirm heuristic label")
    print("  i = incorrect (override to 'incorrect')")
    print("  a = ambiguous (override to 'ambiguous')")
    print("  s = skip (leave pending)")
    print("  q = quit review\n")

    reviewer = args.reviewer or input("Reviewer name (e.g., vittor.oliveira): ").strip()
    if not reviewer:
        print("Reviewer name required.", file=sys.stderr)
        return 1

    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    reviewed = 0

    for rec in pending:
        print(f"\n--- {rec['case_id']} [{rec['stratum']}] ---")
        print(f"  Donor:    {rec.get('donor_name', '')}")
        print(f"  Entity:   {rec.get('entity_name', '')}")
        print(f"  Strategy: {rec['match_strategy']} (score={rec.get('match_score')})")
        print(f"  Tax ID:   {rec['has_tax_id']}")
        print(f"  Heuristic: {rec['heuristic_label']} ({rec['heuristic_basis']})")
        print(f"  Evidence: {rec.get('adjudication_evidence', '')}")

        choice = input("  Decision [c/i/a/s/q]: ").strip().lower()
        if choice == "q":
            break
        if choice == "s":
            continue
        if choice == "c":
            rec["final_label"] = rec["heuristic_label"]
        elif choice == "i":
            rec["final_label"] = "incorrect"
        elif choice == "a":
            rec["final_label"] = "ambiguous"
        else:
            print("  (skipped — invalid input)")
            continue

        evidence = input("  Evidence note (Enter to keep existing): ").strip()
        rec["adjudication_type"] = "human_review"
        rec["adjudicator"] = reviewer
        rec["adjudication_date"] = today
        if evidence:
            rec["adjudication_evidence"] = evidence
        reviewed += 1

    if reviewed > 0:
        write_gold_set(records, gold_path)
        summary = build_summary(records, str(gold_path))
        write_summary_json(summary, gold_path.with_name("gold_set_summary.json"))
        print(f"\n{reviewed} records reviewed and saved.")

    return 0


def _apply_curatorial(records: list[dict], gold_path: Path) -> int:
    """Apply versionned curatorial decisions from _curatorial_decisions.py.

    Decisions are keyed by intrinsic record fingerprint (match_id or
    donor_identity_key|entity_name), not by case_id, so they survive
    gold set regeneration even if case_id assignment order changes.
    """
    from datetime import datetime, timezone

    from gold_set._curatorial_decisions import (
        ADJUDICATION_TYPE,
        ADJUDICATOR,
        DECISIONS,
        record_fingerprint,
    )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    applied = 0

    for rec in records:
        if rec.get("final_label") is not None:
            continue
        fp = record_fingerprint(rec)
        if fp in DECISIONS:
            final_label, justification = DECISIONS[fp]
            rec["final_label"] = final_label
            rec["adjudication_type"] = ADJUDICATION_TYPE
            rec["adjudicator"] = ADJUDICATOR
            rec["adjudication_date"] = today
            rec["adjudication_evidence"] = justification
            applied += 1

    if applied > 0:
        write_gold_set(records, gold_path)
        summary = build_summary(records, str(gold_path))
        write_summary_json(summary, gold_path.with_name("gold_set_summary.json"))

    pending = sum(1 for r in records if r.get("final_label") is None)
    print(f"Curatorial decisions applied: {applied}")
    print(f"Remaining pending: {pending}")
    return 0


def _cmd_summary(args: argparse.Namespace) -> int:
    """Show summary of existing gold set."""
    analytics_dir = Path(args.analytics_dir)
    gold_path = analytics_dir / "gold_set_matches.jsonl"

    if not gold_path.exists():
        print("No gold set found.", file=sys.stderr)
        return 1

    records: list[dict] = []
    with open(gold_path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    summary = build_summary(records, str(gold_path))
    print_summary(summary)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gold set pipeline for Atlas STF")
    parser.add_argument("--analytics-dir", type=str, default="data/analytics", help="Analytics directory")
    sub = parser.add_subparsers(dest="command")

    gen = sub.add_parser("generate", help="Generate gold set from production data")
    gen.add_argument("--output", type=str, default=None, help="Output path")

    rev = sub.add_parser("review", help="Review pending records (interactive or curatorial batch)")
    rev.add_argument("--reviewer", type=str, default=None, help="Reviewer name (interactive mode)")
    rev.add_argument(
        "--apply-curatorial",
        action="store_true",
        help="Apply versionned curatorial decisions from _curatorial_decisions.py",
    )

    sub.add_parser("summary", help="Show summary of existing gold set")

    args = parser.parse_args(argv)

    if args.command == "generate":
        return _cmd_generate(args)
    if args.command == "review":
        return _cmd_review(args)
    if args.command == "summary":
        return _cmd_summary(args)

    # Default: generate
    args.output = None
    return _cmd_generate(args)


if __name__ == "__main__":
    sys.exit(main())
