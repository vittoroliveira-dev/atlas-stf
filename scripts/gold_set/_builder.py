"""Gold set generation: read production data, classify, sample, label, adjudicate."""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

from ._analysis import (
    classify_counsel,
    classify_donation_party,
    classify_sanction,
    classify_scl,
    sample_stratum,
)
from ._constants import ALL_STRATA, REQUIRED_STRATA
from ._labeling import LABEL_FNS
from ._schema import build_record, validate_record


def _read_jsonl_buckets(
    path: Path,
    classifier: object,
    *,
    entity_filter: str | None = None,
) -> dict[str, list[dict]]:
    """Read a JSONL file and classify records into stratum buckets."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    if not path.exists():
        return buckets
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if entity_filter and rec.get("entity_type") != entity_filter:
                continue
            stratum = classifier(rec)  # type: ignore[operator]
            if stratum:
                buckets[stratum].append(rec)
    return buckets


_SOURCE_MAP = {
    "donation_party": "donation_match.jsonl",
    "donation_ambiguous": "donation_match_ambiguous.jsonl",
    "counsel": "donation_match.jsonl",
    "sanction": "sanction_match.jsonl",
    "scl": "sanction_corporate_link.jsonl",
}


def generate_gold_set(*, analytics_dir: Path) -> tuple[list[dict], dict[str, int]]:
    """Generate gold set records from production data.

    Returns (records, population_counts) where population_counts shows
    how many source records exist per stratum before sampling.
    """
    donation_path = analytics_dir / "donation_match.jsonl"
    ambiguous_path = analytics_dir / "donation_match_ambiguous.jsonl"
    sanction_path = analytics_dir / "sanction_match.jsonl"
    scl_path = analytics_dir / "sanction_corporate_link.jsonl"

    # Classify all source records into stratum buckets
    buckets: dict[str, list[dict]] = defaultdict(list)

    for stratum, recs in _read_jsonl_buckets(donation_path, classify_donation_party, entity_filter="party").items():
        buckets[stratum].extend(recs)

    for stratum, recs in _read_jsonl_buckets(donation_path, classify_counsel, entity_filter="counsel").items():
        buckets[stratum].extend(recs)

    for stratum, recs in _read_jsonl_buckets(ambiguous_path, lambda r: "ambiguous_multi").items():
        buckets[stratum].extend(recs)

    for stratum, recs in _read_jsonl_buckets(sanction_path, classify_sanction).items():
        buckets[stratum].extend(recs)

    for stratum, recs in _read_jsonl_buckets(scl_path, classify_scl).items():
        buckets[stratum].extend(recs)

    population: dict[str, int] = {s: len(recs) for s, recs in buckets.items()}

    # Sample, label, and build records
    all_records: list[dict] = []
    case_counter = 1

    for sdef in ALL_STRATA:
        records = buckets.get(sdef.name, [])
        if not records:
            print(f"  SKIP {sdef.name}: 0 records in source data", file=sys.stderr)
            continue

        sampled = sample_stratum(records, sdef.target)
        label_fn = LABEL_FNS[sdef.name]
        source_file = _SOURCE_MAP[sdef.source_type]

        for rec in sampled:
            heuristic_label, justification, heuristic_basis = label_fn(rec)  # type: ignore[operator]
            case_id = f"gs-{case_counter:04d}"

            gold = build_record(
                rec=rec,
                stratum=sdef.name,
                case_id=case_id,
                heuristic_label=heuristic_label,
                heuristic_basis=heuristic_basis,
                justification=justification,
                adjudication_type=sdef.adjudication_default,
                source_file=source_file,
            )

            errors = validate_record(gold)
            if errors:
                print(f"  WARN {case_id}: {errors}", file=sys.stderr)

            all_records.append(gold)
            case_counter += 1

    return all_records, population


def check_required_strata(records: list[dict]) -> list[str]:
    """Check that all required strata have at least one record. Returns violations."""
    present = {r["stratum"] for r in records}
    missing = REQUIRED_STRATA - present
    return [f"Required stratum '{s}' has zero records" for s in sorted(missing)]


def write_gold_set(records: list[dict], path: Path) -> None:
    """Write gold set JSONL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def adjudicated_count(records: list[dict]) -> int:
    """Count records with a final_label set."""
    return sum(1 for r in records if r.get("final_label") is not None)


def apply_human_review(records: list[dict], reviews: dict[str, dict]) -> int:
    """Apply human review decisions to records.

    reviews: {case_id: {"final_label": "...", "adjudicator": "...", "evidence": "..."}}
    Returns count of applied reviews.
    """
    applied = 0
    for rec in records:
        cid = rec["case_id"]
        if cid in reviews:
            rev = reviews[cid]
            rec["final_label"] = rev["final_label"]
            rec["adjudication_type"] = "human_review"
            rec["adjudicator"] = rev.get("adjudicator", "unknown")
            rec["adjudication_evidence"] = rev.get("evidence", rec.get("adjudication_evidence", ""))
            rec["adjudication_date"] = rev.get("date")
            applied += 1
    return applied
