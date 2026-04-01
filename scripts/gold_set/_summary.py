"""Summary generation and reporting for the gold set."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ._constants import MINIMUM_GOLD_SET_SIZE, SEED


@dataclass
class GoldSetSummary:
    total: int
    adjudicated: int
    by_stratum: dict[str, int]
    by_heuristic_label: dict[str, int]
    by_final_label: dict[str, int]
    by_adjudication_type: dict[str, int]
    by_strategy: dict[str, int]
    by_source: dict[str, int]
    output_path: str
    generated_at: str


def build_summary(records: list[dict], output_path: str) -> GoldSetSummary:
    by_stratum: Counter[str] = Counter()
    by_heuristic: Counter[str] = Counter()
    by_final: Counter[str] = Counter()
    by_adj: Counter[str] = Counter()
    by_strategy: Counter[str] = Counter()
    by_source: Counter[str] = Counter()
    adjudicated = 0

    for rec in records:
        by_stratum[rec.get("stratum", "?")] += 1
        by_heuristic[rec.get("heuristic_label", "?")] += 1
        fl = rec.get("final_label")
        if fl:
            by_final[fl] += 1
            adjudicated += 1
        else:
            by_final["(pending)"] += 1
        by_adj[rec.get("adjudication_type", "?")] += 1
        by_strategy[rec.get("match_strategy", "?")] += 1
        by_source[rec.get("source", "?")] += 1

    return GoldSetSummary(
        total=len(records),
        adjudicated=adjudicated,
        by_stratum=dict(sorted(by_stratum.items())),
        by_heuristic_label=dict(sorted(by_heuristic.items())),
        by_final_label=dict(sorted(by_final.items())),
        by_adjudication_type=dict(sorted(by_adj.items())),
        by_strategy=dict(sorted(by_strategy.items())),
        by_source=dict(sorted(by_source.items())),
        output_path=output_path,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def write_summary_json(summary: GoldSetSummary, path: Path) -> None:
    data = {
        "total": summary.total,
        "adjudicated": summary.adjudicated,
        "minimum_required": MINIMUM_GOLD_SET_SIZE,
        "by_stratum": summary.by_stratum,
        "by_heuristic_label": summary.by_heuristic_label,
        "by_final_label": summary.by_final_label,
        "by_adjudication_type": summary.by_adjudication_type,
        "by_strategy": summary.by_strategy,
        "by_source": summary.by_source,
        "generated_at": summary.generated_at,
        "seed": SEED,
        "script": "scripts/build_gold_set.py",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def print_summary(summary: GoldSetSummary) -> None:
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"Gold Set — {summary.generated_at[:19]}")
    print(sep)
    print(f"\nTotal: {summary.total} records (adjudicated: {summary.adjudicated}, minimum: {MINIMUM_GOLD_SET_SIZE})")
    print(f"Output: {summary.output_path}\n")

    print("-- By stratum --")
    for s, n in sorted(summary.by_stratum.items()):
        print(f"  {s:<30s} {n:>4d}")

    print("\n-- By adjudication type --")
    for a, n in sorted(summary.by_adjudication_type.items()):
        pct = n / summary.total * 100 if summary.total else 0
        print(f"  {a:<30s} {n:>4d} ({pct:5.1f}%)")

    print("\n-- By heuristic label --")
    for label, count in sorted(summary.by_heuristic_label.items()):
        print(f"  {label:<20s} {count:>4d}")

    print("\n-- By final label --")
    for label, count in sorted(summary.by_final_label.items()):
        print(f"  {label:<20s} {count:>4d}")

    print("\n-- By source --")
    for s, n in sorted(summary.by_source.items()):
        print(f"  {s:<40s} {n:>4d}")

    print(f"\n{sep}\n")
