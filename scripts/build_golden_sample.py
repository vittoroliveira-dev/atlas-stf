#!/usr/bin/env python3
"""Build golden sample for deterministic audit reproduction.

Extracts a small, diverse, deterministic subset of curated data
that exercises the critical pipeline path:
  groups → baseline → alerts → counsel_network → serving → API

Selection: 5 process_ids per major class (8 classes = 40 processes),
plus all related decision_events, parties, counsel, links.

Output: audit/samples/critical_core/ with manifest.json
"""

from __future__ import annotations

import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CURATED_DIR = REPO_ROOT / "data" / "curated"
SAMPLE_DIR = REPO_ROOT / "audit" / "samples" / "critical_core"

# Deterministic selection: 5 process_ids per class, 8 classes
TARGET_CLASSES = ["ADI", "RE", "ADPF", "ADC", "ACO", "AP", "Ext", "SS"]
PIDS_PER_CLASS = 50


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def _write_jsonl(path: Path, records: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return len(records)


def build_sample() -> dict:
    """Build golden sample. Returns manifest dict."""
    if not CURATED_DIR.exists():
        print("ERROR: curated dir not found")
        sys.exit(1)

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    curated_out = SAMPLE_DIR / "curated"
    curated_out.mkdir(exist_ok=True)

    # Step 1: Select process_ids deterministically
    selected_pids: set[str] = set()
    by_class: dict[str, list[str]] = defaultdict(list)

    for r in _read_jsonl(CURATED_DIR / "process.jsonl"):
        pc = r.get("process_class", "")
        pid = r.get("process_id", "")
        if pc in TARGET_CLASSES and len(by_class[pc]) < PIDS_PER_CLASS:
            by_class[pc].append(pid)
            selected_pids.add(pid)

    # Step 2: Extract related records
    stats: dict[str, int] = {}

    # Processes
    processes = [r for r in _read_jsonl(CURATED_DIR / "process.jsonl") if r.get("process_id") in selected_pids]
    stats["process.jsonl"] = _write_jsonl(curated_out / "process.jsonl", processes)

    # Decision events
    events = [r for r in _read_jsonl(CURATED_DIR / "decision_event.jsonl") if r.get("process_id") in selected_pids]
    stats["decision_event.jsonl"] = _write_jsonl(curated_out / "decision_event.jsonl", events)

    # Party links → party_ids
    party_links = [r for r in _read_jsonl(CURATED_DIR / "process_party_link.jsonl") if r.get("process_id") in selected_pids]
    party_ids = {r.get("party_id") for r in party_links if r.get("party_id")}
    stats["process_party_link.jsonl"] = _write_jsonl(curated_out / "process_party_link.jsonl", party_links)

    # Parties
    parties = [r for r in _read_jsonl(CURATED_DIR / "party.jsonl") if r.get("party_id") in party_ids]
    stats["party.jsonl"] = _write_jsonl(curated_out / "party.jsonl", parties)

    # Counsel links → counsel_ids
    counsel_links = [r for r in _read_jsonl(CURATED_DIR / "process_counsel_link.jsonl") if r.get("process_id") in selected_pids]
    counsel_ids = {r.get("counsel_id") for r in counsel_links if r.get("counsel_id")}
    stats["process_counsel_link.jsonl"] = _write_jsonl(curated_out / "process_counsel_link.jsonl", counsel_links)

    # Counsel
    counsel = [r for r in _read_jsonl(CURATED_DIR / "counsel.jsonl") if r.get("counsel_id") in counsel_ids]
    stats["counsel.jsonl"] = _write_jsonl(curated_out / "counsel.jsonl", counsel)

    # Other curated files: copy first few lines or empty
    # Process-linked files: filter by selected_pids + dedup by primary key
    process_linked = {
        "movement.jsonl": "movement_id",
        "session_event.jsonl": "session_event_id",
        "source_evidence.jsonl": "evidence_id",
    }
    for name, pk in process_linked.items():
        src = CURATED_DIR / name
        if src.exists():
            seen: set[str] = set()
            records = []
            for r in _read_jsonl(src):
                if r.get("process_id") in selected_pids:
                    key = r.get(pk, "")
                    if key and key not in seen:
                        seen.add(key)
                        records.append(r)
            stats[name] = _write_jsonl(curated_out / name, records)
        else:
            stats[name] = 0

    # Non-process-linked files: take first 100 deterministically (no dups)
    for other in ["subject.jsonl", "entity_identifier.jsonl",
                   "entity_identifier_reconciliation.jsonl",
                   "law_firm_entity.jsonl", "lawyer_entity.jsonl",
                   "representation_edge.jsonl", "representation_event.jsonl",
                   "agenda_event.jsonl", "agenda_coverage.jsonl"]:
        src = CURATED_DIR / other
        if src.exists():
            records = []
            seen_ids: set[str] = set()
            for r in _read_jsonl(src):
                # Use first available ID field for dedup
                rid = r.get("entity_id") or r.get("subject_id") or r.get("event_id") or r.get("representation_id") or str(len(records))
                if rid not in seen_ids and len(records) < 100:
                    seen_ids.add(rid)
                    records.append(r)
            stats[other] = _write_jsonl(curated_out / other, records)
        else:
            stats[other] = 0

    # Copy JSON files
    import shutil

    for json_file in CURATED_DIR.glob("*.json"):
        shutil.copy2(json_file, curated_out / json_file.name)

    # Step 3: Generate manifest
    hashes: dict[str, str] = {}
    for f in sorted(curated_out.iterdir()):
        if f.is_file():
            hashes[f.name] = _sha256(f)

    manifest = {
        "name": "critical_core",
        "description": "Minimal diverse sample for critical pipeline path validation",
        "origin": "deterministic selection from data/curated/",
        "selection_criteria": {
            "classes": TARGET_CLASSES,
            "pids_per_class": PIDS_PER_CLASS,
            "total_processes": len(selected_pids),
        },
        "file_stats": stats,
        "file_hashes": hashes,
        "builders_exercised": [
            "build_groups", "build_baseline", "build_alerts",
            "counsel_network", "rapporteur_profile", "assignment_audit",
        ],
        "endpoints_validated": [
            "/health", "/dashboard", "/alerts", "/graph/search", "/graph/metrics",
        ],
        "assertions": {
            "build_groups": {"min_groups": 1, "min_links": 1},
            "build_baseline": {"min_baselines": 1},
            "build_alerts": {"min_alerts": 1},
            "counsel_network": {"min_clusters": 0, "required_fields": ["baseline_rate"]},
            "serving_build": {"min_tables": 30},
            "api_smoke": {"min_ok_endpoints": 3},
        },
        "fallback_limits": {
            "baseline_rate_default_pct": 20.0,
        },
        "blocking_policy": "critical",
    }

    manifest_path = SAMPLE_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    return manifest


def main() -> int:
    print("Building golden sample: critical_core")
    manifest = build_sample()
    stats = manifest["file_stats"]
    total = sum(stats.values())
    print(f"  Processes: {manifest['selection_criteria']['total_processes']}")
    print(f"  Classes: {manifest['selection_criteria']['classes']}")
    print(f"  Total records: {total}")
    for name, count in sorted(stats.items()):
        if count > 0:
            print(f"    {name}: {count}")
    print(f"  Manifest: {SAMPLE_DIR / 'manifest.json'}")
    print("Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
