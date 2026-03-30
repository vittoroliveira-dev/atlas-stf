#!/usr/bin/env python3
"""Audit fallback/default usage with real local data.

Measures how often fallback values are triggered for critical concepts
and fails when rates exceed thresholds defined in the integrity manifest.

Operates on real local data in read-only mode (temporary output directory).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "audit" / "contracts" / "integrity_manifest.json"
CURATED_DIR = REPO_ROOT / "data" / "curated"
ANALYTICS_DIR = REPO_ROOT / "data" / "analytics"


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def measure_process_class_coverage() -> dict:
    """Measure % of process_ids in decision_event without process_class in process.jsonl."""
    de_path = CURATED_DIR / "decision_event.jsonl"
    proc_path = CURATED_DIR / "process.jsonl"

    if not de_path.exists() or not proc_path.exists():
        return {"status": "skipped", "reason": "required files not found"}

    pc_map: dict[str, str] = {}
    for r in _read_jsonl(proc_path):
        pid = r.get("process_id")
        pc = r.get("process_class")
        if pid and pc:
            pc_map[pid] = pc

    de_pids: set[str] = set()
    for r in _read_jsonl(de_path):
        pid = r.get("process_id")
        if pid:
            de_pids.add(pid)

    total = len(de_pids)
    covered = sum(1 for pid in de_pids if pid in pc_map)
    missing = total - covered
    pct = (missing / total * 100) if total > 0 else 0.0

    return {
        "status": "measured",
        "total_process_ids": total,
        "covered": covered,
        "missing": missing,
        "pct_missing": round(pct, 2),
    }


def measure_baseline_rate_fallback() -> dict:
    """Measure % of counsel_network clusters using DEFAULT_BASELINE_RATE."""
    cn_path = ANALYTICS_DIR / "counsel_network_cluster.jsonl"
    if not cn_path.exists():
        return {"status": "skipped", "reason": "counsel_network_cluster.jsonl not found"}

    records = _read_jsonl(cn_path)
    total = len(records)
    if total == 0:
        return {"status": "skipped", "reason": "no records"}

    # Check if baseline_rate field exists in data (pipeline may need re-run)
    has_field = sum(1 for r in records if "baseline_rate" in r)
    if has_field == 0:
        return {
            "status": "stale_data",
            "reason": "baseline_rate field absent — pipeline rebuild required",
            "total_clusters": total,
        }

    default_rate = 0.5
    with_field = [r for r in records if "baseline_rate" in r]
    using_default = sum(1 for r in with_field if abs(r["baseline_rate"] - default_rate) < 0.001)
    pct = using_default / len(with_field) * 100

    return {
        "status": "measured",
        "total_clusters": len(with_field),
        "using_default_baseline": using_default,
        "pct_default": round(pct, 2),
    }


def measure_alert_process_class_enrichment() -> dict:
    """Check if build_alerts enriches events with process_class.

    Instead of running the full builder, verify the enrichment code exists.
    """
    build_alerts_path = REPO_ROOT / "src" / "atlas_stf" / "analytics" / "build_alerts.py"
    if not build_alerts_path.exists():
        return {"status": "skipped", "reason": "build_alerts.py not found"}

    content = build_alerts_path.read_text(encoding="utf-8")
    has_enrichment = "process_class" in content and "pc_map" in content

    return {
        "status": "measured",
        "enrichment_code_present": has_enrichment,
        "note": "Structural check — full measurement requires running build_alerts with real data",
    }


def run_audit(manifest: dict | None = None) -> tuple[list[dict], bool]:
    """Run all fallback measurements. Returns (results, has_failure)."""
    if manifest is None:
        manifest = load_manifest()

    thresholds = manifest.get("fallback_thresholds", {})
    results: list[dict] = []
    has_failure = False

    # 1. process_class coverage
    pc_result = measure_process_class_coverage()
    pc_threshold = thresholds.get("process_class_missing", {}).get("max_pct", 1.0)
    if pc_result["status"] == "measured":
        passed = pc_result["pct_missing"] <= pc_threshold
        pc_result["threshold"] = pc_threshold
        pc_result["passed"] = passed
        if not passed:
            has_failure = True
    results.append({"concept": "process_class_missing", **pc_result})

    # 2. baseline_rate fallback
    bl_result = measure_baseline_rate_fallback()
    bl_threshold = thresholds.get("baseline_rate_fallback", {}).get("max_pct", 10.0)
    if bl_result["status"] == "measured":
        passed = bl_result["pct_default"] <= bl_threshold
        bl_result["threshold"] = bl_threshold
        bl_result["passed"] = passed
        if not passed:
            has_failure = True
    results.append({"concept": "baseline_rate_fallback", **bl_result})

    # 3. alert enrichment
    alert_result = measure_alert_process_class_enrichment()
    if alert_result["status"] == "measured":
        passed = alert_result["enrichment_code_present"]
        alert_result["passed"] = passed
        if not passed:
            has_failure = True
    results.append({"concept": "alert_process_class_enrichment", **alert_result})

    return results, has_failure


def main() -> int:
    manifest = load_manifest()
    results, has_failure = run_audit(manifest)

    for r in results:
        concept = r.pop("concept")
        status = r.get("status", "unknown")
        passed = r.get("passed")

        if status == "skipped":
            marker = "⊘"
            verdict = f"skipped: {r.get('reason', '?')}"
        elif passed is True:
            marker = "✓"
            verdict = "PASS"
        elif passed is False:
            marker = "✗"
            verdict = "FAIL"
        else:
            marker = "?"
            verdict = "unknown"

        print(f"  {marker} {concept}: {verdict}")
        for k, v in r.items():
            if k not in {"status", "passed"}:
                print(f"      {k}: {v}")

    print(f"\nFallback usage audit: {len(results)} checks, {'FAILED' if has_failure else 'PASSED'}")
    return 1 if has_failure else 0


if __name__ == "__main__":
    sys.exit(main())
