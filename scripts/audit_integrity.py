#!/usr/bin/env python3
"""Atlas STF Integrity Audit — Orchestrator.

Runs all integrity auditors and produces a consolidated report.
Fail-closed: exits with code 1 on any critical violation.

Usage:
    python scripts/audit_integrity.py              # full audit
    python scripts/audit_integrity.py --quick      # static checks only (fast)
    python scripts/audit_integrity.py --concept X  # audit specific concept
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "audit" / "contracts" / "integrity_manifest.json"


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Atlas STF Integrity Audit")
    parser.add_argument("--quick", action="store_true", help="Static checks only (no real data)")
    parser.add_argument("--runtime", action="store_true", help="Run builder runtime + end-to-end on sample")
    parser.add_argument("--concept", type=str, help="Audit specific concept only")
    parser.add_argument("--frontend-only", action="store_true", help="Run only frontend↔API coverage")
    args = parser.parse_args()

    manifest = load_manifest()
    start = time.monotonic()
    has_critical = False
    total_checks = 0
    total_violations = 0

    print("=" * 70)
    print("Atlas STF — Integrity Audit")
    print(f"Manifest version: {manifest.get('version', '?')}")
    print("=" * 70)

    # 1. Canonical sources (always runs)
    if not args.frontend_only:
        print("\n─── 1/4: Canonical Source Audit ───")
        from audit_canonical_sources import run_audit as audit_canonical

        violations = audit_canonical(manifest)
        total_checks += 1
        if violations:
            total_violations += len(violations)
            for v in sorted(violations, key=lambda x: (x.severity, x.file)):
                marker = "✗" if v.severity == "critical" else "⚠"
                print(f"  {marker} [{v.severity}] {v.file}:{v.line} — {v.message}")
                if v.severity == "critical":
                    has_critical = True
        else:
            print("  ✓ No violations")

    # 2. Field propagation (always runs)
    if not args.frontend_only:
        print("\n─── 2/4: Field Propagation Audit ───")
        from audit_field_propagation import run_audit as audit_propagation

        results = audit_propagation(manifest)
        total_checks += 1
        for r in results:
            if r.missing:
                marker = "✗"
                if r.severity == "critical":
                    has_critical = True
                total_violations += 1
            else:
                marker = "✓"
            print(f"  {marker} {r.concept}: {'BROKEN — missing: ' + ', '.join(r.missing) if r.missing else 'OK'}")

    # 3. Fallback usage (only in full mode)
    if not args.quick and not args.frontend_only:
        print("\n─── 3/4: Fallback Usage Audit (real data) ───")
        from audit_fallback_usage import run_audit as audit_fallback

        fallback_results, fb_failure = audit_fallback(manifest)
        total_checks += 1
        for r in fallback_results:
            concept = r.get("concept", "?")
            passed = r.get("passed")
            status = r.get("status", "unknown")
            # Resolve stale_data policy from fallback_thresholds or defaults
            threshold_def = manifest.get("fallback_thresholds", {}).get(concept, {})
            stale_policy = threshold_def.get(
                "stale_data_policy",
                manifest.get("defaults", {}).get("stale_data_policy_default", "block"),
            )

            if status == "skipped":
                print(f"  ⊘ {concept}: skipped ({r.get('reason', '?')})")
            elif status == "stale_data":
                if stale_policy == "block":
                    print(f"  ✗ {concept}: BLOCKED — stale data, proof required — {r.get('reason', '?')}")
                    total_violations += 1
                    has_critical = True
                else:
                    print(f"  ⚠ {concept}: stale data (warn) — {r.get('reason', '?')}")
            elif passed:
                pct = r.get("pct_missing") or r.get("pct_default") or "?"
                print(f"  ✓ {concept}: {pct}% (threshold: {r.get('threshold', '?')}%)")
            else:
                total_violations += 1
                if True:  # fallback failures are always critical
                    has_critical = True
                print(f"  ✗ {concept}: FAIL")

    # 4. Pipeline contracts (only in full or runtime mode)
    if not args.quick and not args.frontend_only:
        print("\n─── 4/5: Pipeline Contracts ───")
        from audit_pipeline_contracts import run_audit as audit_pipeline

        pipeline_results, pipe_failure = audit_pipeline()
        total_checks += 1
        for r in pipeline_results:
            marker = "✓" if r["status"] == "OK" else "✗"
            print(f"  {marker} [{r['severity']}] {r['stage']}: {r['status']}")
        if pipe_failure:
            total_violations += 1
            has_critical = True

    # 5. Builder runtime + E2E (only in runtime/full mode)
    if args.runtime and not args.frontend_only:
        print("\n─── 5a/7: Builder Runtime (sampled curated) ───")
        from audit_builder_runtime import run_audit as audit_builders

        builder_results, builder_failure = audit_builders()
        total_checks += 1
        for r in builder_results:
            marker = "✓" if r["status"] == "OK" else "✗"
            outputs = ", ".join(r.get("outputs", []))
            print(f"  {marker} [{r['severity']}] {r['name']}: {r['status']} — {outputs or r.get('reason', '')}")
        if builder_failure:
            total_violations += 1
            has_critical = True

        print("\n─── 5b/7: End-to-End Sample (analytics→serving→API) ───")
        from audit_end_to_end_sample import run_end_to_end

        e2e_results, e2e_failure = run_end_to_end()
        total_checks += 1
        for r in e2e_results:
            marker = "✓" if r["status"] == "OK" else "✗" if r["status"] == "FAILED" else "◐"
            print(f"  {marker} {r['step']}: {r['status']} ({r.get('elapsed', 0)}s) — {r.get('detail', '')}")
            for sub in r.get("sub_results", []):
                print(f"      {sub}")
        if e2e_failure:
            total_violations += 1
            has_critical = True

    # 6. API smoke tests against canonical DB (only in full mode)
    if not args.quick and not args.frontend_only:
        print("\n─── 5/6: API Smoke Tests (real serving DB) ───")
        from audit_api_smoke import run_smoke_tests

        smoke_results, smoke_failure = run_smoke_tests()
        total_checks += 1
        for r in smoke_results:
            marker = "✓" if r["status"] == "OK" else "✗" if r["status"] in ("FAIL", "ERROR") else "⊘"
            line = f"  {marker} [{r.get('severity', '?')}] {r['endpoint']}: {r['status']}"
            if "reason" in r:
                line += f" — {r['reason'][:80]}"
            print(line)
        if smoke_failure:
            total_violations += 1
            has_critical = True

    # 6. Frontend↔API coverage (always runs)
    step_label = "6/6" if not args.quick else "3/3"
    print(f"\n─── {step_label}: Frontend↔API Coverage ───")
    from audit_frontend_api_coverage import run_audit as audit_frontend

    coverage_results, cov_failure = audit_frontend(manifest)
    total_checks += 1
    current_group = ""
    for r in coverage_results:
        if r.group != current_group:
            print(f"  [{r.group}]")
            current_group = r.group
        markers = {"ok": "✓", "indirect": "≈", "fetcher_only": "◐", "not_required": "⊘", "missing": "✗"}
        marker = markers.get(r.status, "?")
        print(f"    {marker} {r.method:4s} {r.path:40s} → {r.status}")
    if cov_failure:
        total_violations += 1
        has_critical = True

    # Summary
    elapsed = time.monotonic() - start
    print("\n" + "=" * 70)
    print(f"Integrity Audit Complete — {elapsed:.1f}s")
    print(f"  Checks: {total_checks}")
    print(f"  Violations: {total_violations}")
    print(f"  Critical: {'YES' if has_critical else 'NO'}")
    print(f"  Verdict: {'BLOCKED' if has_critical else 'PASSED'}")
    print("=" * 70)

    return 1 if has_critical else 0


if __name__ == "__main__":
    sys.exit(main())
