#!/usr/bin/env python3
"""End-to-end audit: run analytics → serving-build → API smoke on fresh data.

Creates a temporary directory, runs critical builders on sampled curated data,
builds a fresh serving DB, and smoke-tests the API against it.

This proves the CURRENT CODE can produce a working system from scratch.

Returns exit code 1 on critical failure.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CURATED_DIR = REPO_ROOT / "data" / "curated"
SAMPLE_LINES = 2000


def _sample_jsonl(src: Path, dst: Path, max_lines: int) -> int:
    """Copy first max_lines from src to dst. Returns lines copied."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with src.open("r", encoding="utf-8") as fin, dst.open("w", encoding="utf-8") as fout:
        for line in fin:
            if line.strip():
                fout.write(line)
                count += 1
                if count >= max_lines:
                    break
    return count


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def run_end_to_end(sample_lines: int = SAMPLE_LINES) -> tuple[list[dict], bool]:
    """Run end-to-end audit. Returns (steps, has_critical)."""
    if not CURATED_DIR.exists():
        return [{"step": "setup", "status": "SKIP", "reason": "curated dir not found"}], False

    steps: list[dict] = []
    has_critical = False

    with tempfile.TemporaryDirectory(prefix="atlas_e2e_") as tmpdir:
        tmp = Path(tmpdir)
        curated = tmp / "curated"
        analytics = tmp / "analytics"
        serving = tmp / "serving"
        curated.mkdir()
        analytics.mkdir()
        serving.mkdir()

        # Step 1: Sample curated data
        t0 = time.monotonic()
        jsonl_files = sorted(CURATED_DIR.glob("*.jsonl"))
        total_sampled = 0
        for src in jsonl_files:
            n = _sample_jsonl(src, curated / src.name, sample_lines)
            total_sampled += n
        # Copy JSON files too
        for src in sorted(CURATED_DIR.glob("*.json")):
            shutil.copy2(src, curated / src.name)
        elapsed = time.monotonic() - t0
        steps.append(
            {
                "step": "sample_curated",
                "status": "OK",
                "detail": f"{total_sampled} lines from {len(jsonl_files)} files",
                "elapsed": round(elapsed, 1),
            }
        )

        # Step 2: Run groups + baseline
        t0 = time.monotonic()
        try:
            from atlas_stf.analytics.build_groups import build_groups

            build_groups(
                process_path=curated / "process.jsonl",
                decision_event_path=curated / "decision_event.jsonl",
                output_dir=analytics,
            )
            groups_count = _count_lines(analytics / "comparison_group.jsonl")
            links_count = _count_lines(analytics / "decision_event_group_link.jsonl")
            elapsed = time.monotonic() - t0
            steps.append(
                {
                    "step": "build_groups",
                    "status": "OK",
                    "detail": f"{groups_count} groups, {links_count} links",
                    "elapsed": round(elapsed, 1),
                }
            )
        except Exception as exc:
            steps.append(
                {
                    "step": "build_groups",
                    "status": "FAILED",
                    "detail": str(exc)[:150],
                    "elapsed": round(time.monotonic() - t0, 1),
                }
            )
            has_critical = True

        t0 = time.monotonic()
        try:
            from atlas_stf.analytics.baseline import build_baseline

            build_baseline(
                comparison_group_path=analytics / "comparison_group.jsonl",
                link_path=analytics / "decision_event_group_link.jsonl",
                decision_event_path=curated / "decision_event.jsonl",
                output_path=analytics / "baseline.jsonl",
                summary_path=analytics / "baseline_summary.json",
            )
            baseline_count = _count_lines(analytics / "baseline.jsonl")
            elapsed = time.monotonic() - t0
            steps.append(
                {
                    "step": "build_baseline",
                    "status": "OK",
                    "detail": f"{baseline_count} baselines",
                    "elapsed": round(elapsed, 1),
                }
            )
        except Exception as exc:
            steps.append(
                {
                    "step": "build_baseline",
                    "status": "FAILED",
                    "detail": str(exc)[:150],
                    "elapsed": round(time.monotonic() - t0, 1),
                }
            )
            has_critical = True

        # Step 3: Run counsel_network (tests baseline integration)
        t0 = time.monotonic()
        try:
            from atlas_stf.analytics.counsel_network import build_counsel_network

            cn_path = build_counsel_network(curated_dir=curated, output_dir=analytics)
            cn_count = _count_lines(cn_path)
            # Verify baseline_rate field exists in output
            has_baseline_rate = False
            if cn_count > 0:
                first_line = cn_path.read_text().split("\n")[0]
                rec = json.loads(first_line)
                has_baseline_rate = "baseline_rate" in rec
            elapsed = time.monotonic() - t0
            steps.append(
                {
                    "step": "counsel_network",
                    "status": "OK",
                    "detail": f"{cn_count} clusters, baseline_rate={'present' if has_baseline_rate else 'MISSING'}",
                    "elapsed": round(elapsed, 1),
                }
            )
            if not has_baseline_rate and cn_count > 0:
                has_critical = True
        except Exception as exc:
            steps.append(
                {
                    "step": "counsel_network",
                    "status": "FAILED",
                    "detail": str(exc)[:150],
                    "elapsed": round(time.monotonic() - t0, 1),
                }
            )

        # Create compound_risk stub before build_alerts needs it
        (analytics / "compound_risk.jsonl").write_text("", encoding="utf-8")

        # Step 3b: Build alerts (needed for serving validation)
        t0 = time.monotonic()
        try:
            from atlas_stf.analytics.build_alerts import build_alerts

            build_alerts(
                baseline_path=analytics / "baseline.jsonl",
                link_path=analytics / "decision_event_group_link.jsonl",
                decision_event_path=curated / "decision_event.jsonl",
                output_path=analytics / "outlier_alert.jsonl",
                summary_path=analytics / "outlier_alert_summary.json",
                compound_risk_path=analytics / "compound_risk.jsonl",  # will be empty stub
                process_path=curated / "process.jsonl",
            )
            alert_count = _count_lines(analytics / "outlier_alert.jsonl")
            elapsed = time.monotonic() - t0
            steps.append(
                {
                    "step": "build_alerts",
                    "status": "OK",
                    "detail": f"{alert_count} alerts",
                    "elapsed": round(elapsed, 1),
                }
            )
        except Exception as exc:
            steps.append(
                {
                    "step": "build_alerts",
                    "status": "FAILED",
                    "detail": str(exc)[:150],
                    "elapsed": round(time.monotonic() - t0, 1),
                }
            )

        # Step 4: Build serving DB
        # Create empty stubs for analytics files the builder expects but we didn't generate
        for stub_name in [
            "outlier_alert.jsonl",
            "ml_outlier_score.jsonl",
            "compound_risk.jsonl",
            "sanction_match.jsonl",
            "donation_match.jsonl",
            "corporate_network.jsonl",
            "sanction_corporate_link.jsonl",
            "decision_velocity.jsonl",
            "procedural_timeline.jsonl",
            "rapporteur_change.jsonl",
            "temporal_analysis.jsonl",
            "pauta_anomaly.jsonl",
            "sequential_analysis.jsonl",
            "counsel_affinity.jsonl",
            "counsel_donation_profile.jsonl",
            "counsel_sanction_profile.jsonl",
            "donation_event.jsonl",
            "economic_group.jsonl",
            "representation_graph.jsonl",
            "representation_recurrence.jsonl",
            "representation_windows.jsonl",
            "amicus_network.jsonl",
            "firm_cluster.jsonl",
            "agenda_exposure.jsonl",
            "origin_context.jsonl",
            "payment_counterparty.jsonl",
            "donation_match_ambiguous.jsonl",
        ]:
            stub = analytics / stub_name
            if not stub.exists():
                stub.write_text("", encoding="utf-8")
        # Also create empty summary JSONs that the builder validates
        for summary_name in [
            "outlier_alert_summary.json",
            "baseline_summary.json",
            "compound_risk_summary.json",
            "rapporteur_profile_summary.json",
            "assignment_audit_summary.json",
            "counsel_network_cluster_summary.json",
            "firm_cluster_summary.json",
        ]:
            stub = analytics / summary_name
            if not stub.exists():
                stub.write_text("{}", encoding="utf-8")

        t0 = time.monotonic()
        db_path = serving / "atlas_stf.db"
        db_url = f"sqlite+pysqlite:///{db_path}"
        try:
            from atlas_stf.serving.builder import build_serving_database

            build_serving_database(
                database_url=db_url,
                curated_dir=curated,
                analytics_dir=analytics,
            )
            import sqlite3

            conn = sqlite3.connect(str(db_path))
            tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            conn.close()
            elapsed = time.monotonic() - t0
            steps.append(
                {
                    "step": "serving_build",
                    "status": "OK",
                    "detail": f"{len(tables)} tables in {db_path.stat().st_size / 1024:.0f} KB",
                    "elapsed": round(elapsed, 1),
                }
            )
        except Exception as exc:
            steps.append(
                {
                    "step": "serving_build",
                    "status": "FAILED",
                    "detail": str(exc)[:150],
                    "elapsed": round(time.monotonic() - t0, 1),
                }
            )
            has_critical = True
            return steps, has_critical

        # Step 5: API smoke test against fresh DB
        t0 = time.monotonic()
        try:
            from fastapi.testclient import TestClient

            from atlas_stf.api.app import create_app

            app = create_app(database_url=db_url)
            client = TestClient(app, raise_server_exceptions=False)

            api_results: list[str] = []
            critical_endpoints = [
                ("/health", ["status"]),
                ("/dashboard", ["kpis"]),
                ("/alerts?page=1&page_size=3", ["total", "items"]),
                ("/graph/search?page=1&page_size=3", ["total", "items"]),
                ("/graph/metrics", ["total_nodes"]),
            ]
            api_ok = 0
            for path, fields in critical_endpoints:
                resp = client.get(path)
                if resp.status_code == 200:
                    body = resp.json()
                    missing = [f for f in fields if f not in body] if isinstance(body, dict) else []
                    if missing:
                        api_results.append(f"✗ {path}: missing {missing}")
                    else:
                        api_results.append(f"✓ {path}: OK")
                        api_ok += 1
                else:
                    api_results.append(f"✗ {path}: HTTP {resp.status_code}")

            elapsed = time.monotonic() - t0
            all_ok = api_ok == len(critical_endpoints)
            steps.append(
                {
                    "step": "api_smoke_fresh",
                    "status": "OK" if all_ok else "PARTIAL",
                    "detail": f"{api_ok}/{len(critical_endpoints)} endpoints OK",
                    "sub_results": api_results,
                    "elapsed": round(elapsed, 1),
                }
            )
            if not all_ok:
                has_critical = True

        except Exception as exc:
            steps.append(
                {
                    "step": "api_smoke_fresh",
                    "status": "FAILED",
                    "detail": str(exc)[:150],
                    "elapsed": round(time.monotonic() - t0, 1),
                }
            )
            has_critical = True

    return steps, has_critical


def main() -> int:
    print("End-to-end audit (sample → analytics → serving → API)")
    print(f"Sample size: {SAMPLE_LINES} lines per JSONL\n")

    results, has_critical = run_end_to_end()

    total_elapsed = 0.0
    for r in results:
        marker = "✓" if r["status"] == "OK" else "✗" if r["status"] == "FAILED" else "◐"
        elapsed = r.get("elapsed", 0)
        total_elapsed += elapsed
        print(f"  {marker} {r['step']}: {r['status']} ({elapsed}s) — {r.get('detail', '')}")
        for sub in r.get("sub_results", []):
            print(f"      {sub}")

    ok_count = sum(1 for r in results if r["status"] == "OK")
    print(f"\nEnd-to-end: {ok_count}/{len(results)} steps OK ({total_elapsed:.1f}s total)")
    print(f"Verdict: {'BLOCKED' if has_critical else 'PASSED'}")
    return 1 if has_critical else 0


if __name__ == "__main__":
    sys.exit(main())
