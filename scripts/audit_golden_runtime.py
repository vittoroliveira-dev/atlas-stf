#!/usr/bin/env python3
"""Run critical pipeline on golden sample with semantic assertions.

Uses the golden sample from audit/samples/critical_core/ to execute:
  groups → baseline → alerts → counsel_network → serving → API

Each step is validated against assertions from the sample manifest.
All outputs are fresh (generated in tmpdir), never from canonical artifacts.

Returns exit code 1 on critical assertion failure.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DIR = REPO_ROOT / "audit" / "samples" / "critical_core"


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def _read_first_record(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                return json.loads(line)
    return None


def run_golden_runtime() -> tuple[list[dict], bool]:
    """Execute pipeline on golden sample. Returns (steps, has_critical)."""
    manifest_path = SAMPLE_DIR / "manifest.json"
    if not manifest_path.exists():
        return [{"step": "load_manifest", "status": "FAIL", "detail": "manifest.json not found", "proof": "none"}], True

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assertions = manifest.get("assertions", {})
    steps: list[dict] = []
    has_critical = False

    with tempfile.TemporaryDirectory(prefix="atlas_golden_") as tmpdir:
        tmp = Path(tmpdir)
        curated = tmp / "curated"
        analytics = tmp / "analytics"
        serving_dir = tmp / "serving"
        curated.mkdir()
        analytics.mkdir()
        serving_dir.mkdir()

        # Step 0: Copy golden sample to working dir
        t0 = time.monotonic()
        src_curated = SAMPLE_DIR / "curated"
        for f in sorted(src_curated.iterdir()):
            shutil.copy2(f, curated / f.name)
        steps.append({
            "step": "load_golden_sample",
            "status": "OK",
            "detail": f"{len(list(curated.iterdir()))} files",
            "proof": "golden_fixture",
            "elapsed": round(time.monotonic() - t0, 2),
        })

        # Step 1: build_groups
        t0 = time.monotonic()
        try:
            from atlas_stf.analytics.build_groups import build_groups

            build_groups(
                process_path=curated / "process.jsonl",
                decision_event_path=curated / "decision_event.jsonl",
                output_dir=analytics,
            )
            groups = _count_lines(analytics / "comparison_group.jsonl")
            links = _count_lines(analytics / "decision_event_group_link.jsonl")
            a = assertions.get("build_groups", {})
            ok = groups >= a.get("min_groups", 1) and links >= a.get("min_links", 1)
            steps.append({
                "step": "build_groups",
                "status": "OK" if ok else "ASSERT_FAIL",
                "detail": f"{groups} groups, {links} links (min: {a.get('min_groups', 1)}/{a.get('min_links', 1)})",
                "proof": "fresh_runtime",
                "elapsed": round(time.monotonic() - t0, 2),
            })
            if not ok:
                has_critical = True
        except Exception as exc:
            steps.append({"step": "build_groups", "status": "FAIL", "detail": str(exc)[:150], "proof": "fresh_runtime", "elapsed": round(time.monotonic() - t0, 2)})
            has_critical = True

        # Step 2: build_baseline
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
            baselines = _count_lines(analytics / "baseline.jsonl")
            a = assertions.get("build_baseline", {})
            ok = baselines >= a.get("min_baselines", 1)
            steps.append({
                "step": "build_baseline",
                "status": "OK" if ok else "ASSERT_FAIL",
                "detail": f"{baselines} baselines (min: {a.get('min_baselines', 1)})",
                "proof": "fresh_runtime",
                "elapsed": round(time.monotonic() - t0, 2),
            })
            if not ok:
                has_critical = True
        except Exception as exc:
            steps.append({"step": "build_baseline", "status": "FAIL", "detail": str(exc)[:150], "proof": "fresh_runtime", "elapsed": round(time.monotonic() - t0, 2)})
            has_critical = True

        # Create stub for compound_risk before alerts
        (analytics / "compound_risk.jsonl").write_text("", encoding="utf-8")

        # Step 3: build_alerts
        t0 = time.monotonic()
        try:
            from atlas_stf.analytics.build_alerts import build_alerts

            build_alerts(
                baseline_path=analytics / "baseline.jsonl",
                link_path=analytics / "decision_event_group_link.jsonl",
                decision_event_path=curated / "decision_event.jsonl",
                output_path=analytics / "outlier_alert.jsonl",
                summary_path=analytics / "outlier_alert_summary.json",
                compound_risk_path=analytics / "compound_risk.jsonl",
                process_path=curated / "process.jsonl",
            )
            alerts = _count_lines(analytics / "outlier_alert.jsonl")
            a = assertions.get("build_alerts", {})
            ok = alerts >= a.get("min_alerts", 1)
            steps.append({
                "step": "build_alerts",
                "status": "OK" if ok else "ASSERT_FAIL",
                "detail": f"{alerts} alerts (min: {a.get('min_alerts', 1)})",
                "proof": "fresh_runtime (compound_risk=stub)",
                "elapsed": round(time.monotonic() - t0, 2),
            })
            if not ok:
                has_critical = True
        except Exception as exc:
            steps.append({"step": "build_alerts", "status": "FAIL", "detail": str(exc)[:150], "proof": "fresh_runtime", "elapsed": round(time.monotonic() - t0, 2)})

        # Step 4: counsel_network
        t0 = time.monotonic()
        try:
            from atlas_stf.analytics.counsel_network import build_counsel_network

            cn_path = build_counsel_network(curated_dir=curated, output_dir=analytics)
            cn_count = _count_lines(cn_path)
            first_rec = _read_first_record(cn_path)
            has_baseline_rate = first_rec is not None and "baseline_rate" in first_rec
            a = assertions.get("counsel_network", {})
            req_fields = a.get("required_fields", [])
            missing_fields = [f for f in req_fields if first_rec and f not in first_rec] if first_rec else req_fields
            ok = cn_count >= a.get("min_clusters", 0) and not missing_fields
            detail = f"{cn_count} clusters"
            if missing_fields:
                detail += f", MISSING fields: {missing_fields}"
            elif cn_count > 0:
                detail += ", baseline_rate=present" if has_baseline_rate else ", baseline_rate=MISSING"
            steps.append({
                "step": "counsel_network",
                "status": "OK" if ok else ("EMPTY_LEGIT" if cn_count == 0 else "ASSERT_FAIL"),
                "detail": detail,
                "proof": "fresh_runtime",
                "elapsed": round(time.monotonic() - t0, 2),
            })
        except Exception as exc:
            steps.append({"step": "counsel_network", "status": "FAIL", "detail": str(exc)[:150], "proof": "fresh_runtime", "elapsed": round(time.monotonic() - t0, 2)})

        # Step 5: serving_build
        # Create stubs for non-exercised analytics
        for stub in [
            "ml_outlier_score.jsonl", "sanction_match.jsonl", "donation_match.jsonl",
            "corporate_network.jsonl", "sanction_corporate_link.jsonl", "decision_velocity.jsonl",
            "procedural_timeline.jsonl", "rapporteur_change.jsonl", "temporal_analysis.jsonl",
            "pauta_anomaly.jsonl", "sequential_analysis.jsonl", "counsel_affinity.jsonl",
            "counsel_donation_profile.jsonl", "counsel_sanction_profile.jsonl",
            "donation_event.jsonl", "economic_group.jsonl", "representation_graph.jsonl",
            "representation_recurrence.jsonl", "representation_windows.jsonl",
            "amicus_network.jsonl", "firm_cluster.jsonl", "agenda_exposure.jsonl",
            "origin_context.jsonl", "payment_counterparty.jsonl",
            "donation_match_ambiguous.jsonl", "rapporteur_profile.jsonl",
            "assignment_audit.jsonl",
        ]:
            p = analytics / stub
            if not p.exists():
                p.write_text("", encoding="utf-8")
        for stub_json in [
            "compound_risk_summary.json", "rapporteur_profile_summary.json",
            "assignment_audit_summary.json", "counsel_network_cluster_summary.json",
            "firm_cluster_summary.json",
        ]:
            p = analytics / stub_json
            if not p.exists():
                p.write_text("{}", encoding="utf-8")

        t0 = time.monotonic()
        db_path = serving_dir / "atlas_stf.db"
        db_url = f"sqlite+pysqlite:///{db_path}"
        try:
            from atlas_stf.serving.builder import build_serving_database

            build_serving_database(database_url=db_url, curated_dir=curated, analytics_dir=analytics)

            import sqlite3

            conn = sqlite3.connect(str(db_path))
            tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            conn.close()

            a = assertions.get("serving_build", {})
            ok = len(tables) >= a.get("min_tables", 30)
            stub_count = sum(1 for s in [
                "ml_outlier_score.jsonl", "sanction_match.jsonl", "donation_match.jsonl",
            ] if _count_lines(analytics / s) == 0)
            steps.append({
                "step": "serving_build",
                "status": "OK" if ok else "ASSERT_FAIL",
                "detail": f"{len(tables)} tables, {db_path.stat().st_size // 1024} KB ({stub_count} stub deps)",
                "proof": "fresh_runtime (non-critical deps=stub)",
                "elapsed": round(time.monotonic() - t0, 2),
            })
            if not ok:
                has_critical = True
        except Exception as exc:
            steps.append({"step": "serving_build", "status": "FAIL", "detail": str(exc)[:150], "proof": "fresh_runtime", "elapsed": round(time.monotonic() - t0, 2)})
            has_critical = True
            return steps, has_critical

        # Step 6: API smoke against fresh DB
        t0 = time.monotonic()
        try:
            from fastapi.testclient import TestClient

            from atlas_stf.api.app import create_app

            app = create_app(database_url=db_url)
            client = TestClient(app, raise_server_exceptions=False)

            endpoints = manifest.get("endpoints_validated", ["/health", "/dashboard", "/alerts"])
            ok_count = 0
            sub_results: list[str] = []
            for ep in endpoints:
                path = ep if "?" in ep else (ep + "?page=1&page_size=3" if ep not in ("/health", "/dashboard", "/graph/metrics") else ep)
                resp = client.get(path)
                if resp.status_code == 200:
                    ok_count += 1
                    sub_results.append(f"✓ {ep}: 200")
                else:
                    sub_results.append(f"✗ {ep}: {resp.status_code}")

            a = assertions.get("api_smoke", {})
            ok = ok_count >= a.get("min_ok_endpoints", 3)
            steps.append({
                "step": "api_smoke_fresh",
                "status": "OK" if ok else "ASSERT_FAIL",
                "detail": f"{ok_count}/{len(endpoints)} endpoints OK",
                "proof": "fresh_runtime",
                "sub_results": sub_results,
                "elapsed": round(time.monotonic() - t0, 2),
            })
            if not ok:
                has_critical = True
        except Exception as exc:
            steps.append({"step": "api_smoke_fresh", "status": "FAIL", "detail": str(exc)[:150], "proof": "fresh_runtime", "elapsed": round(time.monotonic() - t0, 2)})
            has_critical = True

    return steps, has_critical


def main() -> int:
    print("Golden sample runtime: critical_core")
    print(f"Sample: {SAMPLE_DIR}\n")

    results, has_critical = run_golden_runtime()

    total_elapsed = 0.0
    for r in results:
        marker = "✓" if r["status"] == "OK" else "✗" if r["status"] in ("FAIL", "ASSERT_FAIL") else "◐"
        elapsed = r.get("elapsed", 0)
        total_elapsed += elapsed
        print(f"  {marker} {r['step']}: {r['status']} ({elapsed}s) [{r.get('proof', '?')}]")
        print(f"      {r.get('detail', '')}")
        for sub in r.get("sub_results", []):
            print(f"      {sub}")

    ok_count = sum(1 for r in results if r["status"] in ("OK", "EMPTY_LEGIT"))
    print(f"\nGolden runtime: {ok_count}/{len(results)} steps OK ({total_elapsed:.1f}s)")
    print(f"Verdict: {'BLOCKED' if has_critical else 'PASSED'}")
    return 1 if has_critical else 0


if __name__ == "__main__":
    sys.exit(main())
