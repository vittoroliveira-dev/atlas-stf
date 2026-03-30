#!/usr/bin/env python3
"""Audit pipeline stage contracts: verify that each pipeline stage
produced expected artifacts with expected fields and minimum volumes.

This is a POST-HOC check on artifacts already produced — it does NOT
re-execute builders. It verifies the last pipeline run left the system
in a consistent state.

Returns exit code 1 on any critical contract violation.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"


def _read_jsonl_sample(path: Path, n: int = 5) -> list[dict]:
    """Read first N records from JSONL for field inspection."""
    records: list[dict] = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
                if len(records) >= n:
                    break
    return records


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _check_fields(records: list[dict], required_fields: list[str]) -> list[str]:
    """Return list of missing fields (fields absent in ALL sample records)."""
    if not records:
        return required_fields
    missing = []
    for field in required_fields:
        if not any(field in r for r in records):
            missing.append(field)
    return missing


# ---------------------------------------------------------------------------
# Stage definitions
# ---------------------------------------------------------------------------

STAGES: list[dict] = [
    {
        "name": "raw_stf",
        "description": "CSVs do Painel de Transparência STF",
        "severity": "critical",
        "artifacts": [
            {"path": "raw/transparencia/acervo.csv", "min_lines": 100},
            {"path": "raw/transparencia/decisoes.csv", "min_lines": 100},
        ],
    },
    {
        "name": "curate_core",
        "description": "Entidades curadas centrais",
        "severity": "critical",
        "artifacts": [
            {
                "path": "curated/process.jsonl",
                "min_lines": 10000,
                "required_fields": ["process_id", "process_class", "process_number"],
            },
            {
                "path": "curated/decision_event.jsonl",
                "min_lines": 10000,
                "required_fields": ["decision_event_id", "process_id", "decision_date", "current_rapporteur"],
            },
            {
                "path": "curated/party.jsonl",
                "min_lines": 1000,
                "required_fields": ["party_id", "party_name_normalized"],
            },
            {
                "path": "curated/counsel.jsonl",
                "min_lines": 1000,
                "required_fields": ["counsel_id"],
            },
            {
                "path": "curated/process_party_link.jsonl",
                "min_lines": 1000,
                "required_fields": ["process_id", "party_id"],
            },
            {
                "path": "curated/process_counsel_link.jsonl",
                "min_lines": 1000,
                "required_fields": ["process_id", "counsel_id"],
            },
        ],
    },
    {
        "name": "analytics_baseline",
        "description": "Grupos, baseline e alertas estatísticos",
        "severity": "critical",
        "artifacts": [
            {
                "path": "analytics/comparison_group.jsonl",
                "min_lines": 100,
                "required_fields": ["comparison_group_id"],
            },
            {
                "path": "analytics/baseline.jsonl",
                "min_lines": 100,
                "required_fields": ["baseline_id", "favorable_rate"],
            },
            {
                "path": "analytics/outlier_alert.jsonl",
                "min_lines": 100,
                "required_fields": ["alert_id", "alert_score"],
            },
        ],
    },
    {
        "name": "analytics_profiles",
        "description": "Perfis de relator e auditoria de distribuição",
        "severity": "high",
        "artifacts": [
            {
                "path": "analytics/rapporteur_profile.jsonl",
                "min_lines": 10,
                "required_fields": ["rapporteur", "decision_year"],
            },
            {
                "path": "analytics/assignment_audit.jsonl",
                "min_lines": 10,
                "required_fields": ["process_class", "decision_year"],
            },
        ],
    },
    {
        "name": "analytics_network",
        "description": "Redes de advogados e afinidade",
        "severity": "high",
        "artifacts": [
            {
                "path": "analytics/counsel_network_cluster.jsonl",
                "min_lines": 100,
                "required_fields": ["cluster_id", "red_flag"],
            },
            {
                "path": "analytics/counsel_affinity.jsonl",
                "min_lines": 100,
                "required_fields": ["affinity_id"],
            },
        ],
    },
    {
        "name": "cross_source_sanctions",
        "description": "Match de sanções CGU × partes/advogados STF",
        "severity": "high",
        "artifacts": [
            {
                "path": "analytics/sanction_match.jsonl",
                "min_lines": 1,
                "required_fields": ["match_id", "match_strategy", "match_score"],
            },
        ],
    },
    {
        "name": "cross_source_donations",
        "description": "Match de doações TSE × partes/advogados STF",
        "severity": "high",
        "artifacts": [
            {
                "path": "analytics/donation_match.jsonl",
                "min_lines": 1,
                "required_fields": ["match_id", "match_strategy"],
            },
        ],
    },
    {
        "name": "compound_risk",
        "description": "Risco composto (7 sinais agregados)",
        "severity": "critical",
        "artifacts": [
            {
                "path": "analytics/compound_risk.jsonl",
                "min_lines": 100,
                "required_fields": ["pair_id", "signal_count"],
            },
        ],
    },
    {
        "name": "serving_db",
        "description": "SQLite materializado para API",
        "severity": "critical",
        "check_type": "sqlite",
        "artifacts": [
            {"path": "serving/atlas_stf.db", "min_tables": 30},
        ],
    },
]


def _check_sqlite(db_path: Path, min_tables: int) -> tuple[bool, str]:
    """Check SQLite database has minimum number of tables."""
    import sqlite3

    if not db_path.exists():
        return False, "database file not found"
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        if len(tables) < min_tables:
            return False, f"only {len(tables)} tables (min: {min_tables})"
        return True, f"{len(tables)} tables"
    except sqlite3.Error as e:
        return False, f"sqlite error: {e}"


def run_audit() -> tuple[list[dict], bool]:
    """Run pipeline contract checks. Returns (results, has_critical_failure)."""
    results: list[dict] = []
    has_critical = False

    for stage in STAGES:
        stage_name = stage["name"]
        severity = stage.get("severity", "medium")
        check_type = stage.get("check_type", "jsonl")
        stage_ok = True
        stage_details: list[str] = []

        for artifact_def in stage["artifacts"]:
            rel_path = artifact_def["path"]
            full_path = DATA_DIR / rel_path
            min_lines = artifact_def.get("min_lines", 0)
            min_tables = artifact_def.get("min_tables", 0)
            required_fields = artifact_def.get("required_fields", [])

            if check_type == "sqlite":
                ok, detail = _check_sqlite(full_path, min_tables)
                if ok:
                    stage_details.append(f"  ✓ {rel_path}: {detail}")
                else:
                    stage_details.append(f"  ✗ {rel_path}: {detail}")
                    stage_ok = False
                continue

            if not full_path.exists():
                stage_details.append(f"  ✗ {rel_path}: MISSING")
                stage_ok = False
                continue

            count = _count_lines(full_path)
            if count < min_lines:
                stage_details.append(
                    f"  ✗ {rel_path}: {count} lines (min: {min_lines})"
                )
                stage_ok = False
                continue

            # Check required fields
            if required_fields:
                sample = _read_jsonl_sample(full_path)
                missing = _check_fields(sample, required_fields)
                if missing:
                    stage_details.append(
                        f"  ✗ {rel_path}: {count} lines, missing fields: {missing}"
                    )
                    stage_ok = False
                    continue

            stage_details.append(f"  ✓ {rel_path}: {count:,} lines")

        status = "OK" if stage_ok else "FAILED"
        if not stage_ok and severity == "critical":
            has_critical = True

        results.append({
            "stage": stage_name,
            "description": stage["description"],
            "severity": severity,
            "status": status,
            "details": stage_details,
        })

    return results, has_critical


def main() -> int:
    results, has_critical = run_audit()

    for r in results:
        marker = "✓" if r["status"] == "OK" else "✗"
        print(f"\n{marker} [{r['severity']}] {r['stage']}: {r['status']}")
        print(f"  {r['description']}")
        for d in r["details"]:
            print(d)

    ok_count = sum(1 for r in results if r["status"] == "OK")
    total = len(results)
    print(f"\nPipeline contracts: {ok_count}/{total} stages OK")
    print(f"Verdict: {'BLOCKED' if has_critical else 'PASSED'}")
    return 1 if has_critical else 0


if __name__ == "__main__":
    sys.exit(main())
