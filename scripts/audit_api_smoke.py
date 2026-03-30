#!/usr/bin/env python3
"""Smoke test critical API endpoints against the real serving database.

Uses FastAPI TestClient (no server startup needed) to verify that
endpoints return valid responses with expected shape.

Returns exit code 1 on any critical endpoint failure.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Critical endpoints to smoke test with expected response shape
SMOKE_ENDPOINTS: list[dict] = [
    {
        "path": "/health",
        "expected_fields": ["status"],
        "severity": "critical",
    },
    {
        "path": "/dashboard",
        "expected_fields": ["kpis", "filters"],
        "severity": "critical",
    },
    {
        "path": "/alerts?page=1&page_size=5",
        "expected_fields": ["total", "items"],
        "severity": "critical",
    },
    {
        "path": "/graph/search?page=1&page_size=5",
        "expected_fields": ["total", "items"],
        "severity": "critical",
    },
    {
        "path": "/graph/metrics",
        "expected_fields": ["total_nodes", "total_edges"],
        "severity": "critical",
    },
    {
        "path": "/graph/scores?page=1&page_size=5",
        "expected_fields": ["total", "items"],
        "severity": "high",
    },
    {
        "path": "/investigations/top?page=1&limit=5",
        "expected_fields": ["total", "items"],
        "severity": "high",
    },
    {
        "path": "/review/queue?page=1&page_size=5",
        "expected_fields": ["total", "items"],
        "severity": "high",
    },
    {
        "path": "/counsel-network?page=1&page_size=5",
        "expected_fields": ["total", "items"],
        "severity": "high",
    },
    {
        "path": "/sanctions?page=1&page_size=5",
        "expected_fields": ["total", "items"],
        "severity": "high",
    },
]


def run_smoke_tests(db_url: str | None = None) -> tuple[list[dict], bool]:
    """Run smoke tests. Returns (results, has_critical_failure)."""
    if db_url is None:
        db_path = REPO_ROOT / "data" / "serving" / "atlas_stf.db"
        if not db_path.exists():
            return [{"endpoint": "*", "status": "SKIP", "reason": "serving DB not found"}], False
        db_url = f"sqlite+pysqlite:///{db_path}"

    # Import here to avoid import errors when just checking
    from fastapi.testclient import TestClient

    from atlas_stf.api.app import create_app

    app = create_app(database_url=db_url)
    client = TestClient(app, raise_server_exceptions=False)

    results: list[dict] = []
    has_critical = False

    for ep in SMOKE_ENDPOINTS:
        path = ep["path"]
        severity = ep["severity"]
        expected_fields = ep.get("expected_fields", [])
        expect_list = ep.get("expect_list", False)

        try:
            resp = client.get(path)
            status_code = resp.status_code

            if status_code != 200:
                results.append({
                    "endpoint": path,
                    "status": "FAIL",
                    "severity": severity,
                    "reason": f"HTTP {status_code}",
                })
                if severity == "critical":
                    has_critical = True
                continue

            body = resp.json()

            # Check expected fields
            if expect_list:
                if not isinstance(body, list):
                    results.append({
                        "endpoint": path,
                        "status": "FAIL",
                        "severity": severity,
                        "reason": "expected list, got " + type(body).__name__,
                    })
                    if severity == "critical":
                        has_critical = True
                    continue
            elif expected_fields:
                missing = [f for f in expected_fields if f not in body]
                if missing:
                    results.append({
                        "endpoint": path,
                        "status": "FAIL",
                        "severity": severity,
                        "reason": f"missing fields: {missing}",
                    })
                    if severity == "critical":
                        has_critical = True
                    continue

            results.append({
                "endpoint": path,
                "status": "OK",
                "severity": severity,
            })

        except Exception as exc:
            reason = str(exc)[:200]
            # Detect schema mismatch (DB needs rebuild)
            if "no such table" in reason or "no such column" in reason:
                reason = f"SCHEMA_MISMATCH: {reason} — serving DB needs rebuild"
            results.append({
                "endpoint": path,
                "status": "ERROR",
                "severity": severity,
                "reason": reason,
            })
            if severity == "critical":
                has_critical = True

    return results, has_critical


def main() -> int:
    results, has_critical = run_smoke_tests()

    for r in results:
        marker = "✓" if r["status"] == "OK" else "✗" if r["status"] in ("FAIL", "ERROR") else "⊘"
        line = f"  {marker} [{r['severity']}] {r['endpoint']}: {r['status']}"
        if "reason" in r:
            line += f" — {r['reason']}"
        print(line)

    ok_count = sum(1 for r in results if r["status"] == "OK")
    print(f"\nAPI smoke tests: {ok_count}/{len(results)} passed")
    print(f"Verdict: {'BLOCKED' if has_critical else 'PASSED'}")
    return 1 if has_critical else 0


if __name__ == "__main__":
    sys.exit(main())
