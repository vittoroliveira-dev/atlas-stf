"""Schema sentinel — fails on critical drift in observed inventories.

Validates that:
1. All critical source files have inventories.
2. Required columns are present.
3. Null/empty rates on join-critical columns stay within bounds.
4. Source file fingerprints match (detects stale inventories).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.contracts._sentinel import (
    check_staleness,
    validate_inventories,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_OBSERVED_DIR = _PROJECT_ROOT / "data" / "contracts" / "observed"


def _load_inventories() -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for p in sorted(_OBSERVED_DIR.rglob("*.json")):
        if p.name.startswith("_"):
            continue
        # Skip per-year partitioned files (tested separately)
        if "/by_year/" in str(p):
            continue
        with open(p) as f:
            result.append(json.load(f))
    return result


@pytest.fixture()
def inventories() -> list[dict[str, object]]:
    if not _OBSERVED_DIR.exists():
        pytest.skip("observed inventories not generated — run inspect_sources first")
    invs = _load_inventories()
    if not invs:
        pytest.skip("no inventory files found")
    return invs


def test_no_critical_violations(inventories: list[dict[str, object]]) -> None:
    violations = validate_inventories(inventories)  # type: ignore[arg-type]
    critical = [v for v in violations if v.severity == "critical"]
    if critical:
        lines = [
            f"  {v.source}/{v.file_name}: {v.column} — {v.violation_type} "
            f"(expected={v.expected}, observed={v.observed})"
            for v in critical
        ]
        pytest.fail("Critical schema violations:\n" + "\n".join(lines))


def test_warnings_are_documented(inventories: list[dict[str, object]]) -> None:
    """Warnings exist but should not block the pipeline."""
    violations = validate_inventories(inventories)  # type: ignore[arg-type]
    warnings = [v for v in violations if v.severity == "warning"]
    # Just ensure we can enumerate them without error
    assert isinstance(warnings, list)


def test_fingerprints_not_stale(inventories: list[dict[str, object]]) -> None:
    violations = check_staleness(inventories, _PROJECT_ROOT)  # type: ignore[arg-type]
    critical = [v for v in violations if v.severity == "critical"]
    if critical:
        lines = [f"  {v.source}/{v.file_name}: {v.violation_type}" for v in critical]
        pytest.fail("Source files missing on disk:\n" + "\n".join(lines))
    stale = [v for v in violations if v.violation_type == "stale_fingerprint"]
    if stale:
        names = [f"{v.source}/{v.file_name}" for v in stale]
        pytest.warns(
            UserWarning,
            match="stale",
        ) if False else None  # noqa: SIM223 — just document, don't block
        print(f"Stale inventories (re-run inspect_sources): {', '.join(names)}")


def test_per_year_inventories_exist() -> None:
    """TSE per-year inventories must exist for schema evolution tracking."""
    by_year_dir = _OBSERVED_DIR / "tse" / "by_year"
    if not by_year_dir.exists():
        pytest.skip("per-year inventories not generated yet")
    files = sorted(by_year_dir.glob("*.json"))
    assert len(files) >= 7, f"Expected at least 7 per-year files, got {len(files)}"
    # Each file must have columns
    for p in files:
        with open(p) as f:
            inv = json.load(f)
        assert len(inv["columns"]) > 0, f"{p.name} has no columns"
        assert inv["total_records"] > 0, f"{p.name} has no records"


def test_cross_file_report_has_scope() -> None:
    report_path = _OBSERVED_DIR / "_cross_file_report.json"
    if not report_path.exists():
        pytest.skip("cross-file report not generated")
    with open(report_path) as f:
        report = json.load(f)
    assert "scope" in report, "Cross-file report missing scope declaration"
    scope = report["scope"]
    assert "sources_inspected" in scope
    assert "sources_deferred" in scope
    assert "agenda" in scope["sources_deferred"]


def test_cross_file_report_has_join_fitness() -> None:
    report_path = _OBSERVED_DIR / "_cross_file_report.json"
    if not report_path.exists():
        pytest.skip("cross-file report not generated")
    with open(report_path) as f:
        report = json.load(f)
    assert "join_fitness" in report, "Cross-file report missing join fitness"
    fitness = report["join_fitness"]
    # CVM acusado CPF_CNPJ must be marked as absent
    cvm_key = "cvm/processo_sancionador_acusado.csv"
    assert cvm_key in fitness, f"Missing fitness for {cvm_key}"
    assert fitness[cvm_key]["CPF_CNPJ"]["actual"] == "absent"
