"""Governance validator: checks governance declarations against observed inventories."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._coverage import _read_inventory
from ._governance import _EMPTY_THRESHOLD, _NULL_THRESHOLD


@dataclass(frozen=True)
class GovernanceViolation:
    """A mismatch between governance declaration and observed reality."""

    source: str
    column: str
    violation_type: str
    severity: str  # critical | warning
    message: str

    def __str__(self) -> str:
        return f"[{self.severity.upper()}] {self.source}/{self.column}: {self.violation_type} — {self.message}"


def _v(source: str, column: str, vtype: str, severity: str, message: str) -> GovernanceViolation:
    return GovernanceViolation(source, column, vtype, severity, message)


# -- Inventory helpers -------------------------------------------------------


def _load_inventories(observed_dir: Path) -> dict[str, dict[str, Any]]:
    """Load inventory JSONs from *observed_dir*, skipping ``_*`` and ``by_year``."""
    result: dict[str, dict[str, Any]] = {}
    if not observed_dir.is_dir():
        return result
    for path in sorted(observed_dir.rglob("*.json")):
        if path.name.startswith("_") or "by_year" in path.parts:
            continue
        inv = _read_inventory(path)
        if inv is None:
            continue
        source = inv.get("source", "")
        fname = inv.get("file_name", path.name)
        result[f"{source}/{fname}"] = inv
    return result


def _inventory_columns(inv: dict[str, Any]) -> set[str]:
    """Return set of ``observed_column_name`` values from inventory."""
    return {c["observed_column_name"] for c in inv.get("columns", [])}


def _find_column_stats(inv: dict[str, Any], col_name: str) -> dict[str, Any] | None:
    for c in inv.get("columns", []):
        if c["observed_column_name"] == col_name:
            return c
    return None


def _load_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return None


# -- Governance validation ---------------------------------------------------


def validate_governance(governance_path: Path, observed_dir: Path) -> list[GovernanceViolation]:
    """Validate ``column_governance.json`` against observed inventories."""
    violations: list[GovernanceViolation] = []
    raw = _load_json(governance_path)
    if raw is None:
        msg = f"Cannot read or parse {governance_path}"
        return [_v("governance", "", "governance_file_invalid", "critical", msg)]

    inventories = _load_inventories(observed_dir)

    for entry in raw.get("canonical_columns", []):
        canonical: str = entry.get("canonical_name", "")
        join_role: str = entry.get("join_role", "")
        aliases: dict[str, str | None] = entry.get("aliases", {})

        for source_file, col_name_or_list in aliases.items():
            inv = inventories.get(source_file)
            if inv is None:
                continue
            observed = _inventory_columns(inv)

            if col_name_or_list is None:
                if canonical in observed:
                    violations.append(
                        _v(
                            source_file,
                            canonical,
                            "alias_target_present_but_declared_absent",
                            "warning",
                            f"Column '{canonical}' declared absent but found in observed inventory",
                        )
                    )
                continue

            # Aliases can be a single string or a list of strings
            col_names: list[str] = col_name_or_list if isinstance(col_name_or_list, list) else [col_name_or_list]
            for col_name in col_names:
                if col_name not in observed:
                    violations.append(
                        _v(
                            source_file,
                            col_name,
                            "alias_target_missing",
                            "critical",
                            f"Column '{col_name}' (canonical: {canonical}) declared but absent from observed inventory",
                        )
                    )
                    continue

                if join_role == "primary_key":
                    stats = _find_column_stats(inv, col_name)
                    if stats is not None:
                        nr = stats.get("null_rate") or 0.0
                        er = stats.get("empty_rate") or 0.0
                        if nr > _NULL_THRESHOLD or er > _EMPTY_THRESHOLD:
                            violations.append(
                                _v(
                                    source_file,
                                    col_name,
                                    "primary_key_not_deterministic",
                                    "warning",
                                    f"Primary key '{col_name}' (canonical: {canonical}) "
                                    f"null={nr:.1%}, empty={er:.1%} — exceeds {_NULL_THRESHOLD:.0%} threshold",
                                )
                            )

    return violations


# -- Matrix validation -------------------------------------------------------


def validate_matrix(matrix_path: Path, governance_path: Path) -> list[GovernanceViolation]:
    """Validate ``join_matrix.json`` against ``column_governance.json``."""
    violations: list[GovernanceViolation] = []

    matrix = _load_json(matrix_path)
    if matrix is None:
        return [_v("matrix", "", "governance_file_invalid", "critical", f"Cannot read {matrix_path}")]

    governance = _load_json(governance_path)
    if governance is None:
        return [_v("governance", "", "governance_file_invalid", "critical", f"Cannot read {governance_path}")]

    canonical_names: set[str] = set()
    alias_index: dict[str, dict[str, str | None]] = {}
    for entry in governance.get("canonical_columns", []):
        name = entry.get("canonical_name", "")
        canonical_names.add(name)
        alias_index[name] = entry.get("aliases", {})

    for join_entry in matrix.get("matrix", []):
        concept: str = join_entry.get("concept", "")
        if concept not in canonical_names:
            violations.append(
                _v(
                    "matrix",
                    concept,
                    "matrix_references_unknown_concept",
                    "critical",
                    f"Concept '{concept}' not in governance",
                )
            )
            continue

        gov_aliases = alias_index.get(concept, {})
        for side in ("a", "b"):
            src: str = join_entry.get(f"source_{side}", "")
            field: str = join_entry.get(f"field_{side}", "")
            if not src or not field or src not in gov_aliases:
                continue
            declared = gov_aliases[src]
            if declared is None:
                violations.append(
                    _v(
                        src,
                        field,
                        "alias_target_present_but_declared_absent",
                        "warning",
                        f"Matrix references '{field}' but governance declares '{concept}' absent in '{src}'",
                    )
                )
            elif declared != field:
                violations.append(
                    _v(
                        src,
                        field,
                        "alias_target_missing",
                        "critical",
                        f"Matrix references '{field}' but governance maps '{concept}' to '{declared}' in '{src}'",
                    )
                )

    return violations


# -- Convenience entry-point -------------------------------------------------


def validate_all(governance_dir: Path, observed_dir: Path) -> list[GovernanceViolation]:
    """Run all governance validations and return combined violations."""
    gov = governance_dir / "column_governance.json"
    mtx = governance_dir / "join_matrix.json"
    return validate_governance(gov, observed_dir) + validate_matrix(mtx, gov)
