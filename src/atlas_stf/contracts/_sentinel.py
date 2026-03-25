"""Schema sentinel: detect critical drift between expected and observed.

Validates inventories against hard expectations.  Critical violations
(missing files, missing columns) cause test failures; warnings (null
spikes, stale fingerprints) are reported but do not block.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from atlas_stf.contracts._inspector import _file_fingerprint


@dataclass
class DriftViolation:
    source: str
    file_name: str
    column: str
    violation_type: str  # file_missing | missing_column | null_spike | stale_fingerprint
    expected: str
    observed: str
    severity: str  # critical | warning


_CRITICAL_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "source": "cgu",
        "file_name": "ceis.csv",
        "required_columns": ["CPF OU CNPJ DO SANCIONADO", "NOME DO SANCIONADO"],
        "max_null_rates": {"CPF OU CNPJ DO SANCIONADO": 0.05},
    },
    {
        "source": "cgu",
        "file_name": "cnep.csv",
        "required_columns": ["CPF OU CNPJ DO SANCIONADO", "NOME DO SANCIONADO"],
        "max_null_rates": {"CPF OU CNPJ DO SANCIONADO": 0.05},
    },
    {
        "source": "cvm",
        "file_name": "processo_sancionador_acusado.csv",
        "required_columns": ["NUP", "Nome_Acusado"],
        "max_null_rates": {"NUP": 0.01, "Nome_Acusado": 0.05},
    },
    {
        "source": "stf",
        "file_name": "decisoes.csv",
        "required_columns": [
            "processo",
            "relator_atual",
            "data_da_decisao",
            "tipo_decisao",
        ],
        "max_null_rates": {"processo": 0.01, "relator_atual": 0.05},
    },
    {
        "source": "stf",
        "file_name": "plenario_virtual.csv",
        "required_columns": ["processo", "data_decisao", "tipo_decisao"],
        "max_null_rates": {"processo": 0.01},
    },
    {
        "source": "stf",
        "file_name": "distribuidos.csv",
        "required_columns": ["no_do_processo", "ministro_a"],
        "max_null_rates": {"no_do_processo": 0.01},
    },
    {
        "source": "rfb",
        "file_name": "partners_raw.jsonl",
        "required_columns": ["cnpj_basico", "partner_name_normalized"],
        "max_null_rates": {"cnpj_basico": 0.01},
    },
    {
        "source": "rfb",
        "file_name": "companies_raw.jsonl",
        "required_columns": ["cnpj_basico", "razao_social"],
        "max_null_rates": {"cnpj_basico": 0.01},
    },
    {
        "source": "tse",
        "file_name": "donations_raw.jsonl",
        "required_columns": [
            "donor_name_normalized",
            "donation_amount",
            "election_year",
        ],
        "max_null_rates": {"donor_name_normalized": 0.05, "election_year": 0.01},
    },
]


def validate_inventories(
    inventories: list[dict[str, Any]],
) -> list[DriftViolation]:
    """Return violations found when checking inventories against expectations."""
    violations: list[DriftViolation] = []
    inv_by_key = {f"{inv['source']}/{inv['file_name']}": inv for inv in inventories}

    for exp in _CRITICAL_EXPECTATIONS:
        key = f"{exp['source']}/{exp['file_name']}"
        inv = inv_by_key.get(key)
        if inv is None:
            violations.append(
                DriftViolation(
                    source=exp["source"],
                    file_name=exp["file_name"],
                    column="*",
                    violation_type="file_missing",
                    expected="inventory present",
                    observed="not found",
                    severity="critical",
                )
            )
            continue

        col_map = {c["observed_column_name"]: c for c in inv["columns"]}

        for req_col in exp.get("required_columns", []):
            if req_col not in col_map:
                violations.append(
                    DriftViolation(
                        source=exp["source"],
                        file_name=exp["file_name"],
                        column=req_col,
                        violation_type="missing_column",
                        expected="present",
                        observed="absent",
                        severity="critical",
                    )
                )

        for col_name, max_null in exp.get("max_null_rates", {}).items():
            if col_name not in col_map:
                continue
            col = col_map[col_name]
            actual = col["null_rate"] + col["empty_rate"]
            if actual > max_null:
                violations.append(
                    DriftViolation(
                        source=exp["source"],
                        file_name=exp["file_name"],
                        column=col_name,
                        violation_type="null_spike",
                        expected=f"null+empty <= {max_null:.1%}",
                        observed=f"null+empty = {actual:.1%}",
                        severity="warning",
                    )
                )

    return violations


def check_staleness(
    inventories: list[dict[str, Any]],
    project_root: Path,
) -> list[DriftViolation]:
    """Compare fingerprints in inventories with current files on disk."""
    violations: list[DriftViolation] = []
    for inv in inventories:
        rel = inv.get("file_path_relative", "")
        stored_fp = inv.get("file_fingerprint_sha256_1mb", "")
        if not rel or not stored_fp:
            continue
        real_path = project_root / rel
        if not real_path.exists():
            violations.append(
                DriftViolation(
                    source=inv["source"],
                    file_name=inv["file_name"],
                    column="*",
                    violation_type="source_file_missing",
                    expected="file exists on disk",
                    observed="not found",
                    severity="critical",
                )
            )
            continue
        current_fp = _file_fingerprint(real_path)
        if current_fp != stored_fp:
            violations.append(
                DriftViolation(
                    source=inv["source"],
                    file_name=inv["file_name"],
                    column="*",
                    violation_type="stale_fingerprint",
                    expected=f"fingerprint={stored_fp[:12]}...",
                    observed=f"fingerprint={current_fp[:12]}...",
                    severity="warning",
                )
            )
    return violations
