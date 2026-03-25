"""Generate observed data inventories from real source files.

Run:  uv run python -m atlas_stf.contracts.inspect_sources

Output: data/contracts/observed/<source>/<file>.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from atlas_stf.contracts._coverage import build_coverage_metadata, write_coverage_metadata
from atlas_stf.contracts._governance import classify_join_fitness, scope_declaration
from atlas_stf.contracts._governance_validator import validate_all
from atlas_stf.contracts._inspector import (
    inspect_csv,
    inspect_jsonl,
    inspect_jsonl_partitioned,
)
from atlas_stf.contracts._semantic_audit import build_semantic_audit, write_semantic_audit
from atlas_stf.contracts._stf_overlap import analyze_stf_overlap, write_overlap_report

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

# ------------------------------------------------------------------
# Source registry
# ------------------------------------------------------------------


def _s(
    tp: str,
    src: str,
    path: str,
    yc: str,
    out: str,
    **kw: Any,
) -> dict[str, Any]:
    return {"type": tp, "source": src, "path": path, "year_or_cycle": yc, "output": out, **kw}


_SOURCES: list[dict[str, Any]] = [
    # CGU
    _s("csv", "cgu", "data/raw/cgu/ceis.csv", "snapshot", "cgu/ceis.json", delimiter=";"),
    _s("csv", "cgu", "data/raw/cgu/cnep.csv", "snapshot", "cgu/cnep.json", delimiter=";"),
    _s("csv", "cgu", "data/raw/cgu/acordos-leniencia.csv", "snapshot", "cgu/acordos_leniencia.json", delimiter=";"),
    # CVM
    _s(
        "csv",
        "cvm",
        "data/raw/cvm/processo_sancionador.csv",
        "snapshot",
        "cvm/processo_sancionador.json",
        delimiter=";",
    ),
    _s(
        "csv",
        "cvm",
        "data/raw/cvm/processo_sancionador_acusado.csv",
        "snapshot",
        "cvm/processo_sancionador_acusado.json",
        delimiter=";",
    ),
    # STF
    _s("csv", "stf", "data/staging/transparencia/decisoes.csv", "all_years", "stf/decisoes.json", delimiter=","),
    _s(
        "csv",
        "stf",
        "data/staging/transparencia/plenario_virtual.csv",
        "all_years",
        "stf/plenario_virtual.json",
        delimiter=",",
    ),
    _s(
        "csv",
        "stf",
        "data/staging/transparencia/decisoes_covid.csv",
        "all_years",
        "stf/decisoes_covid.json",
        delimiter=",",
    ),
    _s(
        "csv", "stf", "data/staging/transparencia/distribuidos.csv", "all_years", "stf/distribuidos.json", delimiter=","
    ),
    # RFB
    _s("jsonl", "rfb", "data/raw/rfb/partners_raw.jsonl", "snapshot", "rfb/partners_raw.json", sample_size=20_000),
    _s("jsonl", "rfb", "data/raw/rfb/companies_raw.jsonl", "snapshot", "rfb/companies_raw.json", sample_size=20_000),
    _s(
        "jsonl",
        "rfb",
        "data/raw/rfb/establishments_raw.jsonl",
        "snapshot",
        "rfb/establishments_raw.json",
        sample_size=20_000,
    ),
    # TSE (aggregate)
    _s(
        "jsonl",
        "tse",
        "data/raw/tse/donations_raw.jsonl",
        "2002_2024",
        "tse/donations_raw.json",
        sample_size=30_000,
        partition_key="election_year",
    ),
    _s(
        "jsonl",
        "tse",
        "data/raw/tse/campaign_expenses_raw.jsonl",
        "2002_2024",
        "tse/campaign_expenses_raw.json",
        sample_size=30_000,
        partition_key="election_year",
    ),
]

# Sources that also get per-partition (per-year) inventories.
_PARTITIONED_SOURCES: list[dict[str, Any]] = [
    {
        "source": "tse",
        "path": "data/raw/tse/donations_raw.jsonl",
        "partition_key": "election_year",
        "max_per_partition": 3_000,
        "output_prefix": "tse/by_year/donations_raw",
    },
    {
        "source": "tse",
        "path": "data/raw/tse/campaign_expenses_raw.jsonl",
        "partition_key": "election_year",
        "max_per_partition": 3_000,
        "output_prefix": "tse/by_year/campaign_expenses_raw",
    },
]


# ------------------------------------------------------------------
# Cross-file alias / drift analysis
# ------------------------------------------------------------------

_STF_ALIAS_GROUPS: dict[str, list[str]] = {
    "process_number": ["processo", "no_do_processo"],
    "rapporteur": ["relator_atual", "relator_da_decisao", "relator", "ministro_a"],
    "decision_date": ["data_da_decisao", "data_decisao", "data_do_andamento"],
    "filing_date": ["data_de_autuacao", "data_autuacao", "data_da_autuacao"],
    "decision_type": ["tipo_decisao"],
    "decision_progress": ["andamento_decisao", "descricao_andamento", "andamento"],
    "legal_branch": ["ramo_direito", "ramos_do_direito", "ramo_do_direito"],
    "subjects": ["assuntos_do_processo", "assunto_completo", "assunto"],
    "case_class": ["classe", "tipo_de_classe"],
    "discharge_date": ["data_baixa", "data_da_baixa"],
    "in_progress": ["indicador_de_tramitacao", "em_tramitacao"],
    "judging_body": ["orgao_julgador"],
}


def _build_cross_file_report(
    inventories: list[dict[str, Any]],
) -> dict[str, Any]:
    by_source: dict[str, list[dict[str, Any]]] = {}
    for inv in inventories:
        by_source.setdefault(inv["source"], []).append(inv)

    report: dict[str, Any] = {}

    # Scope declaration
    report["scope"] = scope_declaration()

    # STF alias detection
    stf_invs = by_source.get("stf", [])
    if stf_invs:
        presence: dict[str, dict[str, bool]] = {}
        for inv in stf_invs:
            fname = inv["file_name"]
            for c in inv["columns"]:
                presence.setdefault(c["observed_column_name"], {})[fname] = True

        stf_groups: dict[str, dict[str, list[str]]] = {}
        for group_name, aliases in _STF_ALIAS_GROUPS.items():
            gp: dict[str, list[str]] = {}
            for alias in aliases:
                if alias in presence:
                    gp[alias] = sorted(presence[alias].keys())
            if gp:
                stf_groups[group_name] = gp
        report["stf_alias_groups"] = stf_groups
        report["stf_column_presence"] = {c: sorted(f.keys()) for c, f in sorted(presence.items())}

    # CGU schema comparison
    cgu_invs = by_source.get("cgu", [])
    if len(cgu_invs) >= 2:
        cgu_cols: dict[str, list[str]] = {}
        for inv in cgu_invs:
            for c in inv["columns"]:
                cgu_cols.setdefault(c["observed_column_name"], []).append(inv["file_name"])
        only_in = {col: files for col, files in cgu_cols.items() if len(files) < len(cgu_invs)}
        if only_in:
            report["cgu_schema_diff"] = only_in

    # CVM: flag missing CPF/CNPJ column
    for inv in by_source.get("cvm", []):
        if inv["file_name"] == "processo_sancionador_acusado.csv":
            col_names = {c["observed_column_name"] for c in inv["columns"]}
            cpf_aliases = {"CPF_CNPJ", "NR_CPF_CNPJ", "cpf_cnpj"}
            if not col_names & cpf_aliases:
                report["cvm_critical"] = {
                    "file": inv["file_name"],
                    "issue": "no_cpf_cnpj_column",
                    "available_columns": sorted(col_names),
                    "expected_aliases": sorted(cpf_aliases),
                    "impact": (
                        "entity_cnpj_cpf will always be None for CVM sanctions — tax-ID-based matching is impossible"
                    ),
                }

    # Quality findings: high null/empty rates
    threshold = 0.10
    quality: list[dict[str, Any]] = []
    for inv in inventories:
        for col in inv["columns"]:
            nr, er = col["null_rate"], col["empty_rate"]
            if nr > threshold or er > threshold:
                quality.append(
                    {
                        "source": inv["source"],
                        "file": inv["file_name"],
                        "year_or_cycle": inv.get("year_or_cycle", ""),
                        "column": col["observed_column_name"],
                        "null_rate": nr,
                        "empty_rate": er,
                        "severity": "high" if nr > 0.5 or er > 0.5 else "medium",
                    }
                )
    if quality:
        report["quality_findings"] = quality

    # TSE year-coverage gaps
    expected_years = {str(y) for y in range(2002, 2026, 2)}
    for inv in by_source.get("tse", []):
        sampled = inv.get("partition_values_sampled", {})
        if sampled:
            missing = sorted(expected_years - set(sampled.keys()))
            if missing:
                report.setdefault("tse_year_gaps", {})[inv["file_name"]] = {
                    "missing_years": missing,
                    "present_years": sorted(sampled.keys()),
                }

    # Governance seed: join fitness classification
    report["join_fitness"] = classify_join_fitness(inventories)

    return report


# ------------------------------------------------------------------
# Post-annotation: enrich inventories with alias/drift metadata
# ------------------------------------------------------------------

_COL_TO_ALIAS_GROUP: dict[str, str] = {}
for _group, _aliases in _STF_ALIAS_GROUPS.items():
    for _alias in _aliases:
        _COL_TO_ALIAS_GROUP[_alias] = _group


def _annotate_inventories(inventories: list[dict[str, Any]]) -> None:
    for inv in inventories:
        if inv["source"] != "stf":
            continue
        for col in inv["columns"]:
            group = _COL_TO_ALIAS_GROUP.get(col["observed_column_name"])
            if group:
                col["suspected_alias_group"] = group


def _write_inventory(inv: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(inv, f, ensure_ascii=False, indent=2)


# ------------------------------------------------------------------
# Runner
# ------------------------------------------------------------------


def run(project_root: Path | None = None, *, strict: bool = False) -> None:
    """Generate observed inventories and validate governance.

    When *strict* is True, exit with code 1 on any critical governance
    violation.  Intended for CI pipelines (``--strict`` flag).
    """
    root = project_root or _PROJECT_ROOT
    output_base = root / "data" / "contracts" / "observed"
    inventories: list[dict[str, Any]] = []

    # --- Aggregate inventories ---
    for spec in _SOURCES:
        file_path = root / spec["path"]
        if not file_path.exists():
            print(f"SKIP (not found): {spec['path']}", file=sys.stderr)
            continue

        print(f"Inspecting {spec['path']} ...", file=sys.stderr)

        if spec["type"] == "csv":
            inv = inspect_csv(
                file_path,
                source=spec["source"],
                year_or_cycle=spec["year_or_cycle"],
                delimiter=spec.get("delimiter", ";"),
                project_root=root,
            )
        else:
            inv = inspect_jsonl(
                file_path,
                source=spec["source"],
                year_or_cycle=spec["year_or_cycle"],
                project_root=root,
                sample_size=spec.get("sample_size", 20_000),
                partition_key=spec.get("partition_key"),
            )
        inventories.append(inv)

    _annotate_inventories(inventories)

    for inv, spec in zip(inventories, [s for s in _SOURCES if (root / s["path"]).exists()]):
        _write_inventory(inv, output_base / spec["output"])
        n = inv["total_records"]
        c = len(inv["columns"])
        print(f"  {spec['path']}: {n:,} records, {c} cols", file=sys.stderr)

    # --- Per-year partitioned inventories ---
    partitioned_all: list[dict[str, Any]] = []
    for spec in _PARTITIONED_SOURCES:
        file_path = root / spec["path"]
        if not file_path.exists():
            print(f"SKIP partitioned (not found): {spec['path']}", file=sys.stderr)
            continue

        print(f"Partitioning {spec['path']} by {spec['partition_key']} ...", file=sys.stderr)
        by_year = inspect_jsonl_partitioned(
            file_path,
            source=spec["source"],
            project_root=root,
            partition_key=spec["partition_key"],
            max_per_partition=spec.get("max_per_partition", 3_000),
        )
        for year, inv in sorted(by_year.items()):
            partitioned_all.append(inv)
            out_path = output_base / f"{spec['output_prefix']}_{year}.json"
            _write_inventory(inv, out_path)
            n = inv["total_records"]
            c = len(inv["columns"])
            print(f"  {year}: {n:,} records, {c} cols", file=sys.stderr)

    # --- Cross-file report (aggregate + partitioned) ---
    all_invs = inventories + partitioned_all
    if all_invs:
        report = _build_cross_file_report(all_invs)
        report_path = output_base / "_cross_file_report.json"
        _write_inventory(report, report_path)
        print(f"\nCross-file report: {report_path.relative_to(root)}", file=sys.stderr)

    # --- Coverage metadata ---
    coverage = build_coverage_metadata(output_base)
    cov_path = write_coverage_metadata(coverage, output_base)
    print(f"Coverage metadata: {cov_path.relative_to(root)}", file=sys.stderr)

    # --- STF overlap analysis ---
    staging_dir = root / "data" / "staging" / "transparencia"
    if staging_dir.exists():
        print("\nAnalyzing STF cross-CSV overlap ...", file=sys.stderr)
        overlap = analyze_stf_overlap(staging_dir)
        write_overlap_report(overlap, output_base / "stf" / "_overlap_analysis.json")
        dups = overlap.get("structural_duplicates", {}).get("total_candidate_count", 0)
        print(f"STF overlap: {dups} structural duplicate candidates", file=sys.stderr)

    # --- Semantic audit + Governance validation ---
    governance_dir = root / "data" / "contracts" / "governance"
    if governance_dir.exists():
        # Semantic audit
        audit = build_semantic_audit(governance_dir, output_base)
        write_semantic_audit(audit, output_base / "_semantic_audit.json")
        n = audit.get("summary", {}).get("total_findings", 0)
        blk = len(audit.get("summary", {}).get("blocked_columns", []))
        print(f"Semantic audit: {n} findings, {blk} blocked columns", file=sys.stderr)

        # Governance validation (blocking in --strict mode)
        violations = validate_all(governance_dir, output_base)
        critical = [v for v in violations if v.severity == "critical"]
        warnings = [v for v in violations if v.severity == "warning"]
        if critical:
            print(f"\nGovernance: {len(critical)} CRITICAL violations:", file=sys.stderr)
            for v in critical:
                print(f"  {v}", file=sys.stderr)
        if warnings:
            print(f"Governance: {len(warnings)} warnings", file=sys.stderr)
        if not critical and not warnings:
            print("Governance: OK (0 violations)", file=sys.stderr)
        if strict and critical:
            sys.exit(1)

    total_files = len(inventories) + len(partitioned_all)
    print(f"\nDone — {total_files} inventories generated.", file=sys.stderr)


if __name__ == "__main__":
    run(strict="--strict" in sys.argv)
