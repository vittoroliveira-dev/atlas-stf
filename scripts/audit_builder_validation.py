"""Audit schema validation coverage across analytics builders.

Statically analyzes builder source files using Python's ast module to verify
that every builder writing JSONL or summary JSON has proper schema definitions
and calls validate_records before writing output.

Uses only stdlib — no atlas_stf imports.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ANALYTICS_DIR = PROJECT_ROOT / "src" / "atlas_stf" / "analytics"
SCHEMAS_DIR = PROJECT_ROOT / "schemas"

# Files that are helpers, not builders — skip unconditionally.
HELPER_FILES: set[str] = {
    "__init__.py",
    "_match_helpers.py",
    "_match_io.py",
    "_atomic_io.py",
    "_outcome_helpers.py",
    "_corporate_enrichment.py",
    "_parallel.py",
    "_run_context.py",
    "score.py",
    "group_rules.py",
    "_donor_identity.py",
    "_donation_aggregator.py",
    "_donation_match_counsel.py",
    "_compound_risk_evidence.py",
    "_compound_risk_loaders.py",
    "_corporate_network_context.py",
    "_scl_bridge.py",
    "_scl_loaders.py",
    "_scl_traversal.py",
    "_scl_record_builder.py",
    "_temporal_monthly.py",
    "_temporal_events.py",
    "_temporal_corporate.py",
}

# Known exceptions with justifications.
ALLOWLIST: dict[str, str] = {
    "donation_empirical.py": (
        "writes a report JSON, not a standard builder artifact;"
        " exempt from summary schema"
    ),
    "match_calibration.py": (
        "validate_records for main artifact happens before write but"
        " after summary; non-standard order is intentional"
    ),
}

# Schema variable names used across builders (non-standard names).
SCHEMA_VAR_NAMES: set[str] = {
    "SCHEMA_PATH",
    "SUMMARY_SCHEMA_PATH",
    "BASELINE_SCHEMA",
    "SUMMARY_SCHEMA",
    "PROCESS_SCHEMA",
    "LINK_SCHEMA",
    "ALERT_SCHEMA",
    "FLOW_SCHEMA",
}


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class Finding:
    builder: str
    file_path: str
    artifact_type: str  # "main" | "summary"
    status: str  # "ok" | "missing_schema" | ... | "exempt"
    details: str
    validate_line: int | None = None
    write_line: int | None = None


@dataclass
class BuilderInfo:
    name: str
    file_path: Path
    schema_vars: dict[str, SchemaAssignment] = field(default_factory=dict)
    imported_schema_vars: set[str] = field(default_factory=set)
    validate_calls: list[ValidateCall] = field(default_factory=list)
    write_ops: list[WriteOp] = field(default_factory=list)


@dataclass
class SchemaAssignment:
    var_name: str
    schema_path_str: str
    line: int


@dataclass
class ValidateCall:
    schema_var: str
    line: int


@dataclass
class WriteOp:
    kind: str  # "jsonl" | "summary" | "json_other"
    line: int
    target_hint: str  # best-effort description of what's being written


# ---------------------------------------------------------------------------
# AST analysis
# ---------------------------------------------------------------------------


def _extract_string_from_path_call(node: ast.Call) -> str | None:
    """Extract the string argument from Path('...') calls."""
    if not node.args:
        return None
    arg = node.args[0]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    return None


def _is_path_constructor(node: ast.expr) -> bool:
    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Name) and func.id == "Path":
            return True
        if isinstance(func, ast.Attribute) and func.attr == "Path":
            return True
    return False


def _get_call_name(node: ast.Call) -> str | None:
    """Get the function name from a Call node."""
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _call_references_var(
    node: ast.Call, var_names: set[str]
) -> str | None:
    """Check if any argument references one of the given variable names."""
    for arg in node.args:
        if isinstance(arg, ast.Name) and arg.id in var_names:
            return arg.id
    for kw in node.keywords:
        if isinstance(kw.value, ast.Name) and kw.value.id in var_names:
            return kw.value.id
    return None


def _classify_write_text(node: ast.Call) -> WriteOp | None:
    """Classify a .write_text() call as summary, json_other, or None."""
    func = node.func
    if not isinstance(func, ast.Attribute) or func.attr != "write_text":
        return None

    # Check the receiver of .write_text()
    val = func.value

    # Direct variable: summary_path.write_text(...)
    if isinstance(val, ast.Name):
        if "summary" in val.id.lower():
            return WriteOp(
                kind="summary", line=node.lineno, target_hint=val.id
            )
        return WriteOp(
            kind="json_other", line=node.lineno, target_hint=val.id
        )

    # Inline path: (dir / "name_summary.json").write_text(...)
    if isinstance(val, ast.BinOp) and isinstance(val.op, ast.Div):
        right = val.right
        if isinstance(right, ast.Constant) and isinstance(right.value, str):
            if "summary" in right.value.lower():
                return WriteOp(
                    kind="summary",
                    line=node.lineno,
                    target_hint=right.value,
                )
            return WriteOp(
                kind="json_other",
                line=node.lineno,
                target_hint=right.value,
            )

    # Subscript or other expression — conservative: json_other
    return WriteOp(kind="json_other", line=node.lineno, target_hint="")


def _classify_call_write(node: ast.Call) -> WriteOp | None:
    """Detect write operations from a Call node."""
    func = node.func

    # AtomicJsonlWriter(output_path)
    if isinstance(func, ast.Name) and func.id == "AtomicJsonlWriter":
        return WriteOp(
            kind="jsonl", line=node.lineno, target_hint="AtomicJsonlWriter"
        )

    # write_jsonl(records, path)
    if isinstance(func, ast.Name) and func.id == "write_jsonl":
        return WriteOp(
            kind="jsonl", line=node.lineno, target_hint="write_jsonl"
        )

    # .write_text(...)
    return _classify_write_text(node)


def _annotate_parents(tree: ast.Module) -> dict[int, ast.AST]:
    """Build node-id → parent mapping for an AST tree."""
    parents: dict[int, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[id(child)] = node
    return parents


def _enclosing_function(
    node: ast.AST, parents: dict[int, ast.AST]
) -> ast.FunctionDef | None:
    """Walk up to find the enclosing FunctionDef, if any."""
    current: ast.AST = node
    while True:
        parent = parents.get(id(current))
        if parent is None:
            return None
        if isinstance(parent, ast.FunctionDef):
            return parent
        current = parent


def _find_call_sites(
    tree: ast.Module, func_name: str
) -> list[int]:
    """Find line numbers where a function is called in the module."""
    lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _get_call_name(node)
            if name == func_name:
                lines.append(node.lineno)
    return lines


def analyze_builder(file_path: Path) -> BuilderInfo:
    """Parse a builder file and extract schema, validation, and write info."""
    source = file_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(file_path))
    info = BuilderInfo(name=file_path.stem, file_path=file_path)
    parents = _annotate_parents(tree)

    # Collect names of internal helper functions that contain write ops,
    # so we can replace their line with the call-site line.
    helper_write_funcs: dict[str, str] = {}  # func_name → write kind

    for node in ast.walk(tree):
        # Module-level assignments: SCHEMA_PATH = Path("schemas/...")
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if not isinstance(target, ast.Name):
                    continue
                if target.id not in SCHEMA_VAR_NAMES:
                    continue
                if (
                    _is_path_constructor(node.value)
                    and isinstance(node.value, ast.Call)
                ):
                    path_str = _extract_string_from_path_call(node.value)
                    if path_str:
                        info.schema_vars[target.id] = SchemaAssignment(
                            var_name=target.id,
                            schema_path_str=path_str,
                            line=node.lineno,
                        )

        # Imports: from ... import SCHEMA_PATH, SUMMARY_SCHEMA_PATH
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                real_name = alias.asname or alias.name
                if real_name in SCHEMA_VAR_NAMES:
                    info.imported_schema_vars.add(real_name)

        # Calls to validate_records(records, SCHEMA_VAR)
        if isinstance(node, ast.Call):
            name = _get_call_name(node)
            if name == "validate_records":
                ref = _call_references_var(node, SCHEMA_VAR_NAMES)
                if ref:
                    info.validate_calls.append(
                        ValidateCall(schema_var=ref, line=node.lineno)
                    )

        # Write operations
        if isinstance(node, ast.Call):
            write = _classify_call_write(node)
            if write:
                # Check if this write is inside a helper function
                enclosing = _enclosing_function(node, parents)
                if (
                    enclosing
                    and enclosing.name.startswith("_")
                    and write.kind == "jsonl"
                ):
                    helper_write_funcs[enclosing.name] = write.kind
                else:
                    info.write_ops.append(write)

    # For each helper function that wraps a write, find its call sites
    for func_name, kind in helper_write_funcs.items():
        call_lines = _find_call_sites(tree, func_name)
        for line in call_lines:
            info.write_ops.append(
                WriteOp(kind=kind, line=line, target_hint=f"{func_name}()")
            )
        if not call_lines:
            # Helper exists but never called — still register it
            info.write_ops.append(
                WriteOp(
                    kind=kind, line=0, target_hint=f"{func_name}() (uncalled)"
                )
            )

    return info


def is_builder(file_path: Path) -> bool:
    """Check if a file produces output artifacts by scanning for writes."""
    try:
        source = file_path.read_text(encoding="utf-8")
    except OSError:
        return False

    indicators = ["AtomicJsonlWriter", "write_jsonl", "write_text"]
    if not any(ind in source for ind in indicators):
        return False

    try:
        tree = ast.parse(source, filename=str(file_path))
    except SyntaxError:
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _get_call_name(node)
            if name in ("AtomicJsonlWriter", "write_jsonl"):
                return True
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "write_text"
            ):
                return True

    return False


# ---------------------------------------------------------------------------
# Verification logic
# ---------------------------------------------------------------------------


def _find_main_schema_var(info: BuilderInfo) -> str | None:
    """Find the schema variable for main JSONL output."""
    candidates = (
        "SCHEMA_PATH",
        "BASELINE_SCHEMA",
        "PROCESS_SCHEMA",
        "ALERT_SCHEMA",
        "FLOW_SCHEMA",
    )
    for name in candidates:
        if name in info.schema_vars or name in info.imported_schema_vars:
            return name
    return None


def _find_summary_schema_var(info: BuilderInfo) -> str | None:
    """Find the schema variable for summary output."""
    for name in ("SUMMARY_SCHEMA_PATH", "SUMMARY_SCHEMA"):
        if name in info.schema_vars or name in info.imported_schema_vars:
            return name
    return None


def _has_write_of_kind(
    info: BuilderInfo, kind: str
) -> WriteOp | None:
    """Find the last write operation of a given kind (highest line)."""
    matches = [op for op in info.write_ops if op.kind == kind]
    if not matches:
        return None
    return max(matches, key=lambda op: op.line)


def _validation_for_var(
    info: BuilderInfo, var_name: str
) -> ValidateCall | None:
    """Find the validate_records call that uses the given schema variable."""
    for vc in info.validate_calls:
        if vc.schema_var == var_name:
            return vc
    return None


def _schema_var_is_local(info: BuilderInfo, var_name: str) -> bool:
    """Check if a schema variable is defined locally (not just imported)."""
    return var_name in info.schema_vars


def _check_artifact(
    info: BuilderInfo,
    schemas_dir: Path,
    artifact_type: str,
    schema_var: str | None,
    write_op: WriteOp,
) -> Finding:
    """Produce a finding for a single artifact (main or summary)."""
    builder_name = info.name
    file_str = str(info.file_path)

    if not schema_var:
        label = "JSONL" if artifact_type == "main" else "summary"
        return Finding(
            builder=builder_name,
            file_path=file_str,
            artifact_type=artifact_type,
            status="missing_schema",
            details=f"Builder writes {label} but has no schema var defined",
            write_line=write_op.line,
        )

    # If schema var is imported (not locally assigned), we can check
    # validate_records but cannot verify the file on disk easily.
    if _schema_var_is_local(info, schema_var):
        assignment = info.schema_vars[schema_var]
        schema_file = schemas_dir / Path(assignment.schema_path_str).name
        if not schema_file.exists():
            return Finding(
                builder=builder_name,
                file_path=file_str,
                artifact_type=artifact_type,
                status="schema_file_missing",
                details=(
                    f"Schema file {schema_file.name}"
                    f" not found in {schemas_dir}"
                ),
                write_line=write_op.line,
            )

    vc = _validation_for_var(info, schema_var)
    if not vc:
        return Finding(
            builder=builder_name,
            file_path=file_str,
            artifact_type=artifact_type,
            status="missing_validation",
            details=(
                f"Schema defined ({schema_var}) but"
                " validate_records not called with it"
            ),
            write_line=write_op.line,
        )

    if vc.line > write_op.line:
        return Finding(
            builder=builder_name,
            file_path=file_str,
            artifact_type=artifact_type,
            status="wrong_order",
            details=(
                f"validate_records (line {vc.line})"
                f" called AFTER write (line {write_op.line})"
            ),
            validate_line=vc.line,
            write_line=write_op.line,
        )

    return Finding(
        builder=builder_name,
        file_path=file_str,
        artifact_type=artifact_type,
        status="ok",
        details=f"Schema {schema_var} validated before write",
        validate_line=vc.line,
        write_line=write_op.line,
    )


def verify_builder(
    info: BuilderInfo,
    schemas_dir: Path,
) -> list[Finding]:
    """Produce findings for a single builder."""
    findings: list[Finding] = []

    # Check main JSONL artifact
    jsonl_write = _has_write_of_kind(info, "jsonl")
    if jsonl_write:
        schema_var = _find_main_schema_var(info)
        findings.append(
            _check_artifact(
                info, schemas_dir, "main", schema_var, jsonl_write
            )
        )

    # Check summary artifact
    summary_write = _has_write_of_kind(info, "summary")
    if summary_write:
        schema_var = _find_summary_schema_var(info)
        findings.append(
            _check_artifact(
                info, schemas_dir, "summary", schema_var, summary_write
            )
        )

    return findings


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_builders(analytics_dir: Path) -> list[Path]:
    """Find all builder files in the analytics directory."""
    builders: list[Path] = []
    for py_file in sorted(analytics_dir.glob("*.py")):
        if py_file.name in HELPER_FILES:
            continue
        if not is_builder(py_file):
            continue
        builders.append(py_file)
    return builders


# ---------------------------------------------------------------------------
# Main audit
# ---------------------------------------------------------------------------


def audit_builders(
    analytics_dir: Path,
    schemas_dir: Path,
) -> list[Finding]:
    """Run the full audit and return all findings."""
    builder_files = discover_builders(analytics_dir)
    all_findings: list[Finding] = []

    for bf in builder_files:
        if bf.name in ALLOWLIST:
            all_findings.append(Finding(
                builder=bf.stem,
                file_path=str(bf),
                artifact_type="all",
                status="exempt",
                details=f"Allowlisted: {ALLOWLIST[bf.name]}",
            ))
            continue

        info = analyze_builder(bf)
        findings = verify_builder(info, schemas_dir)
        if not findings:
            all_findings.append(Finding(
                builder=bf.stem,
                file_path=str(bf),
                artifact_type="all",
                status="ok",
                details="No write operations detected in detailed analysis",
            ))
        else:
            all_findings.extend(findings)

    return all_findings


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _finding_to_dict(f: Finding) -> dict[str, str | int | None]:
    return {
        "builder": f.builder,
        "file_path": f.file_path,
        "artifact_type": f.artifact_type,
        "status": f.status,
        "details": f.details,
        "validate_line": f.validate_line,
        "write_line": f.write_line,
    }


def _print_table(findings: list[Finding], *, verbose: bool) -> None:
    if not verbose:
        findings = [f for f in findings if f.status != "ok"]

    if not findings:
        print("All builders pass schema validation audit.")
        return

    bw = max(len(f.builder) for f in findings)
    aw = max(len(f.artifact_type) for f in findings)
    sw = max(len(f.status) for f in findings)

    header = f"{'Builder':<{bw}}  {'Artifact':<{aw}}  {'Status':<{sw}}  Details"
    print(header)
    print("-" * len(header) + "-" * 40)

    for f in sorted(findings, key=lambda x: (x.status != "ok", x.builder)):
        line = (
            f"{f.builder:<{bw}}  {f.artifact_type:<{aw}}"
            f"  {f.status:<{sw}}  {f.details}"
        )
        if f.validate_line or f.write_line:
            parts = []
            if f.validate_line:
                parts.append(f"validate@L{f.validate_line}")
            if f.write_line:
                parts.append(f"write@L{f.write_line}")
            line += f"  [{', '.join(parts)}]"
        print(line)


def _has_failures(findings: list[Finding]) -> bool:
    return any(f.status not in ("ok", "exempt") for f in findings)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit schema validation coverage in analytics builders.",
    )
    parser.add_argument(
        "--scope",
        default="analytics",
        choices=["analytics"],
        help="Scope of the audit (default: analytics)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output structured JSON instead of table",
    )
    parser.add_argument(
        "--fail-on-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exit 1 on any gap (default: true)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show details for passing builders too",
    )
    parser.add_argument(
        "--analytics-dir",
        type=Path,
        default=ANALYTICS_DIR,
        help="Path to analytics source directory",
    )
    parser.add_argument(
        "--schemas-dir",
        type=Path,
        default=SCHEMAS_DIR,
        help="Path to schemas directory",
    )

    args = parser.parse_args()

    findings = audit_builders(
        analytics_dir=args.analytics_dir,
        schemas_dir=args.schemas_dir,
    )

    if args.json_output:
        output = {
            "scope": args.scope,
            "total_builders": len({f.builder for f in findings}),
            "total_findings": len(findings),
            "failures": sum(
                1
                for f in findings
                if f.status not in ("ok", "exempt")
            ),
            "findings": [_finding_to_dict(f) for f in findings],
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
    else:
        _print_table(findings, verbose=args.verbose)
        total = len({f.builder for f in findings})
        failures = sum(
            1 for f in findings if f.status not in ("ok", "exempt")
        )
        print(f"\n{total} builders audited, {failures} failure(s).")

    if args.fail_on_missing and _has_failures(findings):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
