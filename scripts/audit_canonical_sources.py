#!/usr/bin/env python3
"""Audit canonical source usage across analytics builders.

Detects:
- Concepts reading from non-canonical sources (e.g., process_class from decision_event)
- Local reimplementations of canonical helpers
- Prohibited patterns defined in the integrity manifest

Returns exit code 1 on any critical violation.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "audit" / "contracts" / "integrity_manifest.json"
ANALYTICS_DIR = REPO_ROOT / "src" / "atlas_stf" / "analytics"


@dataclass
class Violation:
    severity: str
    rule: str
    file: str
    line: int
    message: str


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _scan_prohibited_patterns(manifest: dict) -> list[Violation]:
    """Scan analytics builders for prohibited patterns from concepts."""
    violations: list[Violation] = []
    concepts = manifest.get("concepts", {})

    for concept_name, concept in concepts.items():
        for pp in concept.get("prohibited_patterns", []):
            pattern = re.compile(pp["pattern"])
            raw_exceptions = pp.get("exceptions", [])
            context = pp.get("context", "analytics builders")

            # Parse exceptions: support both old format (string list) and
            # new format (list of dicts with file, context_pattern, reason)
            file_exceptions: set[str] = set()
            contextual_exceptions: list[dict] = []
            for exc in raw_exceptions:
                if isinstance(exc, str):
                    file_exceptions.add(exc)
                elif isinstance(exc, dict):
                    if "context_pattern" in exc:
                        contextual_exceptions.append(exc)
                    else:
                        file_exceptions.add(exc["file"])

            if context == "analytics builders":
                scan_dir = ANALYTICS_DIR
            else:
                scan_dir = REPO_ROOT / "src" / "atlas_stf"

            for py_file in sorted(scan_dir.rglob("*.py")):
                if py_file.name in file_exceptions:
                    continue
                if py_file.name.startswith("test_"):
                    continue

                try:
                    lines = py_file.read_text(encoding="utf-8").splitlines()
                except OSError:
                    continue

                for i, line in enumerate(lines, 1):
                    if pattern.search(line):
                        stripped = line.lstrip()
                        if stripped.startswith("#"):
                            continue

                        # Check contextual exceptions
                        is_excepted = False
                        for ce in contextual_exceptions:
                            if ce["file"] != py_file.name:
                                continue
                            ctx_re = re.compile(ce["context_pattern"])
                            if ctx_re.search(line):
                                is_excepted = True
                                break
                        if is_excepted:
                            continue

                        violations.append(Violation(
                            severity=concept.get("severity", "medium"),
                            rule=f"prohibited_pattern:{concept_name}",
                            file=str(py_file.relative_to(REPO_ROOT)),
                            line=i,
                            message=f"{pp['reason']} (pattern: {pp['pattern']})",
                        ))

    return violations


def _scan_helper_reimplementations(manifest: dict) -> list[Violation]:
    """Detect local reimplementations of canonical helpers."""
    violations: list[Violation] = []
    helpers = manifest.get("canonical_helpers", {})

    for helper_name, helper_def in helpers.items():
        for reimpl in helper_def.get("prohibited_reimplementations", []):
            pattern = re.compile(reimpl["pattern"], re.DOTALL)
            exceptions = set(reimpl.get("exceptions", []))

            for py_file in sorted(ANALYTICS_DIR.rglob("*.py")):
                if py_file.name.startswith("test_"):
                    continue
                basename = py_file.name
                # Check if any exception matches the filename or a function name
                skip = False
                for exc in exceptions:
                    if exc == basename or exc in py_file.read_text(encoding="utf-8"):
                        skip = True
                        break
                if skip:
                    continue

                try:
                    content = py_file.read_text(encoding="utf-8")
                    lines = content.splitlines()
                except OSError:
                    continue

                # Check for multiline pattern across sliding windows
                for i, line in enumerate(lines, 1):
                    if pattern.search(line):
                        stripped = line.lstrip()
                        if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'"):
                            continue
                        violations.append(Violation(
                            severity="medium",
                            rule=f"reimplementation:{helper_name}",
                            file=str(py_file.relative_to(REPO_ROOT)),
                            line=i,
                            message=reimpl["description"],
                        ))

    return violations


def _scan_source_files(manifest: dict) -> list[Violation]:
    """Detect concepts reading from prohibited source files."""
    violations: list[Violation] = []
    concepts = manifest.get("concepts", {})

    for concept_name, concept in concepts.items():
        prohibited = concept.get("prohibited_sources", [])
        for ps in prohibited:
            prohibited_file = ps["file"]
            # Search for reading this file in analytics builders
            pattern = re.compile(rf'["\'].*{re.escape(prohibited_file)}["\']')

            for py_file in sorted(ANALYTICS_DIR.rglob("*.py")):
                if py_file.name.startswith("test_"):
                    continue
                try:
                    lines = py_file.read_text(encoding="utf-8").splitlines()
                except OSError:
                    continue

                for i, line in enumerate(lines, 1):
                    stripped = line.lstrip()
                    if stripped.startswith("#"):
                        continue
                    if pattern.search(line):
                        # Check if this file is being used for the prohibited concept
                        # Look for nearby process_class usage
                        context_start = max(0, i - 5)
                        context_end = min(len(lines), i + 5)
                        context_text = "\n".join(lines[context_start:context_end])
                        # Require direct field access pattern, not just substring match
                        field = concept.get("canonical_field", "")
                        esc = re.escape(field)
                        access_pattern = re.compile(rf'\.get\(["\']{ esc}["\']\)|["\']{ esc}["\']')
                        if field and access_pattern.search(context_text):
                            violations.append(Violation(
                                severity=concept.get("severity", "medium"),
                                rule=f"prohibited_source:{concept_name}",
                                file=str(py_file.relative_to(REPO_ROOT)),
                                line=i,
                                message=f"{ps['reason']}",
                            ))

    return violations


def run_audit(manifest: dict | None = None) -> list[Violation]:
    """Run all canonical source checks. Returns list of violations."""
    if manifest is None:
        manifest = load_manifest()

    violations: list[Violation] = []
    violations.extend(_scan_prohibited_patterns(manifest))
    violations.extend(_scan_helper_reimplementations(manifest))
    violations.extend(_scan_source_files(manifest))
    return violations


def main() -> int:
    manifest = load_manifest()
    violations = run_audit(manifest)

    if not violations:
        print("✓ Canonical source audit: no violations found")
        return 0

    critical_count = 0
    for v in sorted(violations, key=lambda x: (x.severity, x.file, x.line)):
        marker = "✗" if v.severity == "critical" else "⚠"
        print(f"  {marker} [{v.severity}] {v.file}:{v.line} — {v.rule}: {v.message}")
        if v.severity == "critical":
            critical_count += 1

    print(f"\nCanonical source audit: {len(violations)} violations ({critical_count} critical)")
    return 1 if critical_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
