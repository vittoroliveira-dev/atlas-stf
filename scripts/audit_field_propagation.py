#!/usr/bin/env python3
"""Audit field propagation chains defined in the integrity manifest.

For each critical field, verifies that it appears at every layer of
the propagation chain: analytics → schema → serving → API → frontend.

Returns exit code 1 when a critical field chain is broken.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "audit" / "contracts" / "integrity_manifest.json"


@dataclass
class PropagationResult:
    concept: str
    severity: str
    chain: list[dict]
    present: list[str]
    missing: list[str]
    details: list[str]


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _check_field_in_file(repo_root: Path, rel_path: str, field_name: str) -> tuple[bool, int]:
    """Check if field_name appears in the file. Returns (found, line_number)."""
    # Resolve relative paths that may be under src/ or web/
    candidates = [
        repo_root / rel_path,
        repo_root / "src" / "atlas_stf" / rel_path,
    ]
    for full_path in candidates:
        if full_path.exists():
            try:
                lines = full_path.read_text(encoding="utf-8").splitlines()
                for i, line in enumerate(lines, 1):
                    if field_name in line:
                        return True, i
            except OSError:
                pass
    return False, 0


def run_audit(manifest: dict | None = None) -> list[PropagationResult]:
    """Check propagation chains for all concepts with chains defined."""
    if manifest is None:
        manifest = load_manifest()

    results: list[PropagationResult] = []
    concepts = manifest.get("concepts", {})

    for concept_name, concept in concepts.items():
        chain = concept.get("propagation_chain")
        if not chain:
            continue

        severity = concept.get("severity", "medium")
        present: list[str] = []
        missing: list[str] = []
        details: list[str] = []

        for step in chain:
            layer = step["layer"]
            rel_file = step["file"]
            field = step["field"]
            found, line_no = _check_field_in_file(REPO_ROOT, rel_file, field)

            if found:
                present.append(layer)
                details.append(f"  ✓ {layer}: {rel_file}:{line_no} — '{field}' found")
            else:
                missing.append(layer)
                details.append(f"  ✗ {layer}: {rel_file} — '{field}' NOT FOUND")

        results.append(PropagationResult(
            concept=concept_name,
            severity=severity,
            chain=chain,
            present=present,
            missing=missing,
            details=details,
        ))

    return results


def main() -> int:
    manifest = load_manifest()
    results = run_audit(manifest)

    if not results:
        print("✓ Field propagation audit: no chains to check")
        return 0

    has_critical_break = False
    for r in results:
        if r.missing:
            status = "BROKEN"
            if r.severity == "critical":
                has_critical_break = True
        else:
            status = "OK"

        print(f"\n{'✗' if r.missing else '✓'} [{r.severity}] {r.concept}: {status}")
        print(f"  Chain: {' → '.join(s['layer'] for s in r.chain)}")
        if r.missing:
            print(f"  Missing: {', '.join(r.missing)}")
        for detail in r.details:
            print(detail)

    total_broken = sum(1 for r in results if r.missing)
    print(f"\nField propagation audit: {len(results)} chains checked, {total_broken} broken")
    return 1 if has_critical_break else 0


if __name__ == "__main__":
    sys.exit(main())
