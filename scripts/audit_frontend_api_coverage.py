#!/usr/bin/env python3
"""Audit frontend↔API coverage for endpoint groups defined in the manifest.

Maps each backend endpoint to its fetcher, page, and component in the frontend.
Fails when a required endpoint group has gaps in coverage.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "audit" / "contracts" / "integrity_manifest.json"
ROUTES_DIR = REPO_ROOT / "src" / "atlas_stf" / "api"
WEB_LIB_DIR = REPO_ROOT / "web" / "src" / "lib"
WEB_APP_DIR = REPO_ROOT / "web" / "src" / "app"


@dataclass
class EndpointCoverage:
    group: str
    path: str
    method: str
    fetcher: str | None
    page: str | None
    manifest_coverage: str  # ok, indirect, fetcher_only, not_required
    actual_fetcher_found: bool
    actual_page_found: bool
    status: str  # ok, indirect, fetcher_only, missing, not_required


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _search_in_files(
    directory: Path, pattern: str, extensions: tuple[str, ...] = (".ts", ".tsx"),
) -> list[tuple[Path, int]]:
    """Search for pattern in files. Returns list of (file, line_number)."""
    results: list[tuple[Path, int]] = []
    regex = re.compile(pattern)
    for f in sorted(directory.rglob("*")):
        if f.suffix not in extensions:
            continue
        try:
            for i, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
                if regex.search(line):
                    results.append((f, i))
        except OSError:
            pass
    return results


def _verify_fetcher(fetcher_name: str | None) -> bool:
    """Check if fetcher function exists in web/src/lib/."""
    if not fetcher_name:
        return False
    return len(_search_in_files(WEB_LIB_DIR, rf"export\s+(async\s+)?function\s+{fetcher_name}\b")) > 0


def _verify_page(page_path: str | None) -> bool:
    """Check if a Next.js page exists for the given route."""
    if not page_path:
        return False
    # Convert route to filesystem path
    # /investigacao → web/src/app/investigacao/page.tsx
    # /investigacao/[entityId] → web/src/app/investigacao/[entityId]/page.tsx
    fs_path = WEB_APP_DIR / page_path.lstrip("/") / "page.tsx"
    return fs_path.exists()


def run_audit(manifest: dict | None = None) -> tuple[list[EndpointCoverage], bool]:
    """Run frontend↔API coverage audit. Returns (results, has_failure)."""
    if manifest is None:
        manifest = load_manifest()

    fc = manifest.get("frontend_coverage", {})
    groups = fc.get("groups", {})
    results: list[EndpointCoverage] = []
    has_failure = False

    for group_name, group_def in groups.items():
        required = group_def.get("required_coverage", False)
        endpoints = group_def.get("endpoints", [])

        for ep in endpoints:
            path = ep["path"]
            method = ep.get("method", "GET")
            fetcher = ep.get("fetcher")
            page = ep.get("page")
            # Use new ui_requirement field, fallback to old coverage field
            ui_req = ep.get("ui_requirement", ep.get("coverage", "required"))

            actual_fetcher = _verify_fetcher(fetcher)
            actual_page = _verify_page(page)

            # Determine actual status
            if ui_req == "not_required":
                status = "not_required"
            elif ui_req == "indirect_ok":
                status = "indirect"
            elif ui_req == "fetcher_only_ok":
                status = "fetcher_only" if actual_fetcher else "missing"
            elif actual_fetcher and actual_page:
                status = "ok"
            elif actual_fetcher and not actual_page:
                status = "fetcher_only"
            elif not actual_fetcher and actual_page:
                status = "page_without_fetcher"
            else:
                status = "missing"

            # Apply policy: required endpoints MUST have full coverage
            if ui_req == "required" and status != "ok":
                has_failure = True
            elif required and status == "missing":
                has_failure = True

            results.append(EndpointCoverage(
                group=group_name,
                path=path,
                method=method,
                fetcher=fetcher,
                page=page,
                manifest_coverage=ui_req,
                actual_fetcher_found=actual_fetcher,
                actual_page_found=actual_page,
                status=status,
            ))

    return results, has_failure


def main() -> int:
    manifest = load_manifest()
    results, has_failure = run_audit(manifest)

    current_group = ""
    for r in results:
        if r.group != current_group:
            print(f"\n  [{r.group}]")
            current_group = r.group

        markers = {
            "ok": "✓",
            "indirect": "≈",
            "fetcher_only": "◐",
            "not_required": "⊘",
            "missing": "✗",
            "page_without_fetcher": "✗",
        }
        marker = markers.get(r.status, "?")
        parts = [f"{marker} {r.method:4s} {r.path:40s} → {r.status}"]
        if r.fetcher:
            parts.append(f"fetcher={'✓' if r.actual_fetcher_found else '✗'} {r.fetcher}")
        if r.page:
            parts.append(f"page={'✓' if r.actual_page_found else '✗'} {r.page}")
        print("    " + "  ".join(parts))

    ok_count = sum(1 for r in results if r.status == "ok")
    total = len(results)
    print(f"\nFrontend↔API coverage: {ok_count}/{total} fully covered, {'FAILED' if has_failure else 'PASSED'}")
    return 1 if has_failure else 0


if __name__ == "__main__":
    sys.exit(main())
