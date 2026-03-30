#!/usr/bin/env python3
"""Audit builder runtime: execute critical builders on a real data sample
in a temporary directory and verify they produce valid output.

NOT a unit test — this runs actual builders against real (sampled) data
to detect "green but empty", broken joins, and stale dependencies.

Returns exit code 1 on critical builder failure.
"""

from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CURATED_DIR = REPO_ROOT / "data" / "curated"
SAMPLE_LINES = 2000  # Lines per JSONL in sample

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger("audit_builder_runtime")

# Builders to exercise, in dependency order
BUILDERS: list[dict] = [
    {
        "name": "build_groups",
        "module": "atlas_stf.analytics.build_groups",
        "function": "build_groups",
        "kwargs_template": {
            "process_path": "{curated}/process.jsonl",
            "decision_event_path": "{curated}/decision_event.jsonl",
            "output_dir": "{output}",
        },
        "expected_outputs": ["comparison_group.jsonl", "decision_event_group_link.jsonl"],
        "severity": "critical",
    },
    {
        "name": "baseline",
        "module": "atlas_stf.analytics.baseline",
        "function": "build_baseline",
        "kwargs_template": {
            "comparison_group_path": "{output}/comparison_group.jsonl",
            "link_path": "{output}/decision_event_group_link.jsonl",
            "decision_event_path": "{curated}/decision_event.jsonl",
            "output_path": "{output}/baseline.jsonl",
            "summary_path": "{output}/baseline_summary.json",
        },
        "expected_outputs": ["baseline.jsonl"],
        "severity": "critical",
        "depends_on": ["build_groups"],
    },
    {
        "name": "rapporteur_profile",
        "module": "atlas_stf.analytics.rapporteur_profile",
        "function": "build_rapporteur_profiles",
        "kwargs_template": {
            "decision_event_path": "{curated}/decision_event.jsonl",
            "process_path": "{curated}/process.jsonl",
            "output_dir": "{output}",
        },
        "expected_outputs": ["rapporteur_profile.jsonl"],
        "severity": "high",
    },
    {
        "name": "assignment_audit",
        "module": "atlas_stf.analytics.assignment_audit",
        "function": "build_assignment_audit",
        "kwargs_template": {
            "decision_event_path": "{curated}/decision_event.jsonl",
            "process_path": "{curated}/process.jsonl",
            "output_dir": "{output}",
        },
        "expected_outputs": ["assignment_audit.jsonl"],
        "severity": "high",
    },
    {
        "name": "counsel_network",
        "module": "atlas_stf.analytics.counsel_network",
        "function": "build_counsel_network",
        "kwargs_template": {"curated_dir": "{curated}", "output_dir": "{output}"},
        "expected_outputs": ["counsel_network_cluster.jsonl"],
        "severity": "high",
    },
]


def _sample_curated(src_dir: Path, dst_dir: Path, max_lines: int) -> dict[str, int]:
    """Copy sampled curated JSONLs to dst_dir. Returns {filename: lines_copied}."""
    dst_dir.mkdir(parents=True, exist_ok=True)
    stats: dict[str, int] = {}
    for src_file in sorted(src_dir.glob("*.jsonl")):
        lines: list[str] = []
        with src_file.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    lines.append(line)
                    if len(lines) >= max_lines:
                        break
        dst_file = dst_dir / src_file.name
        dst_file.write_text("".join(lines), encoding="utf-8")
        stats[src_file.name] = len(lines)
    # Also copy non-JSONL files (e.g., minister_bio.json)
    for src_file in sorted(src_dir.glob("*.json")):
        shutil.copy2(src_file, dst_dir / src_file.name)
    return stats


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _classify_empty(builder_name: str, output_count: int, input_counts: dict[str, int]) -> str:
    """Classify why a builder produced zero output."""
    if output_count > 0:
        return "OK"
    # Check if inputs were empty
    relevant_inputs = {k: v for k, v in input_counts.items() if v == 0}
    if relevant_inputs:
        return f"EMPTY_NO_INPUT ({', '.join(relevant_inputs)})"
    return "EMPTY_UNEXPECTED"


def run_audit(sample_lines: int = SAMPLE_LINES) -> tuple[list[dict], bool]:
    """Run builders on sampled data. Returns (results, has_critical_failure)."""
    if not CURATED_DIR.exists():
        return [{"name": "*", "status": "SKIP", "reason": "curated dir not found"}], False

    results: list[dict] = []
    has_critical = False
    completed: set[str] = set()

    with tempfile.TemporaryDirectory(prefix="atlas_audit_") as tmpdir:
        tmp = Path(tmpdir)
        curated_sample = tmp / "curated"
        output_dir = tmp / "analytics"
        output_dir.mkdir()

        # Create sample
        sample_stats = _sample_curated(CURATED_DIR, curated_sample, sample_lines)

        for builder in BUILDERS:
            name = builder["name"]
            severity = builder["severity"]
            depends = builder.get("depends_on", [])

            # Check dependencies
            missing_deps = [d for d in depends if d not in completed]
            if missing_deps:
                results.append(
                    {
                        "name": name,
                        "status": "SKIP_DEP",
                        "severity": severity,
                        "reason": f"dependency not met: {missing_deps}",
                        "output_lines": 0,
                    }
                )
                continue

            # Resolve kwargs
            kwargs: dict = {}
            for k, v in builder["kwargs_template"].items():
                resolved = str(v).replace("{curated}", str(curated_sample)).replace("{output}", str(output_dir))
                # Convert path strings to Path objects
                if "/" in resolved or "\\" in resolved:
                    kwargs[k] = Path(resolved)
                else:
                    kwargs[k] = resolved

            # Execute
            start = time.monotonic()
            try:
                import importlib

                mod = importlib.import_module(builder["module"])
                fn = getattr(mod, builder["function"])
                fn(**kwargs)
                elapsed = time.monotonic() - start

                # Check outputs
                total_output = 0
                output_details: list[str] = []
                for expected in builder["expected_outputs"]:
                    out_path = output_dir / expected
                    count = _count_lines(out_path)
                    total_output += count
                    output_details.append(f"{expected}: {count}")

                classification = _classify_empty(name, total_output, sample_stats)

                status = "OK" if total_output > 0 else classification
                if status.startswith("EMPTY_UNEXPECTED") and severity == "critical":
                    has_critical = True

                completed.add(name)
                results.append(
                    {
                        "name": name,
                        "status": status,
                        "severity": severity,
                        "output_lines": total_output,
                        "outputs": output_details,
                        "elapsed_s": round(elapsed, 1),
                    }
                )

            except Exception as exc:
                elapsed = time.monotonic() - start
                results.append(
                    {
                        "name": name,
                        "status": "FAILED",
                        "severity": severity,
                        "reason": str(exc)[:200],
                        "output_lines": 0,
                        "elapsed_s": round(elapsed, 1),
                    }
                )
                if severity == "critical":
                    has_critical = True

    return results, has_critical


def main() -> int:
    print("Builder runtime audit (sampled curated data)")
    print(f"Sample size: {SAMPLE_LINES} lines per JSONL\n")

    results, has_critical = run_audit()

    for r in results:
        marker = (
            "✓"
            if r["status"] == "OK"
            else "✗"
            if r["status"] in ("FAILED",) or r["status"].startswith("EMPTY_UNEXPECTED")
            else "⊘"
        )
        line = f"  {marker} [{r['severity']}] {r['name']}: {r['status']}"
        if "elapsed_s" in r:
            line += f" ({r['elapsed_s']}s)"
        if "outputs" in r:
            line += f" — {', '.join(r['outputs'])}"
        if "reason" in r:
            line += f" — {r['reason']}"
        print(line)

    ok_count = sum(1 for r in results if r["status"] == "OK")
    print(f"\nBuilder runtime: {ok_count}/{len(results)} OK")
    print(f"Verdict: {'BLOCKED' if has_critical else 'PASSED'}")
    return 1 if has_critical else 0


if __name__ == "__main__":
    sys.exit(main())
