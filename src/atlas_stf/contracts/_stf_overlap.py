"""STF cross-CSV overlap analysis: structural duplicates and process overlap.

Compares the 4 STF CSVs that form the backbone of decision events,
quantifying how much process overlap and event duplication exists
between them.  All file I/O streams via ``csv.DictReader`` — no pandas.

This report is **informational** — it does not feed into the serving layer
or trigger automatic deduplication.  The serving layer deduplicates by
primary key within each JSONL file (``_builder_utils._validate_inputs``),
but cross-CSV structural duplicates (same process+date+rapporteur in
different source files) receive distinct ``decision_event_id`` values and
are NOT deduplicated automatically.  This report quantifies the problem
to inform future cross-CSV dedup decisions in the curated layer.
"""

from __future__ import annotations

import contextlib
import csv
import json
import logging
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from statistics import median
from typing import Any

logger = logging.getLogger(__name__)

_STF_FILES: list[dict[str, str]] = [
    {
        "file": "decisoes.csv",
        "process_col": "processo",
        "date_col": "data_da_decisao",
        "rapporteur_col": "relator_atual",
    },
    {
        "file": "plenario_virtual.csv",
        "process_col": "processo",
        "date_col": "data_decisao",
        "rapporteur_col": "relator_atual",
    },
    {
        "file": "decisoes_covid.csv",
        "process_col": "processo",
        "date_col": "data_decisao",
        "rapporteur_col": "relator",
    },
    {
        "file": "distribuidos.csv",
        "process_col": "no_do_processo",
        "date_col": "data_do_andamento",
        "rapporteur_col": "ministro_a",
    },
]

_PROCESS_COL_MAP: dict[str, str] = {s["file"]: s["process_col"] for s in _STF_FILES}

_MAX_SAMPLE_SHARED = 10_000
_DUPLICATE_SAMPLE_SIZE = 5


@contextlib.contextmanager
def _open_csv(path: Path) -> Iterator[csv.DictReader[str]]:
    """Open a CSV trying utf-8, falling back to latin-1."""
    try:
        fh = open(path, encoding="utf-8", newline="")  # noqa: SIM115
        reader = csv.DictReader(fh, delimiter=",")
        if reader.fieldnames is None:
            fh.close()
            raise UnicodeDecodeError("utf-8", b"", 0, 1, "no header")
    except UnicodeDecodeError:
        fh = open(path, encoding="latin-1", newline="")  # noqa: SIM115
        reader = csv.DictReader(fh, delimiter=",")
    try:
        yield reader
    finally:
        fh.close()


def _pair_key(a: str, b: str) -> str:
    first, second = sorted([a.removesuffix(".csv"), b.removesuffix(".csv")])
    return f"{first}_vs_{second}"


def _load_process_sets(staging_dir: Path) -> dict[str, set[str]]:
    """Return ``{filename: set_of_process_numbers}`` for each STF file."""
    result: dict[str, set[str]] = {}
    for spec in _STF_FILES:
        path = staging_dir / spec["file"]
        if not path.exists():
            logger.warning("File not found, skipping: %s", path)
            continue
        processes: set[str] = set()
        col = spec["process_col"]
        with _open_csv(path) as reader:
            for row in reader:
                val = (row.get(col) or "").strip()
                if val:
                    processes.add(val)
        result[spec["file"]] = processes
    return result


def _load_event_keys(staging_dir: Path) -> dict[str, list[tuple[str, str, str]]]:
    """Return ``{filename: [(process, date, rapporteur), ...]}``."""
    result: dict[str, list[tuple[str, str, str]]] = {}
    for spec in _STF_FILES:
        path = staging_dir / spec["file"]
        if not path.exists():
            continue
        events: list[tuple[str, str, str]] = []
        p_col, d_col, r_col = spec["process_col"], spec["date_col"], spec["rapporteur_col"]
        with _open_csv(path) as reader:
            for row in reader:
                proc = (row.get(p_col) or "").strip()
                if not proc:
                    continue
                events.append((proc, (row.get(d_col) or "").strip(), (row.get(r_col) or "").strip()))
        result[spec["file"]] = events
    return result


def profile_stf_keys(staging_dir: Path) -> dict[str, Any]:
    """Per-file key statistics and cross-file unique process count."""
    all_processes: set[str] = set()
    profiles: dict[str, Any] = {}

    for spec in _STF_FILES:
        path = staging_dir / spec["file"]
        if not path.exists():
            logger.warning("File not found, skipping profile: %s", path)
            continue
        p_col, d_col, r_col = spec["process_col"], spec["date_col"], spec["rapporteur_col"]
        proc_counts: dict[str, int] = defaultdict(int)
        rapporteurs: set[str] = set()
        dates: list[str] = []
        total_rows = 0

        with _open_csv(path) as reader:
            for row in reader:
                total_rows += 1
                proc = (row.get(p_col) or "").strip()
                if proc:
                    proc_counts[proc] += 1
                rapp = (row.get(r_col) or "").strip()
                if rapp:
                    rapporteurs.add(rapp)
                dt = (row.get(d_col) or "").strip()
                if dt:
                    dates.append(dt)

        all_processes.update(proc_counts)
        counts = sorted(proc_counts.values()) if proc_counts else [0]
        profile: dict[str, Any] = {
            "total_rows": total_rows,
            "unique_processes": len(proc_counts),
            "rows_per_process_mean": round(sum(counts) / max(len(counts), 1), 2),
            "rows_per_process_max": max(counts),
            "rows_per_process_median": round(median(counts), 2),
            "unique_rapporteurs": len(rapporteurs),
        }
        if len(rapporteurs) < 50:
            profile["rapporteur_list"] = sorted(rapporteurs)
        sorted_dates = sorted(dates) if dates else []
        if sorted_dates:
            profile["date_range"] = [sorted_dates[0], sorted_dates[-1]]
        profiles[spec["file"]] = profile

    return {
        "per_file_profile": profiles,
        "cross_file_unique_processes": len(all_processes),
    }


def _pairwise_overlap(
    process_sets: dict[str, set[str]],
    event_keys: dict[str, list[tuple[str, str, str]]],
) -> dict[str, Any]:
    """Compute pairwise process overlap and rapporteur conflicts."""
    pairwise: dict[str, Any] = {}
    for file_a, file_b in combinations(sorted(process_sets), 2):
        set_a, set_b = process_sets[file_a], process_sets[file_b]
        shared = set_a & set_b
        only_a, only_b = len(set_a - set_b), len(set_b - set_a)
        union_size = len(shared) + only_a + only_b
        # Detect key format mismatch (e.g. "processo" vs "no_do_processo")
        col_a = _PROCESS_COL_MAP.get(file_a, "")
        col_b = _PROCESS_COL_MAP.get(file_b, "")
        key_mismatch = col_a != col_b
        match_status = "key_format_mismatch" if key_mismatch else "matched"
        pair_info: dict[str, Any] = {
            "shared_processes": len(shared),
            f"only_in_{file_a.removesuffix('.csv')}": only_a,
            f"only_in_{file_b.removesuffix('.csv')}": only_b,
            "overlap_rate": round(len(shared) / union_size, 4) if union_size else 0.0,
            "match_status": match_status,
            "process_column_a": col_a,
            "process_column_b": col_b,
        }
        # Event-level rapporteur comparison on sampled shared processes
        events_a, events_b = event_keys.get(file_a, []), event_keys.get(file_b, [])
        if events_a and events_b and shared:
            sampled_set = set(sorted(shared)[:_MAX_SAMPLE_SHARED])
            rapp_a: dict[str, set[str]] = defaultdict(set)
            rapp_b: dict[str, set[str]] = defaultdict(set)
            for proc, _dt, rapp in events_a:
                if proc in sampled_set and rapp:
                    rapp_a[proc].add(rapp)
            for proc, _dt, rapp in events_b:
                if proc in sampled_set and rapp:
                    rapp_b[proc].add(rapp)
            compared = conflicts = 0
            for proc in sampled_set:
                ra, rb = rapp_a.get(proc, set()), rapp_b.get(proc, set())
                if ra and rb:
                    compared += 1
                    if ra != rb:
                        conflicts += 1
            pair_info["event_comparison"] = {
                "shared_events_sampled": compared,
                "rapporteur_conflicts": conflicts,
                "rapporteur_conflict_rate": round(conflicts / compared, 4) if compared else 0.0,
            }
        pairwise[_pair_key(file_a, file_b)] = pair_info
    return pairwise


def _find_structural_duplicates(
    event_keys: dict[str, list[tuple[str, str, str]]],
) -> dict[str, Any]:
    """Find records with same (process, date, rapporteur) across files."""
    key_to_files: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for fname, events in event_keys.items():
        seen: set[tuple[str, str, str]] = set()
        for key in events:
            if key not in seen:
                seen.add(key)
                key_to_files[key].append(fname)
    duplicates = {k: v for k, v in key_to_files.items() if len(v) > 1}
    sample = [
        {"process": k[0], "date": k[1], "rapporteur": k[2], "files": sorted(v)}
        for k, v in sorted(duplicates.items())[:_DUPLICATE_SAMPLE_SIZE]
    ]
    return {"total_candidate_count": len(duplicates), "sample": sample}


def analyze_stf_overlap(staging_dir: Path) -> dict[str, Any]:
    """Produce the full overlap report across STF CSVs."""
    process_sets = _load_process_sets(staging_dir)
    event_keys = _load_event_keys(staging_dir)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "staging_dir": str(staging_dir),
        "pairwise_overlap": _pairwise_overlap(process_sets, event_keys),
        "structural_duplicates": _find_structural_duplicates(event_keys),
        **profile_stf_keys(staging_dir),
    }


def write_overlap_report(report: dict[str, Any], output_path: Path) -> Path:
    """Write the overlap report as pretty-printed JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Overlap report written to %s", output_path)
    return output_path
