"""E4 coverage metadata: field availability and known coverage gaps.

Reads per-year TSE inventories and the full observed/ tree to produce
``_coverage_metadata.json`` — a machine-readable map of which fields are
available per year and which year ranges are absent by design or limitation.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..ingest_manifest import normalize_header_for_signature

FIELD_STATUS = {
    "source_absent": "Field does not exist in source file for this generation/year",
    "design_excluded": "Data exists in source but deliberately not ingested",
    "source_present_low_reliability": "Field exists but null+empty > 50% — not suitable for strong join",
    "parser_output_missing": "Field exists in source but not preserved by parser",
    "parser_semantic_gap": "Parser does not preserve absent vs empty distinction",
}

STATUS_ORIGIN = {
    "observed_from_raw": "Status determined by inspection of raw file",
    "design_decision": "Status by explicit design decision documented in code",
    "parser_limitation": "Status by known parser limitation",
}

# Known design gaps — static, version-controlled
_TSE_ALL_YEARS = [str(y) for y in range(2002, 2026, 2)]
_TSE_EXPENSES_SUPPORTED = {"2002", "2004", "2006", "2008", "2010", "2022", "2024"}
_TSE_EXPENSES_MISSING = sorted(set(_TSE_ALL_YEARS) - _TSE_EXPENSES_SUPPORTED)

_DESIGN_GAPS: list[dict[str, Any]] = [
    {
        "source": "tse",
        "scope": "campaign_expenses",
        "years_missing": _TSE_EXPENSES_MISSING,
        "status": "design_excluded",
        "origin_of_status": "design_decision",
        "blocking": True,
        "explanation": (
            "Years 2012-2020 not supported: 2018 only has despesas_pagas "
            "(no candidate ID), 2012/2014/2016/2020 not inspected"
        ),
        "join_impact": "coverage_gap_blocking",
    },
]

_LOW_RELIABILITY_THRESHOLD = 0.50


def _read_inventory(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[return-value]
    except OSError, json.JSONDecodeError:
        return None


def _is_low_reliability(col: dict[str, Any]) -> bool:
    null_rate = col.get("null_rate") or 0.0
    empty_rate = col.get("empty_rate") or 0.0
    return (null_rate + empty_rate) > _LOW_RELIABILITY_THRESHOLD


def _layout_signature_from_inventory(inv: dict[str, Any]) -> str:
    """Compute layout_signature from an inventory's column names."""
    col_names = [c["observed_column_name"] for c in inv.get("columns", [])]
    return normalize_header_for_signature(col_names) if col_names else ""


def _field_availability_from_inventory(
    inv: dict[str, Any],
    file_label: str,
) -> list[dict[str, Any]]:
    """Produce field_availability entries for columns that are absent or low-reliability."""
    entries: list[dict[str, Any]] = []
    source = inv.get("source", "")
    year_or_cycle = inv.get("year_or_cycle", "")
    layout_sig = _layout_signature_from_inventory(inv)

    for col in inv.get("columns", []):
        if _is_low_reliability(col):
            entries.append(
                {
                    "source": source,
                    "file_name": file_label,
                    "year_or_cycle": year_or_cycle,
                    "layout_signature": layout_sig,
                    "field": col["observed_column_name"],
                    "status": "source_present_low_reliability",
                    "origin_of_status": "observed_from_raw",
                }
            )

    return entries


def _field_availability_from_by_year(
    by_year_dir: Path,
) -> list[dict[str, Any]]:
    """Read all per-year inventories and emit field_availability entries."""
    entries: list[dict[str, Any]] = []

    if not by_year_dir.is_dir():
        return entries

    for path in sorted(by_year_dir.glob("*.json")):
        inv = _read_inventory(path)
        if inv is None:
            continue

        # Derive canonical file label: donations_raw_YYYY.json → donations_raw.jsonl
        stem = path.stem  # e.g. donations_raw_2002
        parts = stem.rsplit("_", 1)
        file_label = f"{parts[0]}.jsonl" if len(parts) == 2 else f"{stem}.jsonl"

        entries.extend(_field_availability_from_inventory(inv, file_label))

    return entries


def build_coverage_metadata(
    observed_dir: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Build the coverage metadata document.

    Reads per-year TSE inventories from ``observed_dir/tse/by_year/``
    and the full inventory tree under ``observed_dir``.  Returns a dict
    ready for JSON serialisation.
    """
    field_availability: list[dict[str, Any]] = []

    # Per-year TSE inventories
    tse_by_year = observed_dir / "tse" / "by_year"
    field_availability.extend(_field_availability_from_by_year(tse_by_year))

    # All other top-level inventories (non-by_year)
    if observed_dir.is_dir():
        for path in sorted(observed_dir.rglob("*.json")):
            # Skip cross-file report, coverage metadata itself, and by_year files
            if path.name.startswith("_"):
                continue
            if "by_year" in path.parts:
                continue
            inv = _read_inventory(path)
            if inv is None:
                continue
            file_label = inv.get("file_name", path.name)
            field_availability.extend(_field_availability_from_inventory(inv, file_label))

    return {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "reconciliation_keys": ["source", "file_name", "year_or_cycle", "layout_signature"],
        "field_availability": field_availability,
        "coverage_gaps": _DESIGN_GAPS,
    }


def write_coverage_metadata(metadata: dict[str, Any], output_dir: Path) -> Path:
    """Serialise *metadata* to ``output_dir/_coverage_metadata.json``."""
    path = output_dir / "_coverage_metadata.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
