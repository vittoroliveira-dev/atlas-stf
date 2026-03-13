"""Build origin context analytics from raw DataJud data + curated processes."""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.origin_mapping import (
    map_origin_to_datajud_indices,
    normalize_state_description,
)

logger = logging.getLogger(__name__)

DEFAULT_DATAJUD_DIR = Path("data/raw/datajud")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _count_stf_by_index(process_path: Path) -> Counter[str]:
    """Count STF processes per DataJud index from curated process data."""
    counter: Counter[str] = Counter()
    for record in _read_jsonl(process_path):
        court = record.get("origin_court_or_body")
        state = record.get("origin_description")
        indices = map_origin_to_datajud_indices(court, state)
        for index in indices:
            counter[index] += 1
    return counter


def _uf_from_index(index: str) -> str | None:
    """Extract UF from index name heuristically."""
    name = index.removeprefix("api_publica_")
    if name.startswith("tj"):
        suffix = name[2:].upper()
        if suffix == "DFT":
            return "DF"
        return suffix if len(suffix) == 2 else None
    return None


def build_origin_context(
    *,
    datajud_dir: Path = DEFAULT_DATAJUD_DIR,
    process_path: Path = DEFAULT_PROCESS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Consolidate DataJud raw data + STF process counts into origin_context analytics."""
    output_dir.mkdir(parents=True, exist_ok=True)
    stf_counts = _count_stf_by_index(process_path)

    datajud_files = sorted(datajud_dir.glob("api_publica_*.json"))
    if not datajud_files:
        logger.warning("No DataJud files found in %s", datajud_dir)
        return output_dir

    records: list[dict[str, Any]] = []
    for path in datajud_files:
        data = _read_json(path)
        index = data.get("index", path.stem)
        total = data.get("total_processes", 0)
        stf_count = stf_counts.get(index, 0)
        share_pct = round(stf_count / total * 100, 4) if total > 0 else 0.0

        record: dict[str, Any] = {
            "origin_index": index,
            "tribunal_label": data.get("tribunal_label", index),
            "state": _uf_from_index(index) or normalize_state_description(data.get("tribunal_label")) or "",
            "datajud_total_processes": total,
            "stf_process_count": stf_count,
            "stf_share_pct": share_pct,
            "top_assuntos": data.get("top_assuntos", []),
            "top_orgaos_julgadores": data.get("top_orgaos_julgadores", []),
            "class_distribution": data.get("class_distribution", []),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

    output_path = output_dir / "origin_context.jsonl"
    with output_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary = {
        "origin_count": len(records),
        "total_datajud_processes": sum(r["datajud_total_processes"] for r in records),
        "total_stf_mapped": sum(r["stf_process_count"] for r in records),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    summary_path = output_dir / "origin_context_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Built origin context: %d indices, %d total DataJud processes",
        len(records),
        summary["total_datajud_processes"],
    )
    return output_path
