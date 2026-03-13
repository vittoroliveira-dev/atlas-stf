"""Shared helpers for curated-layer builders.

Domain logic (identity, parsers, rules) lives in atlas_stf.core.
This module provides I/O helpers and re-exports core symbols for convenience.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import (
    infer_process_class_from_number,
    normalize_entity_name,
    stable_id,
)
from ..core.parsers import (
    as_optional_str,
    counsel_entries_from_juris_partes,
    first_non_null,
    infer_process_number,
    is_missing,
    party_entries_from_juris_partes,
    split_name_list,
    split_party_names,
    split_subjects,
)

# Re-exports so existing callers don't break
__all__ = [
    "SOURCE_ID",
    "PROCESS_NORMALIZATION_VERSION",
    "DECISION_EVENT_NORMALIZATION_VERSION",
    "PROCESS_SOURCE_FILES",
    "as_optional_str",
    "counsel_entries_from_juris_partes",
    "first_non_null",
    "infer_process_class_from_number",
    "infer_process_number",
    "is_missing",
    "normalize_entity_name",
    "party_entries_from_juris_partes",
    "read_jsonl_records",
    "split_name_list",
    "split_party_names",
    "split_subjects",
    "stable_id",
    "stable_record_hash",
    "utc_now_iso",
    "write_jsonl",
]

SOURCE_ID = "STF-TRANSP-REGDIST"
PROCESS_NORMALIZATION_VERSION = "process-v1"
DECISION_EVENT_NORMALIZATION_VERSION = "decision-event-v1"

PROCESS_SOURCE_FILES = [
    "acervo.csv",
    "controle_concentrado.csv",
    "decisoes.csv",
    "decisoes_covid.csv",
    "omissao_inconstitucional.csv",
    "plenario_virtual.csv",
    "reclamacoes.csv",
    "distribuidos.csv",
    "recebidos_baixados.csv",
    "repercussao_geral.csv",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_record_hash(filename: str, row_number: int, process_number: str) -> str:
    payload = f"{filename}:{row_number}:{process_number}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_jsonl(records: list[dict[str, Any]], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return output_path


def _preview_jsonl_line(line: str, *, limit: int = 160) -> str:
    compact = line.strip()
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    preview = _preview_jsonl_line(line)
                    raise ValueError(f"Invalid JSONL record at {path}:{line_number}: content={preview!r}") from exc
    return records
