"""Build canonical movement records from STF portal JSON files."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..core.tpu import categorize_movement_text
from ..schema_validate import validate_records
from .common import utc_now_iso, write_jsonl

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/movement.schema.json")
DEFAULT_PORTAL_DIR = Path("data/raw/stf_portal")
DEFAULT_OUTPUT_PATH = Path("data/curated/movement.jsonl")


def _read_portal_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Skipping corrupted portal JSON: %s", path.name)
        return None


def _build_movement_from_andamento(
    process_number: str,
    process_id: str,
    entry: dict[str, Any],
    rapporteur: str | None,
    timestamp: str,
) -> dict[str, Any]:
    description = entry.get("description") or ""
    date = entry.get("date")
    detail = entry.get("detail")

    category = categorize_movement_text(description)
    has_match = category != "outros"

    return {
        "movement_id": stable_id("mov_", f"{process_number}:{date}:{description}"),
        "process_id": process_id,
        "source_system": "stf_portal",
        "tpu_code": None,
        "tpu_name": None,
        "movement_category": category,
        "movement_raw_description": description or None,
        "movement_date": date,
        "movement_detail": detail,
        "rapporteur_at_event": rapporteur,
        "tpu_match_confidence": "fuzzy" if has_match else None,
        "normalization_method": "regex_rule" if has_match else None,
        "created_at": timestamp,
    }


def _build_movement_from_deslocamento(
    process_number: str,
    process_id: str,
    entry: dict[str, Any],
    rapporteur: str | None,
    timestamp: str,
) -> dict[str, Any]:
    origin = entry.get("origin") or ""
    destination = entry.get("destination") or ""
    reason = entry.get("reason") or ""
    date = entry.get("date")

    description = f"Deslocamento: {origin} → {destination}"
    if reason:
        description = f"{description} ({reason})"

    category = categorize_movement_text(description)
    if category == "outros":
        category = "deslocamento"
    has_match = True  # deslocamentos always have a category

    return {
        "movement_id": stable_id("mov_", f"{process_number}:{date}:{description}"),
        "process_id": process_id,
        "source_system": "stf_portal",
        "tpu_code": None,
        "tpu_name": None,
        "movement_category": category,
        "movement_raw_description": description,
        "movement_date": date,
        "movement_detail": reason or None,
        "rapporteur_at_event": rapporteur,
        "tpu_match_confidence": "fuzzy" if has_match else None,
        "normalization_method": "regex_rule" if has_match else None,
        "created_at": timestamp,
    }


def build_movement_records(
    portal_dir: Path = DEFAULT_PORTAL_DIR,
) -> list[dict[str, Any]]:
    """Build movement records from STF portal JSON files.

    Reads each JSON file from *portal_dir* and produces one movement
    record per andamento and deslocamento entry.
    """
    if not portal_dir.exists():
        return []

    timestamp = utc_now_iso()
    records: list[dict[str, Any]] = []

    for json_path in sorted(portal_dir.glob("*.json")):
        doc = _read_portal_json(json_path)
        if doc is None:
            continue
        process_number = doc.get("process_number", "")
        if not process_number:
            continue

        process_id = stable_id("proc_", process_number)
        informacoes = doc.get("informacoes") or {}
        rapporteur = informacoes.get("relator_atual")

        for entry in doc.get("andamentos", []):
            records.append(
                _build_movement_from_andamento(
                    process_number, process_id, entry, rapporteur, timestamp,
                )
            )

        for entry in doc.get("deslocamentos", []):
            records.append(
                _build_movement_from_deslocamento(
                    process_number, process_id, entry, rapporteur, timestamp,
                )
            )

    records.sort(key=lambda r: (r["process_id"], r.get("movement_date") or ""))
    validate_records(records, SCHEMA_PATH)
    return records


def build_movement_jsonl(
    portal_dir: Path = DEFAULT_PORTAL_DIR,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    """Build and write movement records to JSONL."""
    records = build_movement_records(portal_dir=portal_dir)
    return write_jsonl(records, output_path)
