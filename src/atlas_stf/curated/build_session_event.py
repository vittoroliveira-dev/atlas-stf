"""Build canonical session event records from movements and portal data."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..core.tpu import (
    is_devolvido_vista,
    is_pauta_inclusion,
    is_pauta_withdrawal,
    is_pedido_de_vista,
)
from ..schema_validate import validate_records
from .common import read_jsonl_records, utc_now_iso, write_jsonl

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/session_event.schema.json")
DEFAULT_MOVEMENT_PATH = Path("data/curated/movement.jsonl")
DEFAULT_PORTAL_DIR = Path("data/raw/stf_portal")
DEFAULT_OUTPUT_PATH = Path("data/curated/session_event.jsonl")

_SESSION_CATEGORIES = frozenset({"pauta", "vista", "decisao"})


def _classify_event_type(description: str | None, category: str) -> str:
    """Determine the event_type from movement description and category."""
    # Check devolvido before pedido: devolvido descriptions often also contain "vista"
    if is_devolvido_vista(description):
        return "devolvido_vista"
    if is_pedido_de_vista(description):
        return "pedido_de_vista"
    if is_pauta_inclusion(description):
        return "pauta_inclusion"
    if is_pauta_withdrawal(description):
        return "pauta_withdrawal"

    # Fallback by category
    if category == "pauta":
        return "pauta_inclusion"
    if category == "vista":
        return "pedido_de_vista"
    if category == "decisao":
        return "julgamento"
    return "outros"


def _classify_session_type(description: str | None) -> str | None:
    """Infer session_type from movement description text."""
    if not description:
        return None
    desc_lower = description.lower()

    if "virtual" in desc_lower:
        return "plenario_virtual"
    if "plenário" in desc_lower or "pleno" in desc_lower or "plenario" in desc_lower:
        return "plenario"
    if re.search(r"1[ªa]\s*turma|primeira turma", desc_lower):
        return "turma_1"
    if re.search(r"2[ªa]\s*turma|segunda turma", desc_lower):
        return "turma_2"
    return None


def _compute_vista_durations(
    records: list[dict[str, Any]],
) -> None:
    """Compute vista_duration_days for pedido_de_vista events.

    For each process, finds pairs of pedido_de_vista and devolvido_vista
    events and computes the duration in days.
    """
    by_process: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        pid = record["process_id"]
        by_process.setdefault(pid, []).append(record)

    for process_records in by_process.values():
        vistas = [r for r in process_records if r["event_type"] == "pedido_de_vista"]
        devolvidos = [r for r in process_records if r["event_type"] == "devolvido_vista"]

        # Sort by date for sequential matching
        vistas.sort(key=lambda r: r.get("event_date") or "")
        devolvidos.sort(key=lambda r: r.get("event_date") or "")

        devolvido_idx = 0
        for vista in vistas:
            vista_date_str = vista.get("event_date")
            if not vista_date_str:
                continue
            try:
                vista_date = datetime.strptime(vista_date_str, "%Y-%m-%d")
            except ValueError:
                continue

            # Find the next devolvido after this vista
            while devolvido_idx < len(devolvidos):
                dev_date_str = devolvidos[devolvido_idx].get("event_date")
                if not dev_date_str:
                    devolvido_idx += 1
                    continue
                try:
                    dev_date = datetime.strptime(dev_date_str, "%Y-%m-%d")
                except ValueError:
                    devolvido_idx += 1
                    continue

                if dev_date >= vista_date:
                    duration = (dev_date - vista_date).days
                    vista["vista_duration_days"] = duration
                    devolvido_idx += 1
                    break
                devolvido_idx += 1


def _read_portal_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError, UnicodeDecodeError:
        logger.warning("Skipping corrupted portal JSON: %s", path.name)
        return None


def _build_sessao_virtual_events(
    portal_dir: Path,
    timestamp: str,
    seen_ids: set[str],
) -> list[dict[str, Any]]:
    """Build session events from sessao_virtual entries in portal JSONs."""
    if not portal_dir.exists():
        return []

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

        for entry in doc.get("sessao_virtual", []):
            # Use start_date as event_date
            event_date = entry.get("start_date")
            event_type = "julgamento"

            se_id = stable_id("se_", f"{process_id}:{event_date}:{event_type}")
            if se_id in seen_ids:
                continue
            seen_ids.add(se_id)

            records.append(
                {
                    "session_event_id": se_id,
                    "process_id": process_id,
                    "movement_id": None,
                    "source_system": "stf_portal",
                    "session_type": "plenario_virtual",
                    "event_type": event_type,
                    "event_date": event_date,
                    "rapporteur_at_event": rapporteur,
                    "vista_duration_days": None,
                    "created_at": timestamp,
                }
            )

    return records


def build_session_event_records(
    movement_path: Path = DEFAULT_MOVEMENT_PATH,
    portal_dir: Path = DEFAULT_PORTAL_DIR,
) -> list[dict[str, Any]]:
    """Build session event records from movements and portal data.

    Reads movements from *movement_path*, filters those with session-relevant
    categories, and also reads sessao_virtual entries from portal JSONs.
    """
    timestamp = utc_now_iso()
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    # Phase 1: session events from movements
    if movement_path.exists():
        movements = read_jsonl_records(movement_path)
        for movement in movements:
            category = movement.get("movement_category") or ""
            if category not in _SESSION_CATEGORIES:
                continue

            description = movement.get("movement_raw_description")
            event_type = _classify_event_type(description, category)
            session_type = _classify_session_type(description)
            process_id = movement["process_id"]
            event_date = movement.get("movement_date")

            se_id = stable_id("se_", f"{process_id}:{event_date}:{event_type}")
            if se_id in seen_ids:
                continue
            seen_ids.add(se_id)

            records.append(
                {
                    "session_event_id": se_id,
                    "process_id": process_id,
                    "movement_id": movement.get("movement_id"),
                    "source_system": "stf_portal",
                    "session_type": session_type,
                    "event_type": event_type,
                    "event_date": event_date,
                    "rapporteur_at_event": movement.get("rapporteur_at_event"),
                    "vista_duration_days": None,
                    "created_at": timestamp,
                }
            )

    # Phase 2: session events from sessao_virtual portal entries
    records.extend(
        _build_sessao_virtual_events(portal_dir, timestamp, seen_ids),
    )

    # Phase 3: compute vista durations
    _compute_vista_durations(records)

    records.sort(key=lambda r: (r["process_id"], r.get("event_date") or ""))
    validate_records(records, SCHEMA_PATH)
    return records


def build_session_event_jsonl(
    movement_path: Path = DEFAULT_MOVEMENT_PATH,
    portal_dir: Path = DEFAULT_PORTAL_DIR,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    """Build and write session event records to JSONL."""
    records = build_session_event_records(
        movement_path=movement_path,
        portal_dir=portal_dir,
    )
    return write_jsonl(records, output_path)
