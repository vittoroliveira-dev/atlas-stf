"""Build representation temporal windows analytics from curated representation data."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..curated.common import read_jsonl_records, write_jsonl

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_OUTPUT_DIR = Path("data/analytics")

# Map movement/event types to procedural windows
_WINDOW_TYPE_MAP: dict[str, str] = {
    # Distribution phase
    "DISTRIBUICAO": "distribuicao",
    "REDISTRIBUICAO": "distribuicao",
    "DISTRIBUIDO": "distribuicao",
    # Pauta / scheduling phase
    "INCLUIDO_EM_PAUTA": "pauta",
    "PAUTA": "pauta",
    "RETIRADO_DE_PAUTA": "pauta",
    "ADIADO": "pauta",
    # Vista phase
    "PEDIDO_DE_VISTA": "vista",
    "VISTA": "vista",
    "DEVOLVIDO_DA_VISTA": "vista",
    # Judgment phase
    "JULGAMENTO": "julgamento",
    "JULGADO": "julgamento",
    "PROCLAMACAO_DO_RESULTADO": "julgamento",
    # Publication phase
    "PUBLICACAO": "publicacao",
    "PUBLICADO": "publicacao",
    "TRANSITO_EM_JULGADO": "publicacao",
}

# Map representation event_type to procedural windows
_EVENT_TYPE_WINDOW_MAP: dict[str, str] = {
    "petition": "distribuicao",
    "oral_argument": "julgamento",
    "memorial": "pauta",
    "amicus_brief": "distribuicao",
    "procuracao": "distribuicao",
    "substabelecimento": "distribuicao",
    "withdrawal": "publicacao",
}


def _classify_window(
    *,
    event_type: str | None = None,
    movement_type: str | None = None,
) -> str | None:
    """Classify a procedural moment into a window type."""
    if event_type and event_type in _EVENT_TYPE_WINDOW_MAP:
        return _EVENT_TYPE_WINDOW_MAP[event_type]
    if movement_type:
        upper = movement_type.upper().replace(" ", "_")
        for keyword, window in _WINDOW_TYPE_MAP.items():
            if keyword in upper:
                return window
    return None


def build_representation_windows(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Analyze lawyer presence across procedural windows.

    Windows: distribuicao, pauta, vista, julgamento, publicacao.

    Output fields per record:
    - window_id, lawyer_id, lawyer_name, window_type, event_count
    - process_count, first_date, last_date
    """
    total = 5
    step = 0

    def tick(desc: str) -> None:
        nonlocal step
        if on_progress:
            on_progress(step, total, desc)
        step += 1

    tick("Janelas: Carregando entidades...")

    lawyer_path = curated_dir / "lawyer_entity.jsonl"
    event_path = curated_dir / "representation_event.jsonl"
    movement_path = curated_dir / "movement.jsonl"
    edge_path = curated_dir / "representation_edge.jsonl"

    lawyers = read_jsonl_records(lawyer_path) if lawyer_path.exists() else []
    events = read_jsonl_records(event_path) if event_path.exists() else []
    movements = read_jsonl_records(movement_path) if movement_path.exists() else []
    edges = read_jsonl_records(edge_path) if edge_path.exists() else []

    tick("Janelas: Indexando advogados e arestas...")

    lawyer_lookup: dict[str, str] = {}
    for rec in lawyers:
        lid = rec.get("lawyer_id")
        name = rec.get("lawyer_name_normalized") or rec.get("lawyer_name_raw", "")
        if lid:
            lawyer_lookup[lid] = name

    # Map process_id -> set of lawyer_ids via edges
    process_lawyers: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        pid = edge.get("process_id")
        lid = edge.get("lawyer_id")
        if pid and lid:
            process_lawyers[pid].add(lid)

    tick("Janelas: Classificando eventos em janelas...")

    # Aggregate (lawyer_id, window_type) -> stats
    window_stats: dict[tuple[str, str], dict[str, Any]] = {}

    def _register_window_event(
        lawyer_id: str,
        window_type: str,
        process_id: str,
        event_date: str | None,
    ) -> None:
        key: tuple[str, str] = (lawyer_id, window_type)
        if key not in window_stats:
            window_stats[key] = {
                "event_count": 0,
                "process_ids": set(),
                "dates": [],
            }
        stats = window_stats[key]
        stats["event_count"] += 1
        stats["process_ids"].add(process_id)
        if event_date:
            stats["dates"].append(event_date)

    # Source 1: representation events (directly tied to lawyers)
    for event in events:
        event_type = event.get("event_type")
        window = _classify_window(event_type=event_type)
        if not window:
            continue
        process_id = event.get("process_id", "")
        event_date = event.get("event_date")
        lawyer_id = event.get("lawyer_id")
        if lawyer_id:
            _register_window_event(lawyer_id, window, process_id, event_date)
        else:
            # Attribute to all lawyers on that process
            for lid in process_lawyers.get(process_id, set()):
                _register_window_event(lid, window, process_id, event_date)

    # Source 2: movements (attributed to lawyers via edges)
    for movement in movements:
        movement_type = movement.get("movement_type") or movement.get("movement_description")
        window = _classify_window(movement_type=movement_type)
        if not window:
            continue
        process_id = movement.get("process_id", "")
        event_date = movement.get("movement_date")
        for lid in process_lawyers.get(process_id, set()):
            _register_window_event(lid, window, process_id, event_date)

    tick("Janelas: Montando registros...")

    timestamp = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for (lawyer_id, window_type), stats in window_stats.items():
        sorted_dates = sorted(stats["dates"]) if stats["dates"] else []
        first_date = sorted_dates[0] if sorted_dates else None
        last_date = sorted_dates[-1] if sorted_dates else None

        window_id = stable_id("win_", f"{lawyer_id}:{window_type}")
        records.append({
            "window_id": window_id,
            "lawyer_id": lawyer_id,
            "lawyer_name": lawyer_lookup.get(lawyer_id, ""),
            "window_type": window_type,
            "event_count": stats["event_count"],
            "process_count": len(stats["process_ids"]),
            "first_date": first_date,
            "last_date": last_date,
            "generated_at": timestamp,
        })

    tick("Janelas: Escrevendo resultados...")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = write_jsonl(records, output_dir / "representation_windows.jsonl")

    # Count per window type
    window_type_counts: dict[str, int] = defaultdict(int)
    for r in records:
        window_type_counts[r["window_type"]] += 1

    summary: dict[str, Any] = {
        "total_records": len(records),
        "total_lawyers": len({r["lawyer_id"] for r in records}),
        "window_type_distribution": dict(window_type_counts),
        "generated_at": timestamp,
    }
    summary_path = output_dir / "representation_windows_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    tick("Janelas: Concluido")
    logger.info(
        "Representation windows: %d records written to %s",
        len(records),
        output_path,
    )

    return output_path
