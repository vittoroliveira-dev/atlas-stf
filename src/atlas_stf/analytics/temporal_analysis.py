"""Temporal analysis builder for monthly trends, YoY, seasonality and event windows."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from ..core.constants import EVENT_WINDOW_DAYS, ROLLING_WINDOW_MONTHS
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._temporal_corporate import _build_corporate_link_records
from ._temporal_events import _build_event_window_records
from ._temporal_monthly import _build_monthly_records, _build_seasonality_records, _build_yoy_records
from ._temporal_utils import (
    DEFAULT_COUNSEL_PATH,
    DEFAULT_DECISION_EVENT_PATH,
    DEFAULT_EXTERNAL_EVENTS_DIR,
    DEFAULT_MINISTER_BIO_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PARTY_PATH,
    DEFAULT_PROCESS_COUNSEL_LINK_PATH,
    DEFAULT_PROCESS_PARTY_LINK_PATH,
    DEFAULT_PROCESS_PATH,
    DEFAULT_RFB_DIR,
    SCHEMA_PATH,
    SUMMARY_SCHEMA_PATH,
    _classifiable_events,
)

# Re-export public constants used by api/_temporal_analysis.py
__all__ = ["build_temporal_analysis", "EVENT_WINDOW_DAYS", "ROLLING_WINDOW_MONTHS"]


def build_temporal_analysis(
    *,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    minister_bio_path: Path = DEFAULT_MINISTER_BIO_PATH,
    party_path: Path = DEFAULT_PARTY_PATH,
    counsel_path: Path = DEFAULT_COUNSEL_PATH,
    process_party_link_path: Path = DEFAULT_PROCESS_PARTY_LINK_PATH,
    process_counsel_link_path: Path = DEFAULT_PROCESS_COUNSEL_LINK_PATH,
    external_events_dir: Path = DEFAULT_EXTERNAL_EVENTS_DIR,
    rfb_dir: Path = DEFAULT_RFB_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    if on_progress:
        on_progress(0, 7, "Temporal: Carregando dados...")
    generated_at = datetime.now(timezone.utc).isoformat()
    events = _classifiable_events(decision_event_path, process_path)
    if on_progress:
        on_progress(1, 7, "Temporal: Tendências mensais...")
    records = [
        *_build_monthly_records(events, generated_at),
    ]
    if on_progress:
        on_progress(2, 7, "Temporal: Variação anual...")
    records.extend(_build_yoy_records(events, generated_at))
    if on_progress:
        on_progress(3, 7, "Temporal: Sazonalidade...")
    records.extend(_build_seasonality_records(events, generated_at))
    if on_progress:
        on_progress(4, 7, "Temporal: Janelas de eventos...")
    records.extend(_build_event_window_records(events, external_events_dir, generated_at))
    if on_progress:
        on_progress(5, 7, "Temporal: Vínculos corporativos...")
    records.extend(
        _build_corporate_link_records(
            minister_bio_path=minister_bio_path,
            party_path=party_path,
            counsel_path=counsel_path,
            process_party_link_path=process_party_link_path,
            process_counsel_link_path=process_counsel_link_path,
            decision_event_path=decision_event_path,
            rfb_dir=rfb_dir,
            generated_at=generated_at,
        )
    )
    if on_progress:
        on_progress(6, 7, "Temporal: Gravando resultados...")
    validate_records(records, SCHEMA_PATH)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "temporal_analysis.jsonl"
    with AtomicJsonlWriter(output_path) as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    counts_by_kind: dict[str, int] = defaultdict(int)
    ministers = {record["rapporteur"] for record in records if record.get("rapporteur")}
    event_ids = {
        record["event_id"] for record in records if record["analysis_kind"] == "event_window" and record.get("event_id")
    }
    for record in records:
        counts_by_kind[str(record["analysis_kind"])] += 1
    summary = {
        "generated_at": generated_at,
        "total_records": len(records),
        "counts_by_kind": dict(sorted(counts_by_kind.items())),
        "ministers_covered": len(ministers),
        "events_covered": len(event_ids),
        "rolling_window_months": ROLLING_WINDOW_MONTHS,
        "event_window_days": EVENT_WINDOW_DAYS,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    (output_dir / "temporal_analysis_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if on_progress:
        on_progress(7, 7, "Temporal: Concluído")
    return output_path
