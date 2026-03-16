"""Build outlier alerts from comparison groups, baselines, and decision events."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from .score import ALERT_SCORE_THRESHOLD, score_event_against_baseline

ALERT_SCHEMA = Path("schemas/outlier_alert.schema.json")
SUMMARY_SCHEMA = Path("schemas/outlier_alert_summary.schema.json")
DEFAULT_BASELINE_PATH = Path("data/analytics/baseline.jsonl")
DEFAULT_LINK_PATH = Path("data/analytics/decision_event_group_link.jsonl")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/analytics/outlier_alert.jsonl")
DEFAULT_SUMMARY_PATH = Path("data/analytics/outlier_alert_summary.json")
DEFAULT_COMPOUND_RISK_PATH = Path("data/analytics/compound_risk.jsonl")


@dataclass(frozen=True)
class OutlierAlertRecord:
    alert_id: str
    process_id: str
    decision_event_id: str
    comparison_group_id: str | None
    alert_type: str
    alert_score: float | None
    expected_pattern: str | None
    observed_pattern: str | None
    evidence_summary: str | None
    uncertainty_note: str | None
    status: str
    created_at: str | None
    updated_at: str | None
    risk_signal_count: int = 0
    risk_signals: list[str] | None = None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _stable_alert_id(decision_event_id: str, comparison_group_id: str) -> str:
    payload = f"{decision_event_id}:{comparison_group_id}"
    return "alert_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _build_process_risk_index(compound_risk_path: Path) -> dict[str, dict[str, Any]]:
    """Build process_id → {signals, count} index from compound_risk.jsonl."""
    process_risk: dict[str, dict[str, Any]] = defaultdict(lambda: {"signals": set(), "count": 0})
    for row in _read_jsonl(compound_risk_path):
        signals = row.get("signals") or []
        if isinstance(signals, str):
            signals = json.loads(signals)
        for pid in row.get("shared_process_ids") or []:
            process_risk[pid]["signals"].update(signals)
    for v in process_risk.values():
        v["count"] = len(v["signals"])
    return dict(process_risk)


def build_alerts(
    baseline_path: Path = DEFAULT_BASELINE_PATH,
    link_path: Path = DEFAULT_LINK_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    summary_path: Path = DEFAULT_SUMMARY_PATH,
    compound_risk_path: Path = DEFAULT_COMPOUND_RISK_PATH,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> tuple[Path, Path]:
    if on_progress:
        on_progress(0, 3, "Alerts: Carregando dados...")
    baselines = {row["comparison_group_id"]: row for row in _read_jsonl(baseline_path)}
    events = {row["decision_event_id"]: row for row in _read_jsonl(decision_event_path)}
    links = _read_jsonl(link_path)

    # Build process risk index from compound_risk (auto-detect)
    process_risk: dict[str, dict[str, Any]] = {}
    if compound_risk_path.exists():
        process_risk = _build_process_risk_index(compound_risk_path)

    timestamp = datetime.now(timezone.utc).isoformat()
    if on_progress:
        on_progress(1, 3, "Alerts: Pontuando eventos...")
    alert_records: list[dict[str, Any]] = []
    skipped_missing_baseline = 0
    skipped_missing_event = 0
    skipped_below_threshold = 0
    skipped_without_explanation = 0

    for link in links:
        comparison_group_id = link["comparison_group_id"]
        baseline = baselines.get(comparison_group_id)
        if baseline is None:
            skipped_missing_baseline += 1
            continue

        event = events.get(link["decision_event_id"])
        if event is None:
            skipped_missing_event += 1
            continue

        score_result = score_event_against_baseline(event, baseline)
        if (
            score_result.alert_score is None
            or score_result.expected_pattern is None
            or score_result.observed_pattern is None
            or score_result.evidence_summary is None
            or score_result.alert_type is None
        ):
            skipped_without_explanation += 1
            continue

        if score_result.alert_score < ALERT_SCORE_THRESHOLD:
            skipped_below_threshold += 1
            continue

        # Enrich with risk signals from compound_risk
        risk_info = process_risk.get(event["process_id"], {})
        risk_signals = sorted(risk_info.get("signals", set()))
        risk_signal_count = len(risk_signals)

        status = "inconclusivo" if score_result.alert_type == "inconclusivo" else "novo"
        alert_records.append(
            asdict(
                OutlierAlertRecord(
                    alert_id=_stable_alert_id(event["decision_event_id"], comparison_group_id),
                    process_id=event["process_id"],
                    decision_event_id=event["decision_event_id"],
                    comparison_group_id=comparison_group_id,
                    alert_type=score_result.alert_type,
                    alert_score=score_result.alert_score,
                    expected_pattern=score_result.expected_pattern,
                    observed_pattern=score_result.observed_pattern,
                    evidence_summary=score_result.evidence_summary,
                    uncertainty_note=score_result.uncertainty_note,
                    status=status,
                    created_at=timestamp,
                    updated_at=timestamp,
                    risk_signal_count=risk_signal_count,
                    risk_signals=risk_signals if risk_signals else None,
                )
            )
        )

    if on_progress:
        on_progress(2, 3, "Alerts: Gravando resultados...")
    validate_records(alert_records, ALERT_SCHEMA)
    with AtomicJsonlWriter(output_path) as fh:
        for record in alert_records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    score_values = [record["alert_score"] for record in alert_records if record["alert_score"] is not None]
    summary = {
        "generated_at": timestamp,
        "alert_count": len(alert_records),
        "alert_type_counts": dict(Counter(record["alert_type"] for record in alert_records)),
        "status_counts": dict(Counter(record["status"] for record in alert_records)),
        "threshold": ALERT_SCORE_THRESHOLD,
        "min_score": min(score_values) if score_values else None,
        "max_score": max(score_values) if score_values else None,
        "avg_score": (sum(score_values) / len(score_values)) if score_values else None,
        "skipped_missing_baseline": skipped_missing_baseline,
        "skipped_missing_event": skipped_missing_event,
        "skipped_below_threshold": skipped_below_threshold,
        "skipped_without_explanation": skipped_without_explanation,
    }
    validate_records([summary], SUMMARY_SCHEMA)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if on_progress:
        on_progress(3, 3, "Alerts: Concluído")
    return output_path, summary_path
