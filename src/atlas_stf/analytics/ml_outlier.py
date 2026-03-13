"""ML-based outlier detection using Isolation Forest."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..schema_validate import validate_records

DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_BASELINE_PATH = Path("data/analytics/baseline.jsonl")
DEFAULT_LINK_PATH = Path("data/analytics/decision_event_group_link.jsonl")
DEFAULT_ALERT_PATH = Path("data/analytics/outlier_alert.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
SCHEMA_PATH = Path("schemas/ml_outlier_score.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/ml_outlier_score_summary.schema.json")
MIN_EVENTS_FOR_ML = 30
ENSEMBLE_WEIGHT_ALERT = 0.6
ENSEMBLE_WEIGHT_ML = 0.4

logger = logging.getLogger(__name__)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _encode_categorical_frequencies(
    events: list[dict[str, Any]],
    fields: list[str],
) -> list[list[float]]:
    field_counters: list[dict[str, int]] = []
    for field in fields:
        counter: dict[str, int] = defaultdict(int)
        for event in events:
            value = str(event.get(field, ""))
            if value:
                counter[value] += 1
        field_counters.append(dict(counter))

    feature_matrix: list[list[float]] = []
    for event in events:
        row: list[float] = []
        for i, field in enumerate(fields):
            values = field_counters[i]
            total = sum(values.values())
            observed = str(event.get(field, ""))
            freq = values.get(observed, 0) / total if total > 0 and observed else 0.0
            row.append(freq)
        feature_matrix.append(row)
    return feature_matrix


def build_ml_outlier_scores(
    *,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    baseline_path: Path = DEFAULT_BASELINE_PATH,
    link_path: Path = DEFAULT_LINK_PATH,
    alert_path: Path = DEFAULT_ALERT_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path | None:
    try:
        from sklearn.ensemble import IsolationForest  # type: ignore[import-untyped]
    except ImportError:
        logger.warning("scikit-learn not installed — skipping ML outlier detection")
        return None

    import numpy as np

    if on_progress:
        on_progress(0, 3, "ML Outlier: Carregando dados...")
    events_list = _read_jsonl(decision_event_path)
    events_by_id = {row["decision_event_id"]: row for row in events_list}
    links = _read_jsonl(link_path)
    baselines = {row["comparison_group_id"]: row for row in _read_jsonl(baseline_path)}

    group_event_ids: dict[str, list[str]] = defaultdict(list)
    for link in links:
        group_event_ids[link["comparison_group_id"]].append(link["decision_event_id"])

    alert_scores: dict[str, float] = {}
    if alert_path.exists():
        for row in _read_jsonl(alert_path):
            score = row.get("alert_score")
            if score is not None:
                alert_scores[row["decision_event_id"]] = float(score)

    fields = ["decision_progress", "current_rapporteur", "judging_body", "process_class"]
    timestamp = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    if on_progress:
        on_progress(1, 3, "ML Outlier: Treinando modelos...")
    for group_id, event_ids in group_event_ids.items():
        if group_id not in baselines:
            continue
        group_events = [events_by_id[eid] for eid in event_ids if eid in events_by_id]
        if len(group_events) < MIN_EVENTS_FOR_ML:
            continue

        feature_matrix = _encode_categorical_frequencies(group_events, fields)
        X = np.array(feature_matrix)

        model = IsolationForest(contamination="auto", random_state=42, n_estimators=100)
        model.fit(X)
        raw_scores = model.decision_function(X)

        score_min = float(raw_scores.min())
        score_max = float(raw_scores.max())
        score_range = score_max - score_min

        for i, event in enumerate(group_events):
            event_id = event["decision_event_id"]
            anomaly_score = float(raw_scores[i])
            if score_range == 0.0:
                ml_rarity = 0.5
            else:
                ml_rarity = 1.0 - (anomaly_score - score_min) / score_range

            existing_alert_score = alert_scores.get(event_id)
            ensemble = None
            if existing_alert_score is not None:
                ensemble = round(
                    ENSEMBLE_WEIGHT_ALERT * existing_alert_score + ENSEMBLE_WEIGHT_ML * ml_rarity,
                    6,
                )

            records.append(
                {
                    "decision_event_id": event_id,
                    "comparison_group_id": group_id,
                    "ml_anomaly_score": round(anomaly_score, 6),
                    "ml_rarity_score": round(ml_rarity, 6),
                    "ensemble_score": ensemble,
                    "n_features": len(fields),
                    "n_samples": len(group_events),
                    "generated_at": timestamp,
                }
            )

    if on_progress:
        on_progress(2, 3, "ML Outlier: Gravando resultados...")
    validate_records(records, SCHEMA_PATH)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "ml_outlier_score.jsonl"
    with output_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary_path = output_dir / "ml_outlier_score_summary.json"
    summary = {
        "generated_at": timestamp,
        "record_count": len(records),
        "groups_processed": len({r["comparison_group_id"] for r in records}),
        "ensemble_count": sum(1 for r in records if r["ensemble_score"] is not None),
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if on_progress:
        on_progress(3, 3, "ML Outlier: Concluído")
    return output_path
