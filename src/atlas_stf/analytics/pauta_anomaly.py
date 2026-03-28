"""Build pauta anomaly analytics: detect session behaviour anomalies per minister."""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..core.stats import z_score
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_io import read_jsonl

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/pauta_anomaly.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/pauta_anomaly_summary.schema.json")
DEFAULT_SESSION_EVENT_PATH = Path("data/curated/session_event.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
Z_SCORE_THRESHOLD = 2.0
REJUDGE_WINDOW_DAYS = 90


def _parse_date(date_str: str | None) -> datetime | None:
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except ValueError, IndexError:
        return None


def _compute_std(values: list[float]) -> float:
    """Compute population standard deviation."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _group_session_events(
    session_event_path: Path,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Group session events by rapporteur and year.

    Returns {rapporteur: {year_str: [events]}}.
    """
    result: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for record in read_jsonl(session_event_path):
        rapporteur = record.get("rapporteur_at_event")
        event_date = record.get("event_date")
        if not rapporteur or not event_date:
            continue
        parsed = _parse_date(str(event_date))
        if not parsed:
            continue
        year_str = str(parsed.year)
        result[str(rapporteur)][year_str].append(record)
    return dict(result)


def _count_pauta_no_rejudge(
    process_events: dict[str, list[dict[str, Any]]],
    rapporteur: str,
    period: str,
) -> int:
    """Count pauta withdrawals NOT followed by re-inclusion within 90 days.

    Looks at all session events for processes where this rapporteur withdrew
    from pauta in the given period.
    """
    count = 0
    for pid, events in process_events.items():
        # Find withdrawals by this rapporteur in this period
        for ev in events:
            if ev.get("event_type") != "pauta_withdrawal":
                continue
            if str(ev.get("rapporteur_at_event") or "") != rapporteur:
                continue
            ev_date = _parse_date(str(ev.get("event_date") or ""))
            if not ev_date or str(ev_date.year) != period:
                continue

            # Check if followed by pauta_inclusion within 90 days
            deadline = ev_date + timedelta(days=REJUDGE_WINDOW_DAYS)
            found_reinclusion = False
            for subsequent in events:
                if subsequent.get("event_type") != "pauta_inclusion":
                    continue
                sub_date = _parse_date(str(subsequent.get("event_date") or ""))
                if sub_date and ev_date < sub_date <= deadline:
                    found_reinclusion = True
                    break

            if not found_reinclusion:
                count += 1
    return count


def build_pauta_anomaly(
    *,
    session_event_path: Path = DEFAULT_SESSION_EVENT_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build pauta anomaly analytics: detect anomalous session behaviour per minister."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if on_progress:
        on_progress(0, 4, "Pauta Anomaly: Carregando dados...")

    if not session_event_path.exists():
        logger.warning("Pauta anomaly skipped: session_event.jsonl not found")
        output_path = output_dir / "pauta_anomaly.jsonl"
        output_path.write_text("", encoding="utf-8")
        return output_path

    grouped = _group_session_events(session_event_path)

    # Build process-level event index for re-inclusion checks
    process_session_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in read_jsonl(session_event_path):
        pid = record.get("process_id")
        if pid:
            process_session_events[str(pid)].append(record)

    if on_progress:
        on_progress(1, 4, "Pauta Anomaly: Calculando métricas por ministro...")

    # Collect all (rapporteur, period) records
    all_periods: set[str] = set()
    minister_period_data: dict[tuple[str, str], dict[str, Any]] = {}

    for rapporteur, periods in grouped.items():
        for period, events in periods.items():
            all_periods.add(period)

            # Vista metrics
            vista_events = [e for e in events if e.get("event_type") == "pedido_de_vista"]
            vista_count = len(vista_events)
            vista_durations: list[float] = []
            for ve in vista_events:
                vd = ve.get("vista_duration_days")
                if vd is not None and isinstance(vd, (int, float)):
                    vista_durations.append(float(vd))

            vista_avg: float | None = None
            vista_max: int | None = None
            if vista_durations:
                vista_avg = round(sum(vista_durations) / len(vista_durations), 2)
                vista_max = int(max(vista_durations))

            # Pauta metrics
            pauta_withdrawal_count = sum(1 for e in events if e.get("event_type") == "pauta_withdrawal")

            minister_period_data[(rapporteur, period)] = {
                "vista_count": vista_count,
                "vista_avg": vista_avg,
                "vista_max": vista_max,
                "vista_durations": vista_durations,
                "pauta_withdrawal_count": pauta_withdrawal_count,
            }

    if on_progress:
        on_progress(2, 4, "Pauta Anomaly: Calculando z-scores e flags...")

    now_iso = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for period in sorted(all_periods):
        # Collect global baselines for this period
        period_vista_avgs: list[float] = []
        period_vista_counts: list[float] = []
        minister_keys = [(rap, per) for (rap, per) in minister_period_data if per == period]

        for key in minister_keys:
            data = minister_period_data[key]
            period_vista_counts.append(float(data["vista_count"]))
            if data["vista_avg"] is not None:
                period_vista_avgs.append(data["vista_avg"])

        # Global baselines
        baseline_vista_avg: float | None = None
        if period_vista_avgs:
            baseline_vista_avg = round(sum(period_vista_avgs) / len(period_vista_avgs), 2)

        # Standard deviations for z-scores
        vista_avg_std = _compute_std(period_vista_avgs)
        vista_count_mean = sum(period_vista_counts) / len(period_vista_counts) if period_vista_counts else 0.0
        vista_count_std = _compute_std(period_vista_counts)

        for key in minister_keys:
            rapporteur = key[0]
            data = minister_period_data[key]

            # Vista z-scores
            v_duration_z: float | None = None
            if data["vista_avg"] is not None and baseline_vista_avg is not None:
                v_duration_z = z_score(data["vista_avg"], baseline_vista_avg, vista_avg_std)

            v_frequency_z: float | None = None
            if period_vista_counts:
                v_frequency_z = z_score(float(data["vista_count"]), vista_count_mean, vista_count_std)

            # Pauta no-rejudge count
            pauta_no_rejudge = _count_pauta_no_rejudge(dict(process_session_events), rapporteur, period)

            # Flags
            vista_duration_flag = v_duration_z is not None and v_duration_z > Z_SCORE_THRESHOLD
            vista_frequency_flag = v_frequency_z is not None and v_frequency_z > Z_SCORE_THRESHOLD
            pauta_stall_flag = pauta_no_rejudge > 0

            anomaly_id = stable_id("pa_", f"{rapporteur}:{period}")
            records.append(
                {
                    "anomaly_id": anomaly_id,
                    "rapporteur": rapporteur,
                    "analysis_period": period,
                    "vista_request_count": data["vista_count"],
                    "vista_avg_duration_days": data["vista_avg"],
                    "vista_max_duration_days": data["vista_max"],
                    "baseline_vista_avg_duration": baseline_vista_avg,
                    "vista_duration_z_score": v_duration_z,
                    "vista_frequency_z_score": v_frequency_z,
                    "pauta_withdrawal_count": data["pauta_withdrawal_count"],
                    "pauta_no_rejudge_90d_count": pauta_no_rejudge,
                    "vista_duration_flag": vista_duration_flag,
                    "vista_frequency_flag": vista_frequency_flag,
                    "pauta_stall_flag": pauta_stall_flag,
                    "generated_at": now_iso,
                }
            )

    if on_progress:
        on_progress(3, 4, "Pauta Anomaly: Gravando resultados...")

    validate_records(records, SCHEMA_PATH)

    output_path = output_dir / "pauta_anomaly.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    duration_flags = sum(1 for r in records if r["vista_duration_flag"])
    frequency_flags = sum(1 for r in records if r["vista_frequency_flag"])
    stall_flags = sum(1 for r in records if r["pauta_stall_flag"])
    unique_ministers = len({r["rapporteur"] for r in records})
    unique_periods = len({r["analysis_period"] for r in records})

    summary = {
        "generated_at": now_iso,
        "total_records": len(records),
        "vista_duration_flag_count": duration_flags,
        "vista_frequency_flag_count": frequency_flags,
        "pauta_stall_flag_count": stall_flags,
        "ministers_analyzed": unique_ministers,
        "periods_analyzed": unique_periods,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "pauta_anomaly_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Built pauta anomaly: %d records (%d duration flags, %d frequency flags, %d stall flags)",
        len(records),
        duration_flags,
        frequency_flags,
        stall_flags,
    )
    if on_progress:
        on_progress(4, 4, "Pauta Anomaly: Concluído")
    return output_path
