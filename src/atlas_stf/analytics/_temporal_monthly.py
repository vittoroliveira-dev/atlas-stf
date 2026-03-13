"""Monthly trends, year-over-year, and seasonality builders for temporal analysis."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from statistics import mean, pstdev
from typing import Any

from ..core.identity import stable_id
from ._temporal_utils import (
    BREAKPOINT_SCORE_THRESHOLD,
    CUSUM_DRIFT,
    ROLLING_WINDOW_MONTHS,
    _month_key,
    _month_range,
    _round,
)


def _rolling_rate(points: list[dict[str, Any]], index: int) -> float | None:
    window = points[max(0, index - ROLLING_WINDOW_MONTHS + 1) : index + 1]
    total = sum(item["decision_count"] for item in window)
    if total == 0:
        return None
    favorable = sum(item["favorable_count"] for item in window)
    return favorable / total


def _build_monthly_records(events: list[dict[str, Any]], generated_at: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[date, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"favorable": 0, "unfavorable": 0})
    )
    for event in events:
        grouped[event["rapporteur"]][event["month_start"]][event["outcome"]] += 1

    records: list[dict[str, Any]] = []
    for rapporteur, month_map in grouped.items():
        points: list[dict[str, Any]] = []
        for month_start in _month_range(min(month_map), max(month_map)):
            counts = month_map.get(month_start, {"favorable": 0, "unfavorable": 0})
            favorable_count = counts["favorable"]
            unfavorable_count = counts["unfavorable"]
            decision_count = favorable_count + unfavorable_count
            favorable_rate = (favorable_count / decision_count) if decision_count else None
            points.append(
                {
                    "month_start": month_start,
                    "decision_month": _month_key(month_start),
                    "decision_year": month_start.year,
                    "decision_count": decision_count,
                    "favorable_count": favorable_count,
                    "unfavorable_count": unfavorable_count,
                    "favorable_rate": favorable_rate,
                }
            )
        series = [point["favorable_rate"] for point in points if point["favorable_rate"] is not None]
        overall_mean = mean(series) if series else 0.0
        overall_std = pstdev(series) if len(series) > 1 else 0.0
        cusum_pos = 0.0
        cusum_neg = 0.0
        for index, point in enumerate(points):
            rate = point["favorable_rate"] if point["favorable_rate"] is not None else overall_mean
            z_score = 0.0 if overall_std == 0.0 else (rate - overall_mean) / overall_std
            cusum_pos = max(0.0, cusum_pos + z_score - CUSUM_DRIFT)
            cusum_neg = min(0.0, cusum_neg + z_score + CUSUM_DRIFT)
            score = max(abs(z_score), cusum_pos, -cusum_neg)
            records.append(
                {
                    "analysis_kind": "monthly_minister",
                    "record_id": stable_id("tmp_", f"{rapporteur}|{point['decision_month']}"),
                    "rapporteur": rapporteur,
                    "decision_month": point["decision_month"],
                    "decision_year": point["decision_year"],
                    "decision_count": point["decision_count"],
                    "favorable_count": point["favorable_count"],
                    "unfavorable_count": point["unfavorable_count"],
                    "favorable_rate": _round(point["favorable_rate"]),
                    "rolling_favorable_rate_6m": _round(_rolling_rate(points, index)),
                    "breakpoint_score": _round(score),
                    "breakpoint_flag": bool(
                        point["decision_count"] > 0
                        and index >= ROLLING_WINDOW_MONTHS - 1
                        and score >= BREAKPOINT_SCORE_THRESHOLD
                    ),
                    "generated_at": generated_at,
                }
            )
    return records


def _build_yoy_records(events: list[dict[str, Any]], generated_at: str) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, int], dict[str, int]] = defaultdict(lambda: {"favorable": 0, "unfavorable": 0})
    for event in events:
        process_class = event["process_class"]
        if process_class:
            grouped[(event["rapporteur"], process_class, event["decision_year"])][event["outcome"]] += 1
    records: list[dict[str, Any]] = []
    for (rapporteur, process_class, decision_year), counts in sorted(grouped.items()):
        previous = grouped.get((rapporteur, process_class, decision_year - 1))
        if previous is None:
            continue
        current_count = counts["favorable"] + counts["unfavorable"]
        previous_count = previous["favorable"] + previous["unfavorable"]
        if current_count == 0 or previous_count == 0:
            continue
        current_rate = counts["favorable"] / current_count
        previous_rate = previous["favorable"] / previous_count
        records.append(
            {
                "analysis_kind": "yoy_process_class",
                "record_id": stable_id("tmp_", f"{rapporteur}|{process_class}|{decision_year}"),
                "rapporteur": rapporteur,
                "process_class": process_class,
                "decision_year": decision_year,
                "decision_count": current_count,
                "favorable_count": counts["favorable"],
                "unfavorable_count": counts["unfavorable"],
                "current_favorable_rate": _round(current_rate),
                "favorable_rate": _round(current_rate),
                "prior_decision_count": previous_count,
                "prior_favorable_rate": _round(previous_rate),
                "delta_vs_prior_year": _round(current_rate - previous_rate),
                "generated_at": generated_at,
            }
        )
    return records


def _build_seasonality_records(events: list[dict[str, Any]], generated_at: str) -> list[dict[str, Any]]:
    totals: dict[str, dict[str, int]] = defaultdict(lambda: {"favorable": 0, "unfavorable": 0})
    grouped: dict[tuple[str, int], dict[str, int]] = defaultdict(lambda: {"favorable": 0, "unfavorable": 0})
    for event in events:
        totals[event["rapporteur"]][event["outcome"]] += 1
        grouped[(event["rapporteur"], event["decision_date"].month)][event["outcome"]] += 1
    records: list[dict[str, Any]] = []
    for (rapporteur, month_of_year), counts in sorted(grouped.items()):
        total = counts["favorable"] + counts["unfavorable"]
        overall = totals[rapporteur]
        overall_total = overall["favorable"] + overall["unfavorable"]
        if total == 0 or overall_total == 0:
            continue
        favorable_rate = counts["favorable"] / total
        overall_rate = overall["favorable"] / overall_total
        records.append(
            {
                "analysis_kind": "seasonality",
                "record_id": stable_id("tmp_", f"{rapporteur}|seasonality|{month_of_year}"),
                "rapporteur": rapporteur,
                "month_of_year": month_of_year,
                "decision_count": total,
                "favorable_count": counts["favorable"],
                "unfavorable_count": counts["unfavorable"],
                "favorable_rate": _round(favorable_rate),
                "delta_vs_overall": _round(favorable_rate - overall_rate),
                "generated_at": generated_at,
            }
        )
    return records
