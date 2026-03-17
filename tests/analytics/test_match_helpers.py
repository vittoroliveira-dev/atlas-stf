from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics._match_helpers import (
    build_baseline_rates,
    build_baseline_rates_stratified,
    build_process_jb_category_map,
    compute_favorable_rate,
    compute_favorable_rate_role_aware,
    compute_favorable_rate_substantive,
    lookup_baseline_rate,
)
from atlas_stf.analytics.baseline import MIN_RELIABLE_SIZE


def test_compute_favorable_rate_excludes_neutral_outcomes() -> None:
    rate = compute_favorable_rate(["Provido", "Prejudicado", "Não provido"])
    assert rate == 0.5


def test_compute_favorable_rate_returns_none_when_only_neutral() -> None:
    assert compute_favorable_rate(["Prejudicado", "Extinto o processo"]) is None


def test_compute_favorable_rate_role_aware_excludes_neutral_outcomes() -> None:
    rate = compute_favorable_rate_role_aware(
        [
            ("Agravo regimental não provido", "RECDO.(A/S)"),
            ("Prejudicado", "RECDO.(A/S)"),
            ("Provido", "RECDO.(A/S)"),
        ]
    )
    assert rate == 0.5


def test_substantive_rate_filters_liminars() -> None:
    """Liminares should be excluded from substantive rate."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Provido", None),
            ("Liminar deferida", None),
            ("Não provido", None),
        ]
    )
    assert rate == 0.5
    assert n == 2


def test_substantive_rate_filters_procedural() -> None:
    """Desistências and inadmissibility should be excluded."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Provido", None),
            ("Homologada a desistência", None),
            ("Negado seguimento", None),
        ]
    )
    assert rate == 1.0
    assert n == 1


def test_substantive_rate_returns_none_when_no_substantive() -> None:
    """No substantive decisions → (None, 0)."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Liminar deferida", None),
            ("Prejudicado", None),
        ]
    )
    assert rate is None
    assert n == 0


def test_substantive_rate_returns_denominator() -> None:
    """n_substantive should count all substantive decisions."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Provido", None),
            ("Não provido", None),
            ("Desprovido", None),
        ]
    )
    assert n == 3
    assert rate is not None


def test_substantive_rate_excludes_unknown_embargos() -> None:
    """Embargos are unknown → excluded from substantive rate."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Provido", None),
            ("Embargos rejeitados", None),
            ("Embargos recebidos", None),
        ]
    )
    assert n == 1
    assert rate == 1.0


# ---------------------------------------------------------------------------
# Helpers for stratified baseline tests
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _make_decision_events(
    process_id: str,
    judging_body: str | None,
    is_collegiate: bool | None,
    progress: str,
    count: int,
) -> list[dict]:
    return [
        {
            "decision_event_id": f"de_{process_id}_{i}",
            "process_id": process_id,
            "decision_progress": progress,
            "judging_body": judging_body,
            "is_collegiate": is_collegiate,
        }
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# build_baseline_rates_stratified
# ---------------------------------------------------------------------------


def test_build_baseline_rates_stratified_uses_stratified_cell(tmp_path: Path) -> None:
    """Cell with >= MIN_RELIABLE_SIZE events should appear in stratified_rates."""
    processes = [{"process_id": f"p{i}", "process_class": "RE"} for i in range(12)]
    events: list[dict] = []
    for i in range(12):
        events.extend(_make_decision_events(f"p{i}", "Segunda Turma", True, "Provido", 1))

    _write_jsonl(tmp_path / "process.jsonl", processes)
    _write_jsonl(tmp_path / "decision_event.jsonl", events)

    stratified, fallback = build_baseline_rates_stratified(
        tmp_path / "decision_event.jsonl", tmp_path / "process.jsonl"
    )
    assert ("RE", "turma") in stratified
    assert stratified[("RE", "turma")] == 1.0
    assert "RE" in fallback


def test_build_baseline_rates_stratified_fallback_on_small_cell(tmp_path: Path) -> None:
    """Cell with < MIN_RELIABLE_SIZE events should NOT appear in stratified_rates."""
    n_small = MIN_RELIABLE_SIZE - 1
    processes = [{"process_id": f"p{i}", "process_class": "RE"} for i in range(n_small)]
    events: list[dict] = []
    for i in range(n_small):
        events.extend(_make_decision_events(f"p{i}", None, False, "Provido", 1))

    _write_jsonl(tmp_path / "process.jsonl", processes)
    _write_jsonl(tmp_path / "decision_event.jsonl", events)

    stratified, fallback = build_baseline_rates_stratified(
        tmp_path / "decision_event.jsonl", tmp_path / "process.jsonl"
    )
    assert ("RE", "monocratico") not in stratified
    assert "RE" in fallback
    assert fallback["RE"] == 1.0


def test_build_baseline_rates_stratified_fallback_matches_legacy(tmp_path: Path) -> None:
    """fallback_rates should match build_baseline_rates() for the same data."""
    processes = [{"process_id": f"p{i}", "process_class": "RE"} for i in range(5)]
    events: list[dict] = []
    for i in range(3):
        events.extend(_make_decision_events(f"p{i}", "Segunda Turma", True, "Provido", 1))
    for i in range(3, 5):
        events.extend(_make_decision_events(f"p{i}", None, False, "Não provido", 1))

    _write_jsonl(tmp_path / "process.jsonl", processes)
    _write_jsonl(tmp_path / "decision_event.jsonl", events)

    _, fallback = build_baseline_rates_stratified(tmp_path / "decision_event.jsonl", tmp_path / "process.jsonl")
    legacy = build_baseline_rates(tmp_path / "decision_event.jsonl", tmp_path / "process.jsonl")
    assert fallback == legacy


def test_build_baseline_rates_stratified_no_data(tmp_path: Path) -> None:
    """No classifiable events → both dicts empty."""
    _write_jsonl(tmp_path / "process.jsonl", [])
    _write_jsonl(tmp_path / "decision_event.jsonl", [])

    stratified, fallback = build_baseline_rates_stratified(
        tmp_path / "decision_event.jsonl", tmp_path / "process.jsonl"
    )
    assert stratified == {}
    assert fallback == {}


# ---------------------------------------------------------------------------
# build_process_jb_category_map
# ---------------------------------------------------------------------------


def test_build_process_jb_category_map(tmp_path: Path) -> None:
    """Predominant jb_category should win; missing judging_body → incerto."""
    events = [
        *_make_decision_events("proc1", "Segunda Turma", True, "Provido", 3),
        *_make_decision_events("proc1", None, False, "Provido", 1),
        *_make_decision_events("proc2", None, None, "Provido", 2),
    ]
    _write_jsonl(tmp_path / "decision_event.jsonl", events)

    jb_map = build_process_jb_category_map(tmp_path / "decision_event.jsonl")
    assert jb_map["proc1"] == "turma"  # 3 turma > 1 monocratico
    assert jb_map["proc2"] == "incerto"


# ---------------------------------------------------------------------------
# lookup_baseline_rate
# ---------------------------------------------------------------------------


def test_lookup_baseline_rate_stratified_hit() -> None:
    """Stratified cell exists → return its rate."""
    stratified = {("RE", "turma"): 0.75}
    fallback = {"RE": 0.60}
    assert lookup_baseline_rate(stratified, fallback, "RE", "turma") == 0.75


def test_lookup_baseline_rate_fallback_hit() -> None:
    """Stratified cell missing, fallback present → return fallback."""
    stratified: dict[tuple[str, str], float] = {}
    fallback = {"RE": 0.60}
    assert lookup_baseline_rate(stratified, fallback, "RE", "monocratico") == 0.60


def test_lookup_baseline_rate_miss() -> None:
    """Both missing → return None."""
    assert lookup_baseline_rate({}, {}, "RE", "turma") is None
