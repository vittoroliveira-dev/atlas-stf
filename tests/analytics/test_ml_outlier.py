from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _make_events(n: int) -> list[dict]:
    events = []
    for i in range(n):
        progress = "NEGOU PROVIMENTO" if i % 3 != 0 else "DEFERIU PEDIDO"
        rapporteur = f"MIN {'A' if i % 2 == 0 else 'B'}"
        events.append(
            {
                "decision_event_id": f"de_{i}",
                "process_id": f"proc_{i}",
                "decision_progress": progress,
                "current_rapporteur": rapporteur,
                "judging_body": "TURMA",
                "process_class": "AC",
                "decision_date": "2026-01-15",
                "decision_year": 2026,
            }
        )
    return events


def _seed_ml_inputs(base_dir: Path, n_events: int = 50) -> dict[str, Path]:
    paths = {
        "events": base_dir / "decision_event.jsonl",
        "baseline": base_dir / "baseline.jsonl",
        "links": base_dir / "decision_event_group_link.jsonl",
        "alerts": base_dir / "outlier_alert.jsonl",
    }
    events = _make_events(n_events)
    _write_jsonl(paths["events"], events)

    _write_jsonl(
        paths["baseline"],
        [
            {
                "baseline_id": "base_1",
                "comparison_group_id": "grp_1",
                "rule_version": "v1",
                "event_count": n_events,
                "process_count": n_events,
                "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": n_events},
                "expected_rapporteur_distribution": {"MIN A": n_events},
                "expected_judging_body_distribution": {"TURMA": n_events},
                "observed_period_start": "2026-01-01",
                "observed_period_end": "2026-12-31",
                "generated_at": "2026-01-01T00:00:00+00:00",
                "notes": "test",
            }
        ],
    )

    links = [
        {"comparison_group_id": "grp_1", "decision_event_id": f"de_{i}", "process_id": f"proc_{i}"}
        for i in range(n_events)
    ]
    _write_jsonl(paths["links"], links)

    alerts = [{"alert_id": f"alert_{i}", "decision_event_id": f"de_{i}", "alert_score": 0.8} for i in range(5)]
    _write_jsonl(paths["alerts"], alerts)

    return paths


def test_build_ml_outlier_scores_produces_output(tmp_path: Path):
    pytest.importorskip("sklearn")
    from atlas_stf.analytics.ml_outlier import build_ml_outlier_scores

    paths = _seed_ml_inputs(tmp_path, n_events=50)
    output_dir = tmp_path / "output"

    result = build_ml_outlier_scores(
        decision_event_path=paths["events"],
        baseline_path=paths["baseline"],
        link_path=paths["links"],
        alert_path=paths["alerts"],
        output_dir=output_dir,
    )

    assert result is not None
    assert result.exists()
    records = [json.loads(line) for line in result.read_text().splitlines() if line.strip()]
    assert len(records) == 50
    assert all(0.0 <= r["ml_rarity_score"] <= 1.0 for r in records)

    summary_path = output_dir / "ml_outlier_score_summary.json"
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text())
    assert summary["record_count"] == 50


def test_build_ml_outlier_scores_computes_ensemble(tmp_path: Path):
    pytest.importorskip("sklearn")
    from atlas_stf.analytics.ml_outlier import build_ml_outlier_scores

    paths = _seed_ml_inputs(tmp_path, n_events=50)
    output_dir = tmp_path / "output"

    result = build_ml_outlier_scores(
        decision_event_path=paths["events"],
        baseline_path=paths["baseline"],
        link_path=paths["links"],
        alert_path=paths["alerts"],
        output_dir=output_dir,
    )

    assert result is not None
    records = [json.loads(line) for line in result.read_text().splitlines() if line.strip()]
    with_ensemble = [r for r in records if r["ensemble_score"] is not None]
    assert len(with_ensemble) == 5
    for r in with_ensemble:
        assert 0.0 <= r["ensemble_score"] <= 1.0


def test_build_ml_outlier_scores_skips_small_groups(tmp_path: Path):
    pytest.importorskip("sklearn")
    from atlas_stf.analytics.ml_outlier import build_ml_outlier_scores

    paths = _seed_ml_inputs(tmp_path, n_events=10)
    output_dir = tmp_path / "output"

    result = build_ml_outlier_scores(
        decision_event_path=paths["events"],
        baseline_path=paths["baseline"],
        link_path=paths["links"],
        alert_path=paths["alerts"],
        output_dir=output_dir,
    )

    assert result is not None
    records = [json.loads(line) for line in result.read_text().splitlines() if line.strip()]
    assert len(records) == 0


def test_build_ml_outlier_scores_skips_when_sklearn_missing(tmp_path: Path):
    paths = _seed_ml_inputs(tmp_path, n_events=50)
    output_dir = tmp_path / "output"

    with patch.dict("sys.modules", {"sklearn": None, "sklearn.ensemble": None}):
        from importlib import reload

        import atlas_stf.analytics.ml_outlier as mod

        reload(mod)
        result = mod.build_ml_outlier_scores(
            decision_event_path=paths["events"],
            baseline_path=paths["baseline"],
            link_path=paths["links"],
            alert_path=paths["alerts"],
            output_dir=output_dir,
        )

    assert result is None


def test_build_ml_outlier_scores_uses_neutral_rarity_when_scores_are_constant(tmp_path: Path):
    pytest.importorskip("sklearn")
    import numpy as np

    from atlas_stf.analytics.ml_outlier import build_ml_outlier_scores

    paths = _seed_ml_inputs(tmp_path, n_events=50)
    output_dir = tmp_path / "output"

    class _ConstantIsolationForest:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def fit(self, X):  # noqa: N803
            return self

        def decision_function(self, X):  # noqa: N803
            return np.array([0.25 for _ in range(len(X))])

    with patch("sklearn.ensemble.IsolationForest", _ConstantIsolationForest):
        result = build_ml_outlier_scores(
            decision_event_path=paths["events"],
            baseline_path=paths["baseline"],
            link_path=paths["links"],
            alert_path=paths["alerts"],
            output_dir=output_dir,
        )

    assert result is not None
    records = [json.loads(line) for line in result.read_text().splitlines() if line.strip()]
    assert {record["ml_rarity_score"] for record in records} == {0.5}
