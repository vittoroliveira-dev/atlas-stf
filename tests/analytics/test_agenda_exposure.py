from __future__ import annotations

import json

import pytest

from atlas_stf.analytics.agenda_exposure import build_agenda_exposure


@pytest.fixture
def tmp_dirs(tmp_path):
    c = tmp_path / "curated"
    c.mkdir()
    a = tmp_path / "analytics"
    a.mkdir()
    return c, a


@pytest.fixture
def sample(tmp_dirs):
    c, _ = tmp_dirs
    (c / "agenda_event.jsonl").write_text(json.dumps({
        "agenda_event_id": "agd_t1", "minister_slug": "zanin",
        "minister_name": "MIN. CRISTIANO ZANIN", "owner_scope": "ministerial",
        "event_date": "2024-03-02", "title": "Reuniao ADPF 342",
        "event_category": "private_advocacy", "meeting_nature": "private_meeting",
        "process_refs_matched": [{"process_id": "p1", "process_class": "ADPF",
            "is_own_process": True, "minister_case_role": "relator"}],
        "institutional_role_bias_flag": False, "relevance_track": "A",
    }) + "\n")
    (c / "agenda_coverage.jsonl").write_text(json.dumps({
        "coverage_id": "ac1", "minister_slug": "zanin", "year": 2024,
        "month": 3, "comparability_tier": "high", "business_days_in_month": 21,
    }) + "\n")
    (c / "decision_event.jsonl").write_text(json.dumps({
        "decision_event_id": "d1", "process_id": "p1",
        "decision_date": "2024-03-05", "decision_type": "acordao",
    }) + "\n")
    (c / "process.jsonl").write_text(json.dumps({
        "process_id": "p1", "process_class": "ADPF",
    }) + "\n")


class TestAgendaExposure:
    def test_basic(self, tmp_dirs, sample):
        c, a = tmp_dirs
        build_agenda_exposure(curated_dir=c, analytics_dir=a)
        with (a / "agenda_exposure.jsonl").open() as f:
            exps = [json.loads(line) for line in f if line.strip()]
        m = [e for e in exps if e.get("days_between") == 3]
        assert len(m) >= 1 and m[0]["window"] == "7d" and m[0]["is_own_process"] is True

    def test_scoring(self, tmp_dirs, sample):
        c, a = tmp_dirs
        build_agenda_exposure(curated_dir=c, analytics_dir=a)
        with (a / "agenda_exposure.jsonl").open() as f:
            exps = [json.loads(line) for line in f if line.strip()]
        m = [e for e in exps if e.get("window") == "7d"]
        assert m[0]["priority_score"] <= 0.29
        assert m[0]["priority_tier_override_reason"] == "insufficient_baseline_n"

    def test_summary(self, tmp_dirs, sample):
        c, a = tmp_dirs
        build_agenda_exposure(curated_dir=c, analytics_dir=a)
        s = json.loads((a / "agenda_exposure_summary.json").read_text())
        assert s["total_relevant_events"] >= 1 and s["within_minister_only"] is True

    def test_empty(self, tmp_dirs):
        c, a = tmp_dirs
        for f in ["agenda_event.jsonl", "agenda_coverage.jsonl", "decision_event.jsonl", "process.jsonl"]:
            (c / f).touch()
        build_agenda_exposure(curated_dir=c, analytics_dir=a)
        assert json.loads((a / "agenda_exposure_summary.json").read_text())["total_relevant_events"] == 0
