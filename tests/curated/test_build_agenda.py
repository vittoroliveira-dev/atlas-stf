from __future__ import annotations

import json

import pytest

from atlas_stf.curated.build_agenda import build_agenda_events


@pytest.fixture
def tmp_dirs(tmp_path):
    raw_dir = tmp_path / "raw" / "agenda"
    raw_dir.mkdir(parents=True)
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir(parents=True)
    return raw_dir, curated_dir


@pytest.fixture
def raw_events(tmp_dirs):
    raw_dir, _ = tmp_dirs
    events = [
        {
            "event_id": "agd_t1",
            "minister_slug": "zanin",
            "minister_name": "MIN. CRISTIANO ZANIN",
            "owner_scope": "ministerial",
            "event_date": "2024-03-02",
            "event_title": "Reuniao sobre ADPF 342",
            "event_category": "private_advocacy",
            "meeting_nature": "private_meeting",
            "process_refs": [{"class": "ADPF", "number": "342", "raw": "ADPF 342"}],
            "has_process_ref": True,
            "relevance_track": "A",
        }
    ]
    with (raw_dir / "2024-03.jsonl").open("w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


@pytest.fixture
def process_data(tmp_dirs):
    _, curated_dir = tmp_dirs
    with (curated_dir / "process.jsonl").open("w") as f:
        f.write(
            json.dumps(
                {
                    "process_id": "proc_001",
                    "process_number": "ADPF 342",
                    "process_class": "ADPF",
                    "rapporteur_slug": "ZANIN",
                }
            )
            + "\n"
        )


class TestBuildAgendaEvents:
    def test_basic(self, tmp_dirs, raw_events, process_data):
        raw_dir, curated_dir = tmp_dirs
        build_agenda_events(raw_dir=raw_dir, curated_dir=curated_dir)
        with (curated_dir / "agenda_event.jsonl").open() as f:
            events = [json.loads(line) for line in f if line.strip()]
        assert len(events) == 1
        m = events[0].get("process_refs_matched", [])
        assert len(m) >= 1 and m[0]["process_id"] == "proc_001" and m[0]["is_own_process"] is True

    def test_coverage(self, tmp_dirs, raw_events, process_data):
        raw_dir, curated_dir = tmp_dirs
        build_agenda_events(raw_dir=raw_dir, curated_dir=curated_dir)
        with (curated_dir / "agenda_coverage.jsonl").open() as f:
            covs = [json.loads(line) for line in f if line.strip()]
        assert len(covs) >= 1
        assert covs[0]["minister_slug"] == "zanin" and covs[0]["year"] == 2024

    def test_no_match(self, tmp_dirs, raw_events):
        raw_dir, curated_dir = tmp_dirs
        build_agenda_events(raw_dir=raw_dir, curated_dir=curated_dir)
        with (curated_dir / "agenda_event.jsonl").open() as f:
            events = [json.loads(line) for line in f if line.strip()]
        m = events[0].get("process_refs_matched", [])
        assert len(m) >= 1 and m[0]["process_id"] is None
