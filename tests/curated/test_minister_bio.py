"""Tests for minister bio validation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.curated.minister_bio import (
    _validate_entry,
    build_minister_bio_index,
)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


class TestValidateEntry:
    def test_valid_entry(self):
        entry = {
            "minister_name": "TEST",
            "appointment_date": "2024-01-01",
            "appointing_president": "Presidente X",
        }
        assert _validate_entry("TEST", entry) == []

    def test_missing_required(self):
        entry = {"minister_name": "TEST"}
        errors = _validate_entry("TEST", entry)
        assert len(errors) >= 2

    def test_invalid_party_history_type(self):
        entry = {
            "minister_name": "TEST",
            "appointment_date": "2024-01-01",
            "appointing_president": "X",
            "political_party_history": "PT",  # should be list
        }
        errors = _validate_entry("TEST", entry)
        assert any("political_party_history" in e for e in errors)


class TestBuildMinisterBioIndex:
    def test_full_coverage(self, tmp_path: Path):
        bio = {
            "MINISTRO_A": {
                "minister_name": "MINISTRO_A",
                "appointment_date": "2020-01-01",
                "appointing_president": "Presidente X",
            }
        }
        bio_path = tmp_path / "minister_bio.json"
        bio_path.write_text(json.dumps(bio), encoding="utf-8")

        events = [
            {"decision_event_id": "evt_1", "current_rapporteur": "MINISTRO_A"},
        ]
        evt_path = tmp_path / "decision_event.jsonl"
        _write_jsonl(evt_path, events)

        result = build_minister_bio_index(bio_path=bio_path, decision_event_path=evt_path)
        assert result.total_ministers_in_bio == 1
        assert result.total_ministers_in_data == 1
        assert result.covered_count == 1
        assert result.missing_from_bio == []
        assert result.schema_errors == []

    def test_missing_ministers(self, tmp_path: Path):
        bio = {
            "MINISTRO_A": {
                "minister_name": "MINISTRO_A",
                "appointment_date": "2020-01-01",
                "appointing_president": "Presidente X",
            }
        }
        bio_path = tmp_path / "minister_bio.json"
        bio_path.write_text(json.dumps(bio), encoding="utf-8")

        events = [
            {"decision_event_id": "evt_1", "current_rapporteur": "MINISTRO_A"},
            {"decision_event_id": "evt_2", "current_rapporteur": "MINISTRO_B"},
        ]
        evt_path = tmp_path / "decision_event.jsonl"
        _write_jsonl(evt_path, events)

        result = build_minister_bio_index(bio_path=bio_path, decision_event_path=evt_path)
        assert result.covered_count == 1
        assert "MINISTRO_B" in result.missing_from_bio

    def test_file_not_found(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            build_minister_bio_index(bio_path=tmp_path / "nonexistent.json")

    def test_schema_errors(self, tmp_path: Path):
        bio = {
            "BAD": {
                "minister_name": "BAD",
                # missing appointment_date and appointing_president
            }
        }
        bio_path = tmp_path / "minister_bio.json"
        bio_path.write_text(json.dumps(bio), encoding="utf-8")

        evt_path = tmp_path / "decision_event.jsonl"
        _write_jsonl(evt_path, [])

        result = build_minister_bio_index(bio_path=bio_path, decision_event_path=evt_path)
        assert len(result.schema_errors) >= 2

    def test_actual_bio_file_validates(self):
        """Validate the actual seed data file."""
        bio_path = Path("data/curated/minister_bio.json")
        if not bio_path.exists():
            pytest.skip("Seed data not available")
        result = build_minister_bio_index(
            bio_path=bio_path,
            decision_event_path=Path("/tmp/nonexistent_evt.jsonl"),
        )
        assert result.schema_errors == []
        assert result.total_ministers_in_bio >= 11
