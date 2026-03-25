"""Tests for classified dedup in donation event loader."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from atlas_stf.serving._builder_loaders_analytics_sanctions import load_donation_events


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


_BASE_EVENT = {
    "match_id": "m1",
    "election_year": 2022,
    "donation_date": "2022-08-15",
    "donation_amount": 500.0,
    "candidate_name": "CANDIDATO A",
    "party_abbrev": "PT",
    "position": "DEPUTADO FEDERAL",
    "state": "SP",
    "donor_name": "DOADOR X",
    "donor_cpf_cnpj": "12345678900",
}


def test_donation_events_dedup_exact_duplicate(tmp_path: Path):
    """Two records with same event_id and identical content: only 1 loaded."""
    analytics_dir = tmp_path / "analytics"
    record = {"event_id": "ev_1", **_BASE_EVENT}
    _write_jsonl(analytics_dir / "donation_event.jsonl", [record, record])

    events = load_donation_events(analytics_dir)

    assert len(events) == 1
    assert events[0].event_id == "ev_1"


def test_donation_events_dedup_conflict(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    """Two records with same event_id but different amounts: 1 loaded, warning logged."""
    analytics_dir = tmp_path / "analytics"
    record_a = {"event_id": "ev_1", **_BASE_EVENT, "donation_amount": 500.0}
    record_b = {"event_id": "ev_1", **_BASE_EVENT, "donation_amount": 1000.0}
    _write_jsonl(analytics_dir / "donation_event.jsonl", [record_a, record_b])

    with caplog.at_level(logging.WARNING, logger="atlas_stf.serving._builder_loaders_analytics_sanctions"):
        events = load_donation_events(analytics_dir)

    assert len(events) == 1
    assert events[0].event_id == "ev_1"
    assert events[0].donation_amount == 500.0
    assert any("conflict" in msg.lower() for msg in caplog.messages)


def test_donation_events_dedup_mixed(tmp_path: Path):
    """Exact duplicate + conflict + unique: loads 2, skips 2."""
    analytics_dir = tmp_path / "analytics"
    record_a = {"event_id": "ev_1", **_BASE_EVENT}
    record_a_dup = {"event_id": "ev_1", **_BASE_EVENT}
    record_a_conflict = {"event_id": "ev_1", **_BASE_EVENT, "donation_amount": 999.0}
    record_b = {"event_id": "ev_2", **_BASE_EVENT, "donation_amount": 200.0}
    _write_jsonl(
        analytics_dir / "donation_event.jsonl",
        [record_a, record_a_dup, record_a_conflict, record_b],
    )

    events = load_donation_events(analytics_dir)

    assert len(events) == 2
    ids = {e.event_id for e in events}
    assert ids == {"ev_1", "ev_2"}


def test_donation_events_empty_event_id_skipped(tmp_path: Path):
    """Records with empty event_id are skipped entirely."""
    analytics_dir = tmp_path / "analytics"
    record_ok = {"event_id": "ev_1", **_BASE_EVENT}
    record_empty = {"event_id": "", **_BASE_EVENT}
    _write_jsonl(analytics_dir / "donation_event.jsonl", [record_ok, record_empty])

    events = load_donation_events(analytics_dir)

    assert len(events) == 1
    assert events[0].event_id == "ev_1"


def test_donation_events_no_duplicates(tmp_path: Path):
    """All unique records: all loaded, no dedup log."""
    analytics_dir = tmp_path / "analytics"
    records = [{"event_id": f"ev_{i}", **_BASE_EVENT, "donation_amount": float(i * 100)} for i in range(5)]
    _write_jsonl(analytics_dir / "donation_event.jsonl", records)

    events = load_donation_events(analytics_dir)

    assert len(events) == 5
