from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.agenda._parser import (
    RAW_EVENT_SCHEMA_VERSION,
    _clean_html,
    _event_identity_base,
    _parse_time,
    canonicalize_process_ref,
    classify_event_category,
    classify_meeting_nature,
    extract_process_refs,
    normalize_raw_day,
)
from atlas_stf.core.identity import stable_id


class TestProcessRefExtraction:
    def test_basic(self):
        refs = extract_process_refs("Reuniao sobre ADPF 342")
        assert len(refs) == 1 and refs[0]["class"] == "ADPF" and refs[0]["number"] == "342"

    def test_n_dot(self):
        refs = extract_process_refs("ADPF n. 342/DF")
        assert refs[0]["number"] == "342"

    def test_no_accent(self):
        refs = extract_process_refs("RE no 123456")
        assert refs[0]["class"] == "RE"

    def test_multiple(self):
        assert len(extract_process_refs("ADI 1234 e HC 56789")) == 2

    def test_none(self):
        assert extract_process_refs("Reuniao administrativa") == []

    def test_canon_uf(self):
        assert canonicalize_process_ref("ADPF n. 342/DF") == ("ADPF", "342")

    def test_canon_dots(self):
        assert canonicalize_process_ref("RE 1.234.567") == ("RE", "1234567")

    def test_canon_all(self):
        for v in ["ADPF 342", "ADPF n. 342", "ADPF 342/DF", "ADPF no 342"]:
            assert canonicalize_process_ref(v) == ("ADPF", "342"), f"Failed: {v}"


class TestTaxonomy:
    def test_core(self):
        cat, *_ = classify_event_category("Sessao Plenaria", "Julgamento")
        assert cat == "institutional_core"

    def test_external(self):
        cat, _, _, pub, _ = classify_event_category("Reuniao com AGU", "")
        assert cat == "institutional_external_actor" and pub is True

    def test_private(self):
        cat, *_ = classify_event_category("Reuniao", "Assunto: ADPF 342 - Dr. Carlos Silva")
        assert cat == "private_advocacy"

    def test_mixed(self):
        cat, _, conf, pub, priv = classify_event_category("Reuniao", "AGU e Dr. Silva")
        assert cat == "unclear" and conf <= 0.4 and pub and priv

    def test_default(self):
        cat, *_ = classify_event_category("Evento", "Sem detalhes")
        assert cat == "unclear"

    def test_ceremony(self):
        assert classify_meeting_nature("Posse de ministro", "") == "ceremony"

    def test_academic(self):
        assert classify_meeting_nature("Seminario de direito", "") == "academic_event"


class TestHelpers:
    def test_html(self):
        assert _clean_html("<b>Hello</b> <i>World</i>") == "Hello World"

    def test_time(self):
        t = _parse_time("14h00")
        assert t is not None and t.hour == 14

    def test_time_none(self):
        assert _parse_time("") is None and _parse_time(None) is None


class TestNormalizeRawDay:
    def test_missing_event_description_is_explicit_limitation(self):
        events = normalize_raw_day(
            {
                "data": "02/03/2024",
                "descricaoData": "sábado",
                "fetched_at": "2026-04-01T00:00:00+00:00",
                "ministro": [
                    {
                        "nomeMinistro": "MIN. EDSON FACHIN",
                        "eventos": [{"titulo": "Audiência", "hora": "13h00"}],
                    }
                ],
            },
            [],
        )

        assert len(events) == 1
        assert events[0]["event_description"] is None
        assert events[0]["participants_raw"] == []
        assert events[0]["organizations_raw"] == []
        assert events[0]["normalization_version"] == RAW_EVENT_SCHEMA_VERSION

    def test_duplicate_signatures_receive_distinct_event_ids(self):
        day = {
            "data": "01/08/2024",
            "descricaoData": "quinta-feira",
            "fetched_at": "2026-04-01T00:00:00+00:00",
            "ministro": [
                {
                    "nomeMinistro": "MIN. EDSON FACHIN",
                    "eventos": [
                        {"titulo": "Audiência", "hora": "13h00"},
                        {"titulo": "Audiência", "hora": "13h00"},
                    ],
                }
            ],
        }

        events = normalize_raw_day(day, [])

        assert len(events) == 2
        assert events[0]["event_id"] != events[1]["event_id"]

    def test_unique_signature_keeps_legacy_event_id_shape(self):
        day = {
            "data": "02/03/2024",
            "descricaoData": "sábado",
            "fetched_at": "2026-04-01T00:00:00+00:00",
            "ministro": [
                {
                    "nomeMinistro": "MIN. EDSON FACHIN",
                    "eventos": [{"titulo": "Audiência", "hora": "13h00"}],
                }
            ],
        }

        events = normalize_raw_day(day, [])
        expected = stable_id("agd_", _event_identity_base("fachin", "2024-03-02", "13h00", "Audiência", ""))

        assert events[0]["event_id"] == expected


def test_curated_agenda_event_ids_are_unique_when_dataset_available():
    path = Path("data/curated/agenda_event.jsonl")
    if not path.exists():
        pytest.skip("dataset de agenda não está disponível no repositório local")

    ids: set[str] = set()
    for line in path.open(encoding="utf-8"):
        if not line.strip():
            continue
        row = json.loads(line)
        event_id = row["agenda_event_id"]
        assert event_id not in ids
        ids.add(event_id)
