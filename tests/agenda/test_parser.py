from __future__ import annotations

from atlas_stf.agenda._parser import (
    _clean_html,
    _parse_time,
    canonicalize_process_ref,
    classify_event_category,
    classify_meeting_nature,
    extract_process_refs,
)


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
