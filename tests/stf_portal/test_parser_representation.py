"""Tests for STF portal HTML parser — representation-network functions.

Test HTML mirrors the real portal structure (div-based, not tables).
Patterns validated against live portal 2026-03.
"""

from __future__ import annotations

from atlas_stf.stf_portal._parser import (
    build_process_document,
    parse_oral_argument_html,
    parse_partes_representantes_html,
    parse_peticoes_detailed_html,
)

# ---------------------------------------------------------------------------
# parse_partes_representantes_html
# ---------------------------------------------------------------------------


def test_parse_partes_representantes_html_basic():
    html = """
    <div class="processo-partes lista-dados m-l-16 p-t-0">
        <div class="detalhe-parte">REQTE.(S)</div>
        <div class="nome-parte">ESTADO DE SAO PAULO</div>
    </div>
    """
    results = parse_partes_representantes_html(html)

    assert len(results) == 1
    assert results[0]["party_name"] == "ESTADO DE SAO PAULO"
    assert results[0]["party_role"] == "REQTE.(S)"
    assert results[0]["oab_number"] is None


def test_parse_partes_representantes_html_with_oab():
    html = """
    <div class="processo-partes lista-dados m-l-16 p-t-0">
        <div class="detalhe-parte">ADV.(A/S)</div>
        <div class="nome-parte">AMANDA SOUTO BALIZA (36578/GO)</div>
    </div>
    """
    results = parse_partes_representantes_html(html)

    assert len(results) == 1
    assert results[0]["party_name"] == "AMANDA SOUTO BALIZA"
    assert results[0]["party_role"] == "ADV.(A/S)"
    assert results[0]["oab_number"] == "36578/GO"
    assert results[0]["oab_state"] == "GO"


def test_parse_partes_representantes_html_multiple():
    html = """
    <div id="todas-partes">
        <div class="processo-partes lista-dados m-l-16 p-t-0">
            <div class="detalhe-parte">REQTE.(S)</div>
            <div class="nome-parte">ALIANCA NACIONAL LGBTI</div>
        </div>
        <div class="processo-partes lista-dados m-l-16 p-t-0">
            <div class="detalhe-parte">ADV.(A/S)</div>
            <div class="nome-parte">PAULO IOTTI (242668/SP)</div>
        </div>
        <div class="processo-partes lista-dados m-l-16 p-t-0">
            <div class="detalhe-parte">INTDO.(A/S)</div>
            <div class="nome-parte">ASSEMBLEIA LEGISLATIVA</div>
        </div>
    </div>
    """
    results = parse_partes_representantes_html(html)

    assert len(results) == 3
    assert results[0]["oab_number"] is None
    assert results[1]["oab_number"] == "242668/SP"
    assert results[2]["party_role"] == "INTDO.(A/S)"


def test_parse_partes_representantes_html_empty():
    assert parse_partes_representantes_html("") == []
    assert parse_partes_representantes_html("<html><body>No data</body></html>") == []


# ---------------------------------------------------------------------------
# parse_peticoes_detailed_html
# ---------------------------------------------------------------------------


def test_parse_peticoes_detailed_html():
    html = """
    <div class="col-md-12 lista-dados">
        <div class="col-6">
            <span class="processo-detalhes-bold">78721/2025</span>
            <span class="processo-detalhes">Peticionado em 09/06/2025</span>
        </div>
        <div class="col-6">
            <span class="processo-detalhes">Recebido em 09/06/2025 11:21:15 por SEÇÃO X</span>
        </div>
    </div>
    """
    results = parse_peticoes_detailed_html(html)

    assert len(results) == 1
    assert results[0]["date"] == "2025-06-09"
    assert results[0]["protocol"] == "78721/2025"
    assert results[0]["tab_name"] == "Peticoes"


def test_parse_peticoes_detailed_html_empty():
    assert parse_peticoes_detailed_html("") == []
    assert parse_peticoes_detailed_html("<div></div>") == []


# ---------------------------------------------------------------------------
# parse_oral_argument_html
# ---------------------------------------------------------------------------


def test_parse_oral_argument_html_returns_empty():
    """abaSessao is JS-rendered; static parsing returns empty."""
    html = """
    <input type="hidden" id="env" value="p">
    <div class="col-12"><script>$.ajax({...});</script></div>
    """
    results = parse_oral_argument_html(html)
    assert results == []


def test_parse_oral_argument_html_empty():
    assert parse_oral_argument_html("") == []
    assert parse_oral_argument_html("<html></html>") == []


# ---------------------------------------------------------------------------
# build_process_document — representation fields
# ---------------------------------------------------------------------------


def test_build_process_document_includes_representantes():
    doc = build_process_document(
        process_number="ADI 1234",
        source_url="https://portal.stf.jus.br/x",
        raw_html="<html></html>",
        andamentos=[],
        deslocamentos=[],
        peticoes=[],
        sessao_virtual=[],
        informacoes={},
        representantes=[{"party_name": "X", "party_role": "REQTE"}],
    )

    assert "representantes" in doc
    assert len(doc["representantes"]) == 1


def test_build_process_document_includes_peticoes_detailed():
    doc = build_process_document(
        process_number="ADI 1234",
        source_url="https://portal.stf.jus.br/x",
        raw_html="<html></html>",
        andamentos=[],
        deslocamentos=[],
        peticoes=[],
        sessao_virtual=[],
        informacoes={},
        peticoes_detailed=[{"date": "2026-03-15", "protocol": "123/2026"}],
    )

    assert "peticoes_detailed" in doc
    assert len(doc["peticoes_detailed"]) == 1


def test_build_process_document_includes_oral_arguments():
    doc = build_process_document(
        process_number="ADI 1234",
        source_url="https://portal.stf.jus.br/x",
        raw_html="<html></html>",
        andamentos=[],
        deslocamentos=[],
        peticoes=[],
        sessao_virtual=[],
        informacoes={},
        oral_arguments=[{"lawyer_name": "A", "session_date": "2026-03-15"}],
    )

    assert "oral_arguments" in doc
    assert len(doc["oral_arguments"]) == 1


def test_build_process_document_omits_new_fields_when_not_provided():
    doc = build_process_document(
        process_number="ADI 1234",
        source_url="https://portal.stf.jus.br/x",
        raw_html="<html></html>",
        andamentos=[],
        deslocamentos=[],
        peticoes=[],
        sessao_virtual=[],
        informacoes={},
    )

    assert "representantes" not in doc
    assert "peticoes_detailed" not in doc
    assert "oral_arguments" not in doc
