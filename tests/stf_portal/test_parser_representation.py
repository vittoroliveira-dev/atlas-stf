"""Tests for STF portal HTML parser — representation-network functions."""

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


def test_parse_partes_representantes_html_table():
    html = """
    <table>
    <tr><th>Parte</th><th>Qualificacao</th><th>Representante</th></tr>
    <tr><td>Estado de Sao Paulo</td><td>REQTE</td><td>Joao da Silva</td></tr>
    </table>
    """
    results = parse_partes_representantes_html(html)

    assert len(results) == 1
    assert results[0]["party_name"] == "Estado de Sao Paulo"
    assert results[0]["party_role"] == "REQTE"
    assert results[0]["lawyer_name"] == "Joao da Silva"


def test_parse_partes_representantes_html_with_oab_in_name():
    html = """
    <table>
    <tr><td>Uniao</td><td>REQDO</td><td>Maria Oliveira</td><td>OAB 12345/SP</td></tr>
    </table>
    """
    results = parse_partes_representantes_html(html)

    assert len(results) == 1
    assert results[0]["oab_number"] == "12345/SP"
    assert results[0]["oab_state"] == "SP"


def test_parse_partes_representantes_html_with_firm_name():
    html = """
    <table>
    <tr><td>Empresa X</td><td>REQTE</td><td>Ana Costa</td><td>OAB 999/RJ</td><td>Costa Advogados</td></tr>
    </table>
    """
    results = parse_partes_representantes_html(html)

    assert len(results) == 1
    assert results[0]["firm_name"] == "Costa Advogados"
    assert results[0]["affiliation_confidence"] == "low"


def test_parse_partes_representantes_html_empty():
    assert parse_partes_representantes_html("") == []
    assert parse_partes_representantes_html("<html><body>No data</body></html>") == []


def test_parse_partes_representantes_html_no_oab():
    html = """
    <table>
    <tr><td>Municipio Y</td><td>INTDO</td><td>Pedro Santos</td></tr>
    </table>
    """
    results = parse_partes_representantes_html(html)

    assert len(results) == 1
    assert results[0]["oab_number"] is None
    assert results[0]["oab_state"] is None
    assert results[0]["firm_name"] is None


# ---------------------------------------------------------------------------
# parse_peticoes_detailed_html
# ---------------------------------------------------------------------------


def test_parse_peticoes_detailed_html_table():
    html = """
    <table>
    <tr><td>15/03/2026</td><td>Joao da Silva</td><td>Recurso Extraordinario</td><td>PROT-123</td></tr>
    </table>
    """
    results = parse_peticoes_detailed_html(html)

    assert len(results) == 1
    assert results[0]["date"] == "2026-03-15"
    assert results[0]["petitioner_name"] == "Joao da Silva"
    assert results[0]["document_type"] == "Recurso Extraordinario"
    assert results[0]["protocol"] == "PROT-123"
    assert results[0]["tab_name"] == "Peticoes"


def test_parse_peticoes_detailed_html_empty():
    assert parse_peticoes_detailed_html("") == []
    assert parse_peticoes_detailed_html("<table></table>") == []


def test_parse_peticoes_detailed_html_no_date():
    html = """
    <table>
    <tr><td>invalid</td><td>Someone</td><td>Type</td></tr>
    </table>
    """
    results = parse_peticoes_detailed_html(html)

    assert results == []


# ---------------------------------------------------------------------------
# parse_oral_argument_html
# ---------------------------------------------------------------------------


def test_parse_oral_argument_html_table():
    html = """
    <table>
    <tr><td>Ana Costa</td><td>Estado X</td><td>20/04/2026</td><td>Plenario</td></tr>
    </table>
    """
    results = parse_oral_argument_html(html)

    assert len(results) == 1
    assert results[0]["lawyer_name"] == "Ana Costa"
    assert results[0]["party_represented"] == "Estado X"
    assert results[0]["session_date"] == "2026-04-20"
    assert results[0]["session_type"] == "Plenario"
    assert results[0]["tab_name"] == "Sustentacao Oral"


def test_parse_oral_argument_html_empty():
    assert parse_oral_argument_html("") == []
    assert parse_oral_argument_html("<html></html>") == []


def test_parse_oral_argument_html_no_date():
    html = """
    <table>
    <tr><td>Lawyer A</td><td>Party B</td><td>sem data</td></tr>
    </table>
    """
    results = parse_oral_argument_html(html)

    assert results == []


# ---------------------------------------------------------------------------
# build_process_document — new representation fields
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
        representantes=[{"party_name": "X", "lawyer_name": "Y"}],
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
        peticoes_detailed=[{"date": "2026-03-15", "petitioner_name": "Z"}],
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
