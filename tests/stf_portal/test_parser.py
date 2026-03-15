"""Tests for STF portal HTML parser."""

from __future__ import annotations

from atlas_stf.stf_portal._parser import (
    _clean_text,
    _parse_date,
    build_process_document,
    parse_andamentos_html,
    parse_informacoes_html,
)


def test_clean_text():
    assert _clean_text("<b>Hello</b> World") == "Hello World"
    assert _clean_text("  Multiple   spaces  ") == "Multiple spaces"
    assert _clean_text(None) is None
    assert _clean_text("") is None


def test_parse_date_dd_mm_yyyy():
    assert _parse_date("15/03/2026") == "2026-03-15"
    assert _parse_date("01/01/2020") == "2020-01-01"


def test_parse_date_iso():
    assert _parse_date("2026-03-15") == "2026-03-15"


def test_parse_date_none():
    assert _parse_date(None) is None
    assert _parse_date("") is None
    assert _parse_date("invalid") is None


def test_parse_andamentos_html():
    html = """
    <table>
    <tr><th>Data</th><th>Andamento</th></tr>
    <tr><td>15/03/2026</td><td>Distribuído por sorteio</td></tr>
    <tr><td>20/04/2026</td><td>Pedido de vista</td><td>Min. X</td></tr>
    </table>
    """
    events = parse_andamentos_html(html)
    assert len(events) == 2
    assert events[0]["date"] == "2026-03-15"
    assert events[0]["description"] == "Distribuído por sorteio"
    assert events[0]["tab_name"] == "Andamentos"
    assert events[1]["detail"] == "Min. X"


def test_parse_andamentos_empty():
    assert parse_andamentos_html("") == []
    assert parse_andamentos_html("<html><body>No table</body></html>") == []


def test_parse_informacoes_html():
    html = """
    <dl>
    <dt>Classe</dt><dd>ADI</dd>
    <dt>Relator</dt><dd>Min. Alexandre de Moraes</dd>
    <dt>Origem</dt><dd>Assembleia Legislativa de São Paulo</dd>
    </dl>
    """
    info = parse_informacoes_html(html)
    assert info.get("classe") == "ADI"
    assert info.get("relator_atual") == "Min. Alexandre de Moraes"
    assert info.get("origem") == "Assembleia Legislativa de São Paulo"


def test_build_process_document():
    doc = build_process_document(
        process_number="ADI 1234",
        source_url="https://portal.stf.jus.br/processos/detalhe.asp?incidente=12345",
        raw_html="<html></html>",
        andamentos=[{"date": "2026-03-15", "description": "Distribuído"}],
        deslocamentos=[],
        peticoes=[],
        sessao_virtual=[],
        informacoes={"classe": "ADI"},
    )
    assert doc["process_number"] == "ADI 1234"
    assert doc["source_system"] == "stf_portal"
    assert doc["source_url"].startswith("https://")
    assert doc["raw_html_hash"] is not None
    assert len(doc["andamentos"]) == 1
    assert doc["fetched_at"] is not None
