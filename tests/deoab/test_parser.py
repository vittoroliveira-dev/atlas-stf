"""Tests for DEOAB gazette parser."""

from __future__ import annotations

from atlas_stf.deoab._parser import (
    _clean_pdf_artifacts,
    canonicalize_oab,
    format_oab,
    parse_sociedade_records,
)

# --- canonicalize_oab ---


def test_canonicalize_oab_standard():
    assert canonicalize_oab("OAB/SP 145.785") == [("145785", "SP")]


def test_canonicalize_oab_glued():
    result = canonicalize_oab("OABAM \u2013 20075")
    assert ("20075", "AM") in result


def test_canonicalize_oab_numero():
    assert canonicalize_oab("OAB n\u00ba 123.456 RJ") == [("123456", "RJ")]


def test_canonicalize_oab_space():
    assert canonicalize_oab("OAB SC 65.119") == [("65119", "SC")]


def test_canonicalize_oab_dedup():
    """Same OAB in two formats should not duplicate."""
    result = canonicalize_oab("OAB/SP 145.785 and OAB SP 145.785")
    assert len(result) == 1
    assert result[0] == ("145785", "SP")


def test_canonicalize_oab_multiple():
    result = canonicalize_oab("OAB/SP 111 and OAB/RJ 222")
    assert len(result) == 2


def test_canonicalize_oab_no_match():
    assert canonicalize_oab("no OAB here") == []


# --- format_oab ---


def test_format_oab():
    assert format_oab("12345", "SP") == "12345/SP"


# --- _clean_pdf_artifacts ---


def test_clean_pdf_artifacts_signature():
    text = (
        "Some text Documento assinado digitalmente conforme MP n\u00ba2.200-2 de 24/08/2001, "
        "que instituiu a Infraestrutura de Chaves P\u00fablicas Brasileira - ICP-Brasil more text"
    )
    cleaned = _clean_pdf_artifacts(text)
    assert "Documento assinado" not in cleaned
    assert "Some text" in cleaned
    assert "more text" in cleaned


def test_clean_pdf_artifacts_page_header():
    text = "DI\u00c1RIO ELETR\u00d4NICO DA OAB   ter\u00e7a-feira, 10 de mar\u00e7o de 2026 | Pagina: 25"
    cleaned = _clean_pdf_artifacts(text)
    assert "Pagina:" not in cleaned


def test_clean_pdf_artifacts_preserves_normal():
    text = "Normal text without any artifacts"
    assert _clean_pdf_artifacts(text) == text


# --- parse_sociedade_records (denominada) ---


def test_parse_sociedade_records_denominada():
    text = (
        'Sociedade Individual de Advocacia denominada \u201cTESTE SOCIEDADE INDIVIDUAL DE ADVOCACIA\u201d, '
        "de titularidade do(a) advogado (a), FULANO DE TAL, OABAM \u201320075, "
        "regularmente inscrito nesta Seccional."
    )
    records = parse_sociedade_records(text, "https://example.com/test.pdf", "2026-01-01")
    assert len(records) >= 1
    r = records[0]
    assert r["sociedade_nome"] == "TESTE SOCIEDADE INDIVIDUAL DE ADVOCACIA"
    assert r["oab_number"] == "20075/AM"
    assert r["sociedade_tipo"] == "individual"
    assert r["tipo_ato"] == "registro"
    assert r["confidence"] == 0.95
    assert r["data_publicacao"] == "2026-01-01"


# --- parse_sociedade_records (compact) — Codex finding ---


def test_parse_sociedade_records_compacto():
    text = (
        "Reg. n\u00ba 53644 - Ribeiro Goncalves Sociedade Individual de Advocacia - Bauru/SP - "
        "Reg. n\u00ba 53679 - Rasinovsky, Calil Carvalho Sociedade de Advogados - S\u00e3o Paulo/SP"
    )
    records = parse_sociedade_records(text, "https://example.com/test.pdf", "2026-06-19")
    assert len(records) >= 2
    r0 = records[0]
    assert r0["sociedade_registro"] == "53644"
    assert "Ribeiro Goncalves" in r0["sociedade_nome"]
    assert r0["cidade"] == "Bauru"
    assert r0["seccional"] == "SP"
    assert r0["confidence"] == 0.85
    r1 = records[1]
    assert r1["sociedade_registro"] == "53679"
    assert "Rasinovsky" in r1["sociedade_nome"]


# --- parse_sociedade_records (empty) ---


def test_parse_sociedade_records_empty():
    records = parse_sociedade_records("Nothing relevant here", "https://x.com/t.pdf", "2026-01-01")
    assert records == []
