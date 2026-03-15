"""Tests for jurisprudencia enrichment in curated builders."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.curated.build_decision_event import build_decision_event_records
from atlas_stf.curated.build_process import build_process_records


def test_build_process_records_with_juris_enrichment(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text(
        "processo,classe,data_autuacao\nAC 1,AC,2026-03-01\n",
        encoding="utf-8",
    )

    juris_index = {
        "AC 1": {
            "juris_inteiro_teor_url": "https://example.com/ac1.pdf",
            "juris_partes": "PARTE A vs PARTE B",
            "juris_legislacao_citada": "CF art 5",
            "juris_procedencia": "SAO PAULO - SP",
            "juris_classe_extenso": "ACAO CAUTELAR",
            "juris_publicacao_data": "2026-04-01",
            "juris_acompanhamento_url": "https://portal.stf.jus.br/processos/ac1",
            "juris_tese_texto": "Tese firmada",
            "juris_acordao_ata": "Ata do acordao",
            "juris_doc_count": 3,
            "juris_has_acordao": True,
            "juris_has_decisao_monocratica": True,
        },
    }

    records = build_process_records(staging_dir=staging_dir, juris_index=juris_index)

    assert len(records) == 1
    rec = records[0]
    assert rec["juris_inteiro_teor_url"] == "https://example.com/ac1.pdf"
    assert rec["juris_partes"] == "PARTE A vs PARTE B"
    assert rec["juris_doc_count"] == 3
    assert rec["juris_has_acordao"] is True
    assert rec["juris_publicacao_data"] == "2026-04-01"
    assert rec["juris_acompanhamento_url"] == "https://portal.stf.jus.br/processos/ac1"
    assert rec["juris_tese_texto"] == "Tese firmada"
    assert rec["juris_acordao_ata"] == "Ata do acordao"


def test_build_process_records_without_juris_has_null_fields(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text(
        "processo,classe,data_autuacao\nAC 1,AC,2026-03-01\n",
        encoding="utf-8",
    )

    records = build_process_records(staging_dir=staging_dir)

    rec = records[0]
    assert rec["juris_inteiro_teor_url"] is None
    assert rec["juris_doc_count"] is None
    assert rec["juris_has_acordao"] is None
    assert rec["juris_publicacao_data"] is None
    assert rec["juris_acompanhamento_url"] is None
    assert rec["juris_tese_texto"] is None
    assert rec["juris_acordao_ata"] is None


def test_build_decision_event_records_with_juris_enrichment(tmp_path: Path):
    staging_file = tmp_path / "decisoes.csv"
    staging_file.write_text(
        "idfatodecisao,processo,ano_da_decisao,data_da_decisao\n1,AC 1,2026,2026-03-06\n",
        encoding="utf-8",
    )

    decision_index = {
        "AC 1::2026-03-06": [
            {
                "juris_doc_id": "doc-abc",
                "juris_decisao_texto": "Texto completo da decisao",
                "juris_ementa_texto": None,
                "juris_inteiro_teor_url": "https://example.com/ac1.pdf",
                "juris_publicacao_data": "2026-04-01",
            },
        ],
    }

    records = build_decision_event_records(staging_file=staging_file, decision_index=decision_index)

    assert len(records) == 1
    rec = records[0]
    assert rec["juris_doc_id"] == "doc-abc"
    assert rec["juris_decisao_texto"] == "Texto completo da decisao"
    assert rec["juris_inteiro_teor_url"] == "https://example.com/ac1.pdf"
    assert rec["juris_publicacao_data"] == "2026-04-01"


def test_build_decision_event_records_without_juris_has_null_fields(tmp_path: Path):
    staging_file = tmp_path / "decisoes.csv"
    staging_file.write_text(
        "idfatodecisao,processo,ano_da_decisao,data_da_decisao\n1,AC 1,2026,2026-03-06\n",
        encoding="utf-8",
    )

    records = build_decision_event_records(staging_file=staging_file)

    rec = records[0]
    assert rec["juris_doc_id"] is None
    assert rec["juris_decisao_texto"] is None
    assert rec["juris_ementa_texto"] is None
    assert rec["juris_inteiro_teor_url"] is None
    assert rec["juris_publicacao_data"] is None


def test_build_decision_event_no_match_keeps_nulls(tmp_path: Path):
    staging_file = tmp_path / "decisoes.csv"
    staging_file.write_text(
        "idfatodecisao,processo,ano_da_decisao,data_da_decisao\n1,AC 1,2026,2026-03-06\n",
        encoding="utf-8",
    )

    decision_index = {
        "AC 1::2026-03-07": [
            {
                "juris_doc_id": "doc-wrong-date",
                "juris_decisao_texto": "Wrong date",
                "juris_ementa_texto": None,
                "juris_inteiro_teor_url": None,
                "juris_publicacao_data": "2026-04-01",
            },
        ],
    }

    records = build_decision_event_records(staging_file=staging_file, decision_index=decision_index)
    rec = records[0]
    assert rec["juris_doc_id"] is None
    assert rec["juris_publicacao_data"] is None
