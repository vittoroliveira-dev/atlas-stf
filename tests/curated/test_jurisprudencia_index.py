from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.jurisprudencia_index import (
    build_decision_index,
    build_process_index,
    normalize_process_code,
)


def test_normalize_process_code_simple():
    assert normalize_process_code("EP 75") == "EP 75"


def test_normalize_process_code_strips_incident_suffix():
    assert normalize_process_code("HC 266401 AgR") == "HC 266401"


def test_normalize_process_code_uppercases():
    assert normalize_process_code("  are 1590806  ") == "ARE 1590806"


def test_normalize_process_code_preserves_non_matching():
    assert normalize_process_code("12345") == "12345"


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def test_build_process_index(tmp_path: Path):
    decisoes_dir = tmp_path / "decisoes"
    acordaos_dir = tmp_path / "acordaos"
    _write_jsonl(
        decisoes_dir / "2024-01.jsonl",
        [
            {
                "processo_codigo_completo": "AC 1",
                "inteiro_teor_url": "https://example.com/ac1.pdf",
                "partes_lista_texto": "PARTE A vs PARTE B",
                "documental_legislacao_citada_texto": "CF art 5",
                "procedencia_geografica_completo": "SAO PAULO - SP",
                "processo_classe_processual_unificada_extenso": "ACAO CAUTELAR",
            },
            {
                "processo_codigo_completo": "AC 1",
                "inteiro_teor_url": "https://example.com/ac1-v2.pdf",
                "partes_lista_texto": "OTHER PARTIES",
            },
        ],
    )
    _write_jsonl(
        acordaos_dir / "2024-01.jsonl",
        [
            {
                "processo_codigo_completo": "HC 200",
                "inteiro_teor_url": "https://example.com/hc200.pdf",
                "ementa_texto": "Ementa do HC 200",
            },
        ],
    )

    index = build_process_index(tmp_path)

    assert len(index) == 2
    ac1 = index["AC 1"]
    assert ac1["juris_inteiro_teor_url"] == "https://example.com/ac1.pdf"
    assert ac1["juris_partes"] == "PARTE A vs PARTE B"
    assert ac1["juris_legislacao_citada"] == "CF art 5"
    assert ac1["juris_procedencia"] == "SAO PAULO - SP"
    assert ac1["juris_classe_extenso"] == "ACAO CAUTELAR"
    assert ac1["juris_doc_count"] == 2
    assert ac1["juris_has_decisao_monocratica"] is True
    assert ac1["juris_has_acordao"] is False

    hc200 = index["HC 200"]
    assert hc200["juris_doc_count"] == 1
    assert hc200["juris_has_acordao"] is True
    assert hc200["juris_has_decisao_monocratica"] is False


def test_build_decision_index(tmp_path: Path):
    decisoes_dir = tmp_path / "decisoes"
    _write_jsonl(
        decisoes_dir / "2024-01.jsonl",
        [
            {
                "processo_codigo_completo": "AC 1",
                "julgamento_data": "2024-01-15",
                "decisao_texto": "Texto da decisao monocratica",
                "_id": "doc-abc",
                "inteiro_teor_url": "https://example.com/ac1.pdf",
            },
        ],
    )
    acordaos_dir = tmp_path / "acordaos"
    _write_jsonl(
        acordaos_dir / "2024-01.jsonl",
        [
            {
                "processo_codigo_completo": "HC 200",
                "julgamento_data": "2024-01-20",
                "ementa_texto": "Ementa do acordao",
                "_id": "doc-xyz",
                "inteiro_teor_url": "https://example.com/hc200.pdf",
            },
        ],
    )

    index = build_decision_index(tmp_path)

    assert "AC 1::2024-01-15" in index
    entries = index["AC 1::2024-01-15"]
    assert len(entries) == 1
    assert entries[0]["juris_decisao_texto"] == "Texto da decisao monocratica"
    assert entries[0]["juris_doc_id"] == "doc-abc"
    assert entries[0]["juris_ementa_texto"] is None

    hc_entries = index["HC 200::2024-01-20"]
    assert hc_entries[0]["juris_ementa_texto"] == "Ementa do acordao"
    assert hc_entries[0]["juris_decisao_texto"] is None


def test_build_process_index_empty_dir(tmp_path: Path):
    index = build_process_index(tmp_path / "nonexistent")
    assert index == {}


def test_build_decision_index_skips_docs_without_date(tmp_path: Path):
    decisoes_dir = tmp_path / "decisoes"
    _write_jsonl(
        decisoes_dir / "2024-01.jsonl",
        [
            {
                "processo_codigo_completo": "AC 1",
                "decisao_texto": "Texto sem data",
                "_id": "doc-no-date",
            },
        ],
    )

    index = build_decision_index(tmp_path)
    assert len(index) == 0


def test_incident_suffix_matches_base_process(tmp_path: Path):
    """HC 266401 AgR in jurisprudencia should match HC 266401 from CSV."""
    decisoes_dir = tmp_path / "decisoes"
    _write_jsonl(
        decisoes_dir / "2024-01.jsonl",
        [
            {
                "processo_codigo_completo": "HC 266401 AgR",
                "inteiro_teor_url": "https://example.com/hc.pdf",
                "julgamento_data": "2024-01-10",
                "decisao_texto": "Agravo regimental...",
                "_id": "agr-1",
            },
        ],
    )

    proc_index = build_process_index(tmp_path)
    assert "HC 266401" in proc_index

    dec_index = build_decision_index(tmp_path)
    assert "HC 266401::2024-01-10" in dec_index
