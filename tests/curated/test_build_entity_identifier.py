from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.build_entity_identifier import build_entity_identifier_jsonl, build_entity_identifier_records


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def test_build_entity_identifier_records_extracts_labeled_cpf(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_jsonl(
        process_path,
        [
            {
                "process_id": "proc_1",
                "process_number": "AC 1",
            }
        ],
    )

    decisoes_dir = tmp_path / "juris" / "decisoes"
    _write_jsonl(
        decisoes_dir / "2026-03.jsonl",
        [
            {
                "_id": "doc-1",
                "processo_codigo_completo": "AC 1",
                "decisao_texto": "PACIENTE JOAO DA SILVA CPF: 529.982.247-25.",
                "inteiro_teor_url": "https://example.com/doc-1.pdf",
            }
        ],
    )

    records = build_entity_identifier_records(process_path=process_path, juris_dir=tmp_path / "juris")

    assert len(records) == 1
    record = records[0]
    assert record["process_id"] == "proc_1"
    assert record["process_number"] == "AC 1"
    assert record["identifier_kind"] == "cpf"
    assert record["identifier_value_normalized"] == "52998224725"
    assert record["source_doc_type"] == "decisoes"
    assert record["source_field"] == "decisao_texto"
    assert "CPF: 529.982.247-25" in record["context_snippet"]


def test_build_entity_identifier_records_extracts_labeled_cnpj_from_acordao(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_jsonl(
        process_path,
        [
            {
                "process_id": "proc_2",
                "process_number": "HC 200",
            }
        ],
    )

    acordaos_dir = tmp_path / "juris" / "acordaos"
    _write_jsonl(
        acordaos_dir / "2026-03.jsonl",
        [
            {
                "_id": "doc-2",
                "processo_codigo_completo": "HC 200",
                "inteiro_teor_texto": "EMPRESA ACME S.A. CNPJ 04.252.011/0001-10 consta dos autos.",
                "inteiro_teor_url": "https://example.com/doc-2.pdf",
            }
        ],
    )

    records = build_entity_identifier_records(process_path=process_path, juris_dir=tmp_path / "juris")

    assert len(records) == 1
    record = records[0]
    assert record["process_id"] == "proc_2"
    assert record["identifier_kind"] == "cnpj"
    assert record["identifier_value_normalized"] == "04252011000110"
    assert record["source_doc_type"] == "acordaos"
    assert record["source_field"] == "inteiro_teor_texto"


def test_build_entity_identifier_records_ignores_invalid_tax_ids(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_jsonl(
        process_path,
        [
            {
                "process_id": "proc_1",
                "process_number": "AC 1",
            }
        ],
    )

    decisoes_dir = tmp_path / "juris" / "decisoes"
    _write_jsonl(
        decisoes_dir / "2026-03.jsonl",
        [
            {
                "_id": "doc-1",
                "processo_codigo_completo": "AC 1",
                "decisao_texto": "CPF: 111.111.111-11 e CNPJ 11.111.111/1111-11.",
            }
        ],
    )

    records = build_entity_identifier_records(process_path=process_path, juris_dir=tmp_path / "juris")

    assert records == []


def test_build_entity_identifier_jsonl_writes_file(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_jsonl(
        process_path,
        [
            {
                "process_id": "proc_1",
                "process_number": "AC 1",
            }
        ],
    )
    decisoes_dir = tmp_path / "juris" / "decisoes"
    _write_jsonl(
        decisoes_dir / "2026-03.jsonl",
        [
            {
                "_id": "doc-1",
                "processo_codigo_completo": "AC 1",
                "decisao_texto": "CPF: 529.982.247-25.",
            }
        ],
    )

    output = tmp_path / "entity_identifier.jsonl"
    build_entity_identifier_jsonl(process_path=process_path, juris_dir=tmp_path / "juris", output_path=output)

    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["identifier_occurrence_id"].startswith("eid_")
