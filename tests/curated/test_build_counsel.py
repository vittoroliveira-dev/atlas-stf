from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.core.identity import build_identity_key, stable_id
from atlas_stf.curated.build_counsel import build_counsel_jsonl, build_counsel_records


def test_build_counsel_records_returns_empty_without_source_fields(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(json.dumps({"process_id": "proc_1"}) + "\n", encoding="utf-8")

    records = build_counsel_records(process_path=process_path)

    assert records == []


def test_build_counsel_records_extracts_supported_source_field(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_advogados": "ADVOGADO A; ADVOGADO B",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_counsel_records(process_path=process_path)

    assert len(records) == 2
    assert {record["counsel_name_normalized"] for record in records} == {"ADVOGADO A", "ADVOGADO B"}


def test_build_counsel_records_extracts_labeled_jurisprudencia_counsels(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): ESTADO X ADV.(A/S): ADV A REQDO.(A/S): UNIÃO ADV.(A/S): ADV B",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_counsel_records(process_path=process_path)

    assert {record["counsel_name_normalized"] for record in records} == {"ADV A", "ADV B"}


def test_build_counsel_jsonl_writes_empty_file(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(json.dumps({"process_id": "proc_1"}) + "\n", encoding="utf-8")

    output = tmp_path / "counsel.jsonl"
    build_counsel_jsonl(process_path=process_path, output_path=output)

    assert output.read_text(encoding="utf-8") == ""


def test_build_counsel_records_populates_identity_fields(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_advogados": "JOÃO DA SILVA",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_counsel_records(process_path=process_path)

    assert len(records) == 1
    record = records[0]
    assert record["canonical_name_normalized"] == "JOAO DA SILVA"
    assert record["entity_tax_id"] is None
    assert record["identity_strategy"] == "name"
    assert record["identity_key"] == build_identity_key("JOÃO DA SILVA")
    assert record["counsel_id"] == stable_id("csl_", record["counsel_name_normalized"])
