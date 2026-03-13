from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.core.identity import build_identity_key, stable_id
from atlas_stf.curated.build_party import build_party_jsonl, build_party_records


def test_build_party_records_extracts_parties_from_jurisprudencia_text(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "PARTE A vs PARTE B",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_party_records(process_path=process_path)

    assert len(records) == 2
    assert {record["party_name_normalized"] for record in records} == {"PARTE A", "PARTE B"}


def test_build_party_records_extracts_labeled_process_parties(tmp_path: Path):
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

    records = build_party_records(process_path=process_path)

    assert {record["party_name_normalized"] for record in records} == {"ESTADO X", "UNIÃO"}


def test_build_party_records_extracts_labeled_process_parties_with_spaced_colon(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "AGTE.(S) : JULIANA MATIAS AGDO.(A/S) : JOÃO ALFREDO",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_party_records(process_path=process_path)

    assert {record["party_name_normalized"] for record in records} == {"JULIANA MATIAS", "JOÃO ALFREDO"}


def test_build_party_jsonl_writes_file(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "PARTE A vs PARTE B",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    output = tmp_path / "party.jsonl"
    build_party_jsonl(process_path=process_path, output_path=output)

    payload = json.loads(output.read_text(encoding="utf-8").splitlines()[0])
    assert payload["party_id"].startswith("party_")


def test_build_party_records_populates_identity_fields(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): PETRÓLEO BRASILEIRO S.A.",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_party_records(process_path=process_path)

    assert len(records) == 1
    record = records[0]
    assert record["canonical_name_normalized"] == "PETRÓLEO BRASILEIRO"
    assert record["entity_tax_id"] is None
    assert record["identity_strategy"] == "name"
    assert record["identity_key"] == build_identity_key("PETRÓLEO BRASILEIRO S.A.")
    assert record["party_id"] == stable_id("party_", record["party_name_normalized"])


def test_build_party_records_does_not_store_role_in_notes(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): EMPRESA X REQDO.(A/S): EMPRESA X",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_party_records(process_path=process_path)

    assert len(records) == 1
    assert records[0]["notes"] is None
