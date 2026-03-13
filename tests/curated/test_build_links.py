from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.build_links import (
    build_process_counsel_link_records,
    build_process_links_jsonl,
    build_process_party_link_records,
)


def test_build_process_party_link_records_extracts_links(tmp_path: Path):
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

    records = build_process_party_link_records(process_path=process_path)

    assert len(records) == 2
    assert all(record["process_id"] == "proc_1" for record in records)


def test_build_process_party_link_records_preserves_role_when_labeled(tmp_path: Path):
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

    records = build_process_party_link_records(process_path=process_path)

    assert {record["role_in_case"] for record in records} == {"REQTE.(S)", "REQDO.(A/S)"}


def test_build_process_party_link_records_deduplicates_same_party(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): ESTADO X REQTE.(S): ESTADO X REQDO.(A/S): UNIÃO",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_process_party_link_records(process_path=process_path)

    assert len(records) == 2


def test_build_process_counsel_link_records_extracts_links(tmp_path: Path):
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

    records = build_process_counsel_link_records(process_path=process_path)

    assert len(records) == 2
    assert all(record["process_id"] == "proc_1" for record in records)


def test_build_process_counsel_link_records_preserves_party_side_when_labeled(tmp_path: Path):
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

    records = build_process_counsel_link_records(process_path=process_path)

    assert {record["side_in_case"] for record in records} == {"REQTE.(S)", "REQDO.(A/S)"}


def test_build_process_counsel_link_records_deduplicates_same_counsel_across_sources(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): ESTADO X ADV.(A/S): ADV A",
                "juris_advogados": "ADV A; ADV A",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_process_counsel_link_records(process_path=process_path)

    assert len(records) == 1
    assert records[0]["side_in_case"] == "REQTE.(S)"


def test_build_process_links_jsonl_writes_both_files(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "PARTE A vs PARTE B",
                "juris_advogados": "ADVOGADO A",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    party_output = tmp_path / "process_party_link.jsonl"
    counsel_output = tmp_path / "process_counsel_link.jsonl"
    build_process_links_jsonl(
        process_path=process_path,
        party_output_path=party_output,
        counsel_output_path=counsel_output,
    )

    assert party_output.exists()
    assert counsel_output.exists()
