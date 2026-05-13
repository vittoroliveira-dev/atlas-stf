from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.core.identity import normalize_entity_name, stable_id
from atlas_stf.curated.build_links import (
    build_process_counsel_link_records,
    build_process_links_jsonl,
    build_process_party_link_records,
)


def _normalized(value: str) -> str:
    normalized = normalize_entity_name(value)
    if normalized is None:
        raise AssertionError(f"Expected normalized entity name for {value!r}")
    return normalized


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


def test_build_process_party_link_records_keeps_legacy_link_id_when_not_colliding(tmp_path: Path):
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

    expected_ids = {
        stable_id("ppl_", f"proc_1:{stable_id('party_', _normalized(name))}")
        for name in ("PARTE A", "PARTE B")
    }
    assert {record["link_id"] for record in records} == expected_ids


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


def test_build_process_party_link_records_preserves_distinct_roles_for_same_party(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): ESTADO X REQDO.(A/S): ESTADO X",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_process_party_link_records(process_path=process_path)

    assert len(records) == 2
    assert {record["role_in_case"] for record in records} == {"REQTE.(S)", "REQDO.(A/S)"}
    assert len({record["link_id"] for record in records}) == 2
    normalized = _normalized("ESTADO X")
    party_id = stable_id("party_", normalized)
    expected_ids = {
        stable_id("ppl_", f"proc_1:{party_id}:REQTE.(S)"),
        stable_id("ppl_", f"proc_1:{party_id}:REQDO.(A/S)"),
    }
    assert {record["link_id"] for record in records} == expected_ids


def test_build_process_party_link_records_is_order_invariant_for_colliding_roles(tmp_path: Path):
    process_path_a = tmp_path / "process_a.jsonl"
    process_path_b = tmp_path / "process_b.jsonl"
    process_a = {
        "process_id": "proc_1",
        "juris_partes": "REQTE.(S): ESTADO X REQDO.(A/S): ESTADO X",
    }
    process_b = {
        "process_id": "proc_1",
        "juris_partes": "REQDO.(A/S): ESTADO X REQTE.(S): ESTADO X",
    }
    process_path_a.write_text(json.dumps(process_a) + "\n", encoding="utf-8")
    process_path_b.write_text(json.dumps(process_b) + "\n", encoding="utf-8")

    records_a = build_process_party_link_records(process_path=process_path_a)
    records_b = build_process_party_link_records(process_path=process_path_b)

    normalized_a = {
        (record["party_id"], record["role_in_case"], record["link_id"]) for record in records_a
    }
    normalized_b = {
        (record["party_id"], record["role_in_case"], record["link_id"]) for record in records_b
    }
    assert normalized_a == normalized_b


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


def test_build_process_counsel_link_records_keeps_legacy_link_id_when_not_colliding(tmp_path: Path):
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

    expected_ids = {
        stable_id("pcl_", f"proc_1:{stable_id('csl_', _normalized(name))}")
        for name in ("ADVOGADO A", "ADVOGADO B")
    }
    assert {record["link_id"] for record in records} == expected_ids


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
    normalized = _normalized("ADV A")
    counsel_id = stable_id("csl_", normalized)
    assert records[0]["link_id"] == stable_id("pcl_", f"proc_1:{counsel_id}")


def test_build_process_counsel_link_records_preserves_distinct_sides_for_same_counsel(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): ESTADO X ADV.(A/S): ADV A REQDO.(A/S): UNIÃO ADV.(A/S): ADV A",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_process_counsel_link_records(process_path=process_path)

    assert len(records) == 2
    assert {record["side_in_case"] for record in records} == {"REQTE.(S)", "REQDO.(A/S)"}
    assert len({record["link_id"] for record in records}) == 2
    normalized = _normalized("ADV A")
    counsel_id = stable_id("csl_", normalized)
    expected_ids = {
        stable_id("pcl_", f"proc_1:{counsel_id}:REQTE.(S)"),
        stable_id("pcl_", f"proc_1:{counsel_id}:REQDO.(A/S)"),
    }
    assert {record["link_id"] for record in records} == expected_ids


def test_build_process_counsel_link_records_is_order_invariant_for_colliding_sides(tmp_path: Path):
    process_path_a = tmp_path / "process_a.jsonl"
    process_path_b = tmp_path / "process_b.jsonl"
    process_a = {
        "process_id": "proc_1",
        "juris_partes": "REQTE.(S): ESTADO X ADV.(A/S): ADV A REQDO.(A/S): UNIÃO ADV.(A/S): ADV A",
    }
    process_b = {
        "process_id": "proc_1",
        "juris_partes": "REQDO.(A/S): UNIÃO ADV.(A/S): ADV A REQTE.(S): ESTADO X ADV.(A/S): ADV A",
    }
    process_path_a.write_text(json.dumps(process_a) + "\n", encoding="utf-8")
    process_path_b.write_text(json.dumps(process_b) + "\n", encoding="utf-8")

    records_a = build_process_counsel_link_records(process_path=process_path_a)
    records_b = build_process_counsel_link_records(process_path=process_path_b)

    normalized_a = {
        (record["counsel_id"], record["side_in_case"], record["link_id"]) for record in records_a
    }
    normalized_b = {
        (record["counsel_id"], record["side_in_case"], record["link_id"]) for record in records_b
    }
    assert normalized_a == normalized_b


def test_build_process_counsel_link_records_uses_none_token_for_colliding_unspecified_side(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): ESTADO X ADV.(A/S): ADV A REQDO.(A/S): UNIÃO ADV.(A/S): ADV A",
                "juris_advogados": "ADV B",
                "juris_procuradores": "ADV B",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_process_counsel_link_records(process_path=process_path)

    normalized = _normalized("ADV B")
    counsel_id = stable_id("csl_", normalized)
    unspecified_records = [record for record in records if record["counsel_id"] == counsel_id]
    assert len(unspecified_records) == 1
    assert unspecified_records[0]["side_in_case"] is None
    assert unspecified_records[0]["link_id"] == stable_id("pcl_", f"proc_1:{counsel_id}")


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
