from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.build_entity_identifier_reconciliation import (
    build_entity_identifier_reconciliation_jsonl,
    build_entity_identifier_reconciliation_records,
)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def test_reconciliation_proposes_party_link_on_exact_process_local_match(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    _write_jsonl(
        curated_dir / "entity_identifier.jsonl",
        [
            {
                "identifier_occurrence_id": "eid_1",
                "process_id": "proc_1",
                "process_number": "AC 1",
                "identifier_kind": "cpf",
                "identifier_value_raw": "529.982.247-25",
                "identifier_value_normalized": "52998224725",
                "context_snippet": "JOAO DA SILVA CPF: 529.982.247-25",
                "entity_name_hint": "JOAO DA SILVA",
                "source_doc_type": "decisoes",
                "source_file": "2026-03.jsonl",
                "source_field": "decisao_texto",
                "source_url": None,
                "juris_doc_id": "doc-1",
                "extraction_confidence": 0.95,
                "extraction_method": "regex_labeled_tax_id",
                "uncertainty_note": None,
                "created_at": "2026-03-09T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [
            {
                "party_id": "party_1",
                "party_name_raw": "Joao da Silva",
                "party_name_normalized": "JOAO DA SILVA",
                "canonical_name_normalized": "JOAO DA SILVA",
            }
        ],
    )
    _write_jsonl(curated_dir / "counsel.jsonl", [])
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {
                "link_id": "ppl_1",
                "process_id": "proc_1",
                "party_id": "party_1",
                "role_in_case": "REQTE.(S)",
                "source_id": "juris",
            }
        ],
    )
    _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

    records = build_entity_identifier_reconciliation_records(
        entity_identifier_path=curated_dir / "entity_identifier.jsonl",
        party_path=curated_dir / "party.jsonl",
        counsel_path=curated_dir / "counsel.jsonl",
        process_party_link_path=curated_dir / "process_party_link.jsonl",
        process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
    )

    assert len(records) == 1
    record = records[0]
    assert record["proposal_status"] == "proposed"
    assert record["entity_kind"] == "party"
    assert record["entity_id"] == "party_1"
    assert record["proposal_strategy"] == "exact"


def test_reconciliation_proposes_counsel_link_on_exact_process_local_match(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    _write_jsonl(
        curated_dir / "entity_identifier.jsonl",
        [
            {
                "identifier_occurrence_id": "eid_2",
                "process_id": "proc_1",
                "process_number": "AC 1",
                "identifier_kind": "cpf",
                "identifier_value_raw": "529.982.247-25",
                "identifier_value_normalized": "52998224725",
                "context_snippet": "ADVOGADO JOAO DA SILVA CPF: 529.982.247-25",
                "entity_name_hint": "JOAO DA SILVA",
                "source_doc_type": "decisoes",
                "source_file": "2026-03.jsonl",
                "source_field": "decisao_texto",
                "source_url": None,
                "juris_doc_id": "doc-2",
                "extraction_confidence": 0.95,
                "extraction_method": "regex_labeled_tax_id",
                "uncertainty_note": None,
                "created_at": "2026-03-09T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(curated_dir / "party.jsonl", [])
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {
                "counsel_id": "csl_1",
                "counsel_name_raw": "Joao da Silva",
                "counsel_name_normalized": "JOAO DA SILVA",
                "canonical_name_normalized": "JOAO DA SILVA",
            }
        ],
    )
    _write_jsonl(curated_dir / "process_party_link.jsonl", [])
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {
                "link_id": "pcl_1",
                "process_id": "proc_1",
                "counsel_id": "csl_1",
                "side_in_case": "REQTE.(S)",
                "source_id": "juris",
            }
        ],
    )

    records = build_entity_identifier_reconciliation_records(
        entity_identifier_path=curated_dir / "entity_identifier.jsonl",
        party_path=curated_dir / "party.jsonl",
        counsel_path=curated_dir / "counsel.jsonl",
        process_party_link_path=curated_dir / "process_party_link.jsonl",
        process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
    )

    assert len(records) == 1
    record = records[0]
    assert record["proposal_status"] == "proposed"
    assert record["entity_kind"] == "counsel"
    assert record["entity_id"] == "csl_1"


def test_reconciliation_marks_ambiguous_candidates(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    _write_jsonl(
        curated_dir / "entity_identifier.jsonl",
        [
            {
                "identifier_occurrence_id": "eid_3",
                "process_id": "proc_1",
                "process_number": "AC 1",
                "identifier_kind": "cpf",
                "identifier_value_raw": "529.982.247-25",
                "identifier_value_normalized": "52998224725",
                "context_snippet": "JOAO SILVA CPF: 529.982.247-25",
                "entity_name_hint": "JOAO SILVA",
                "source_doc_type": "decisoes",
                "source_file": "2026-03.jsonl",
                "source_field": "decisao_texto",
                "source_url": None,
                "juris_doc_id": "doc-3",
                "extraction_confidence": 0.95,
                "extraction_method": "regex_labeled_tax_id",
                "uncertainty_note": None,
                "created_at": "2026-03-09T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [
            {"party_id": "party_1", "party_name_raw": "Joao Silva", "party_name_normalized": "JOAO SILVA"},
        ],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {"counsel_id": "csl_1", "counsel_name_raw": "Joao Silva", "counsel_name_normalized": "JOAO SILVA"},
        ],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {
                "link_id": "ppl_1",
                "process_id": "proc_1",
                "party_id": "party_1",
                "role_in_case": None,
                "source_id": "juris",
            },
        ],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {
                "link_id": "pcl_1",
                "process_id": "proc_1",
                "counsel_id": "csl_1",
                "side_in_case": None,
                "source_id": "juris",
            },
        ],
    )

    records = build_entity_identifier_reconciliation_records(
        entity_identifier_path=curated_dir / "entity_identifier.jsonl",
        party_path=curated_dir / "party.jsonl",
        counsel_path=curated_dir / "counsel.jsonl",
        process_party_link_path=curated_dir / "process_party_link.jsonl",
        process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
    )

    assert len(records) == 1
    record = records[0]
    assert record["proposal_status"] == "ambiguous"
    assert record["entity_id"] is None
    assert record["candidate_count"] == 2


def test_reconciliation_jsonl_writes_file(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    _write_jsonl(
        curated_dir / "entity_identifier.jsonl",
        [
            {
                "identifier_occurrence_id": "eid_4",
                "process_id": "proc_1",
                "process_number": "AC 1",
                "identifier_kind": "cpf",
                "identifier_value_raw": "529.982.247-25",
                "identifier_value_normalized": "52998224725",
                "context_snippet": "JOAO DA SILVA CPF: 529.982.247-25",
                "entity_name_hint": "JOAO DA SILVA",
                "source_doc_type": "decisoes",
                "source_file": "2026-03.jsonl",
                "source_field": "decisao_texto",
                "source_url": None,
                "juris_doc_id": "doc-4",
                "extraction_confidence": 0.95,
                "extraction_method": "regex_labeled_tax_id",
                "uncertainty_note": None,
                "created_at": "2026-03-09T00:00:00+00:00",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [{"party_id": "party_1", "party_name_raw": "Joao da Silva", "party_name_normalized": "JOAO DA SILVA"}],
    )
    _write_jsonl(curated_dir / "counsel.jsonl", [])
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {
                "link_id": "ppl_1",
                "process_id": "proc_1",
                "party_id": "party_1",
                "role_in_case": None,
                "source_id": "juris",
            }
        ],
    )
    _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

    output = curated_dir / "entity_identifier_reconciliation.jsonl"
    build_entity_identifier_reconciliation_jsonl(
        entity_identifier_path=curated_dir / "entity_identifier.jsonl",
        party_path=curated_dir / "party.jsonl",
        counsel_path=curated_dir / "counsel.jsonl",
        process_party_link_path=curated_dir / "process_party_link.jsonl",
        process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
        output_path=output,
    )

    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["reconciliation_id"].startswith("eir_")
