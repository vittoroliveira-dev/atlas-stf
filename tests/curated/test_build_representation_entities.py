"""Tests for representation-network entity builders (lawyer + law firm)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from atlas_stf.curated._build_representation_firms import build_law_firm_entity_records
from atlas_stf.curated._build_representation_lawyers import build_lawyer_entity_records


def _write_process_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _write_portal_json(portal_dir: Path, doc: dict[str, Any], filename: str = "ADI_1234.json") -> None:
    portal_dir.mkdir(parents=True, exist_ok=True)
    (portal_dir / filename).write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Lawyer entity tests
# ---------------------------------------------------------------------------


def test_build_lawyer_entity_records_with_juris_partes(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(
        process_path,
        [
            {
                "process_id": "proc_1",
                "juris_partes": "REQTE.(S): ESTADO X ADV.(A/S): JOAO DA SILVA",
            },
        ],
    )

    records = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    assert len(records) >= 1
    names = {r["lawyer_name_normalized"] for r in records}
    assert "JOAO DA SILVA" in names


def test_build_lawyer_entity_records_empty_data(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    records = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    assert records == []


def test_build_lawyer_entity_records_with_counsel_source_fields(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(
        process_path,
        [
            {
                "process_id": "proc_1",
                "juris_advogados": "MARIA OLIVEIRA; PEDRO SANTOS",
            },
        ],
    )

    records = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    names = {r["lawyer_name_normalized"] for r in records}
    assert "MARIA OLIVEIRA" in names
    assert "PEDRO SANTOS" in names


def test_build_lawyer_entity_records_with_portal_oab(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(
        portal_dir,
        {
            "process_number": "ADI 1234",
            "source_url": "https://portal.stf.jus.br/processos/detalhe.asp?incidente=12345",
            "representantes": [
                {
                    "lawyer_name": "Ana Costa",
                    "oab_number": "12345/SP",
                    "oab_state": "SP",
                },
            ],
        },
    )

    records = build_lawyer_entity_records(process_path, portal_dir, tmp_path)

    oab_records = [r for r in records if r.get("oab_number")]
    assert len(oab_records) >= 1
    assert oab_records[0]["oab_number"] == "12345/SP"
    assert "portal_stf" in oab_records[0]["source_systems"]


def test_build_lawyer_entity_dedup_by_identity_key(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(
        process_path,
        [
            {
                "process_id": "proc_1",
                "juris_advogados": "JOAO DA SILVA",
            },
            {
                "process_id": "proc_2",
                "juris_advogados": "JOAO DA SILVA",
            },
        ],
    )

    records = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    names = [r["lawyer_name_normalized"] for r in records if r["lawyer_name_normalized"] == "JOAO DA SILVA"]
    assert len(names) == 1


def test_build_lawyer_entity_stable_id_deterministic(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(
        process_path,
        [
            {"process_id": "proc_1", "juris_advogados": "JOAO DA SILVA"},
        ],
    )

    records_1 = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)
    records_2 = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    assert records_1[0]["lawyer_id"] == records_2[0]["lawyer_id"]


def test_build_lawyer_entity_identity_strategy_name(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(
        process_path,
        [
            {"process_id": "proc_1", "juris_advogados": "CARLOS MENDES"},
        ],
    )

    records = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    assert len(records) == 1
    assert records[0]["identity_strategy"] == "name"


def test_build_lawyer_entity_identity_strategy_oab(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(
        portal_dir,
        {
            "process_number": "ADI 1234",
            "source_url": "https://portal.stf.jus.br/x",
            "representantes": [
                {"lawyer_name": "Carlos Mendes", "oab_number": "999/RJ", "oab_state": "RJ"},
            ],
        },
    )

    records = build_lawyer_entity_records(process_path, portal_dir, tmp_path)

    oab_records = [r for r in records if r.get("oab_number")]
    assert len(oab_records) >= 1
    assert oab_records[0]["identity_strategy"] == "oab"


def test_lawyer_rekey_no_duplicate(tmp_path: Path):
    """Lawyer enters by name, then OAB upgrades identity -> no duplicate."""
    process_path = tmp_path / "process.jsonl"
    # Source 1: name-only entry for "ANA COSTA"
    _write_process_jsonl(
        process_path,
        [
            {"process_id": "proc_1", "juris_advogados": "ANA COSTA"},
        ],
    )

    # Source 2: portal provides OAB for the same person
    portal_dir = tmp_path / "portal"
    _write_portal_json(
        portal_dir,
        {
            "process_number": "ADI 1234",
            "source_url": "https://portal.stf.jus.br/x",
            "representantes": [
                {
                    "lawyer_name": "Ana Costa",
                    "oab_number": "12345/SP",
                    "oab_state": "SP",
                },
            ],
        },
    )

    records = build_lawyer_entity_records(process_path, portal_dir, tmp_path)

    # Must have exactly 1 record (no duplicate from stale name key)
    normalized_names = [r["lawyer_name_normalized"] for r in records if r["lawyer_name_normalized"] == "ANA COSTA"]
    assert len(normalized_names) == 1, f"Expected 1 ANA COSTA record, got {len(normalized_names)}"

    # The surviving record must use OAB identity
    ana = [r for r in records if r["lawyer_name_normalized"] == "ANA COSTA"][0]
    assert ana["identity_strategy"] == "oab"
    assert ana["oab_number"] == "12345/SP"


# ---------------------------------------------------------------------------
# Law firm entity tests
# ---------------------------------------------------------------------------


def test_build_law_firm_entity_records_with_portal_data(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(
        portal_dir,
        {
            "process_number": "ADI 1234",
            "source_url": "https://portal.stf.jus.br/x",
            "representantes": [
                {
                    "lawyer_name": "Ana Costa",
                    "firm_name": "Costa e Associados Advogados",
                    "affiliation_confidence": "low",
                },
            ],
        },
    )

    records = build_law_firm_entity_records(process_path, portal_dir, tmp_path)

    assert len(records) == 1
    assert records[0]["firm_name_raw"] == "Costa e Associados Advogados"
    assert "portal_stf" in records[0]["source_systems"]


def test_build_law_firm_entity_records_no_portal_data(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    records = build_law_firm_entity_records(process_path, tmp_path / "portal", tmp_path)

    assert records == []


def test_build_law_firm_entity_dedup(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(
        portal_dir,
        {
            "process_number": "ADI 1234",
            "source_url": "https://portal.stf.jus.br/x",
            "representantes": [
                {"lawyer_name": "A", "firm_name": "Escritorio ABC"},
                {"lawyer_name": "B", "firm_name": "Escritorio ABC"},
            ],
        },
    )

    records = build_law_firm_entity_records(process_path, portal_dir, tmp_path)

    assert len(records) == 1


def test_build_law_firm_entity_stable_id_deterministic(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(
        portal_dir,
        {
            "process_number": "ADI 1234",
            "source_url": "https://portal.stf.jus.br/x",
            "representantes": [{"lawyer_name": "A", "firm_name": "Escritorio ABC"}],
        },
    )

    r1 = build_law_firm_entity_records(process_path, portal_dir, tmp_path)
    r2 = build_law_firm_entity_records(process_path, portal_dir, tmp_path)

    assert r1[0]["firm_id"] == r2[0]["firm_id"]
