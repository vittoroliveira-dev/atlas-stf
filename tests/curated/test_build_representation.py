"""Tests for the representation-network curated builder."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from atlas_stf.core.identity import (
    build_lawyer_identity_key,
    normalize_entity_name,
    stable_id,
)
from atlas_stf.curated._build_representation_edges import (
    build_representation_edge_records,
    build_representation_event_records,
    build_source_evidence_records,
)
from atlas_stf.curated._build_representation_firms import build_law_firm_entity_records
from atlas_stf.curated._build_representation_lawyers import build_lawyer_entity_records
from atlas_stf.curated.build_representation import build_representation_jsonl


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
    _write_process_jsonl(process_path, [
        {
            "process_id": "proc_1",
            "juris_partes": "REQTE.(S): ESTADO X ADV.(A/S): JOAO DA SILVA",
        },
    ])

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
    _write_process_jsonl(process_path, [
        {
            "process_id": "proc_1",
            "juris_advogados": "MARIA OLIVEIRA; PEDRO SANTOS",
        },
    ])

    records = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    names = {r["lawyer_name_normalized"] for r in records}
    assert "MARIA OLIVEIRA" in names
    assert "PEDRO SANTOS" in names


def test_build_lawyer_entity_records_with_portal_oab(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/processos/detalhe.asp?incidente=12345",
        "representantes": [
            {
                "lawyer_name": "Ana Costa",
                "oab_number": "12345/SP",
                "oab_state": "SP",
            },
        ],
    })

    records = build_lawyer_entity_records(process_path, portal_dir, tmp_path)

    oab_records = [r for r in records if r.get("oab_number")]
    assert len(oab_records) >= 1
    assert oab_records[0]["oab_number"] == "12345/SP"
    assert "portal_stf" in oab_records[0]["source_systems"]


def test_build_lawyer_entity_dedup_by_identity_key(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [
        {
            "process_id": "proc_1",
            "juris_advogados": "JOAO DA SILVA",
        },
        {
            "process_id": "proc_2",
            "juris_advogados": "JOAO DA SILVA",
        },
    ])

    records = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    names = [r["lawyer_name_normalized"] for r in records if r["lawyer_name_normalized"] == "JOAO DA SILVA"]
    assert len(names) == 1


def test_build_lawyer_entity_stable_id_deterministic(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [
        {"process_id": "proc_1", "juris_advogados": "JOAO DA SILVA"},
    ])

    records_1 = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)
    records_2 = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    assert records_1[0]["lawyer_id"] == records_2[0]["lawyer_id"]


def test_build_lawyer_entity_identity_strategy_name(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [
        {"process_id": "proc_1", "juris_advogados": "CARLOS MENDES"},
    ])

    records = build_lawyer_entity_records(process_path, tmp_path / "portal", tmp_path)

    assert len(records) == 1
    assert records[0]["identity_strategy"] == "name"


def test_build_lawyer_entity_identity_strategy_oab(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "representantes": [
            {"lawyer_name": "Carlos Mendes", "oab_number": "999/RJ", "oab_state": "RJ"},
        ],
    })

    records = build_lawyer_entity_records(process_path, portal_dir, tmp_path)

    oab_records = [r for r in records if r.get("oab_number")]
    assert len(oab_records) >= 1
    assert oab_records[0]["identity_strategy"] == "oab"


# ---------------------------------------------------------------------------
# Law firm entity tests
# ---------------------------------------------------------------------------


def test_build_law_firm_entity_records_with_portal_data(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "representantes": [
            {
                "lawyer_name": "Ana Costa",
                "firm_name": "Costa e Associados Advogados",
                "affiliation_confidence": "low",
            },
        ],
    })

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
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "representantes": [
            {"lawyer_name": "A", "firm_name": "Escritorio ABC"},
            {"lawyer_name": "B", "firm_name": "Escritorio ABC"},
        ],
    })

    records = build_law_firm_entity_records(process_path, portal_dir, tmp_path)

    assert len(records) == 1


def test_build_law_firm_entity_stable_id_deterministic(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "representantes": [{"lawyer_name": "A", "firm_name": "Escritorio ABC"}],
    })

    r1 = build_law_firm_entity_records(process_path, portal_dir, tmp_path)
    r2 = build_law_firm_entity_records(process_path, portal_dir, tmp_path)

    assert r1[0]["firm_id"] == r2[0]["firm_id"]


# ---------------------------------------------------------------------------
# Representation edge tests
# ---------------------------------------------------------------------------


def test_build_representation_edge_records(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [
        {
            "process_id": "proc_1",
            "juris_advogados": "JOAO DA SILVA",
        },
    ])

    normalized = normalize_entity_name("JOAO DA SILVA")
    assert normalized is not None
    identity_key = build_lawyer_identity_key(name=normalized)
    assert identity_key is not None
    lawyer_id = stable_id("law_", identity_key)

    lawyer_map = {
        identity_key: {
            "lawyer_id": lawyer_id,
            "lawyer_name_normalized": normalized,
        },
    }

    records = build_representation_edge_records(
        process_path=process_path,
        portal_dir=tmp_path / "portal",
        curated_dir=tmp_path,
        lawyer_map=lawyer_map,
        firm_map={},
        party_map={},
    )

    assert len(records) >= 1
    assert records[0]["process_id"] == "proc_1"
    assert records[0]["representative_entity_id"] == lawyer_id
    assert records[0]["representative_kind"] == "lawyer"


def test_build_representation_edge_records_empty(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    records = build_representation_edge_records(
        process_path=process_path,
        portal_dir=tmp_path / "portal",
        curated_dir=tmp_path,
        lawyer_map={},
        firm_map={},
        party_map={},
    )

    assert records == []


# ---------------------------------------------------------------------------
# Representation event tests
# ---------------------------------------------------------------------------


def test_build_representation_event_records_with_oral_argument(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [
        {"process_id": "proc_1", "process_number": "ADI 1234"},
    ])

    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "oral_arguments": [
            {
                "lawyer_name": "Ana Costa",
                "party_represented": "Estado X",
                "session_date": "2026-03-15",
                "session_type": "Plenario",
            },
        ],
    })

    records = build_representation_event_records(
        process_path=process_path,
        portal_dir=portal_dir,
    )

    assert len(records) == 1
    assert records[0]["event_type"] == "oral_argument"
    assert records[0]["process_id"] == "proc_1"
    assert records[0]["event_date"] == "2026-03-15"


def test_build_representation_event_records_with_petition(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [
        {"process_id": "proc_1", "process_number": "ADI 1234"},
    ])

    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "peticoes_detailed": [
            {
                "petitioner_name": "Joao Silva",
                "date": "2026-02-10",
                "document_type": "Recurso",
                "protocol": "123456",
            },
        ],
    })

    records = build_representation_event_records(
        process_path=process_path,
        portal_dir=portal_dir,
    )

    assert len(records) == 1
    assert records[0]["event_type"] == "petition"
    assert records[0]["protocol_number"] == "123456"


def test_build_representation_event_records_empty(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    records = build_representation_event_records(
        process_path=process_path,
        portal_dir=tmp_path / "portal",
    )

    assert records == []


# ---------------------------------------------------------------------------
# Source evidence tests
# ---------------------------------------------------------------------------


def test_build_source_evidence_records_with_representantes(tmp_path: Path):
    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "fetched_at": "2026-03-15T00:00:00+00:00",
        "representantes": [
            {"lawyer_name": "Ana Costa", "party_name": "Estado X"},
        ],
    })

    records = build_source_evidence_records(portal_dir=portal_dir)

    assert len(records) >= 1
    assert records[0]["source_system"] == "portal_stf"
    assert records[0]["source_tab"] == "Partes"
    assert records[0]["process_number"] == "ADI 1234"


def test_build_source_evidence_records_empty(tmp_path: Path):
    records = build_source_evidence_records(portal_dir=tmp_path / "portal")

    assert records == []


def test_build_source_evidence_records_with_oral_arguments(tmp_path: Path):
    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 5678",
        "source_url": "https://portal.stf.jus.br/y",
        "fetched_at": "2026-03-15T00:00:00+00:00",
        "oral_arguments": [
            {"lawyer_name": "Carlos Mendes", "party_represented": "Uniao"},
        ],
    })

    records = build_source_evidence_records(portal_dir=portal_dir)

    assert len(records) >= 1
    oral_recs = [r for r in records if r["source_tab"] == "Sustentacao Oral"]
    assert len(oral_recs) == 1


# ---------------------------------------------------------------------------
# Orchestrator tests
# ---------------------------------------------------------------------------


def test_build_representation_jsonl_orchestrator(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [
        {
            "process_id": "proc_1",
            "process_number": "ADI 1234",
            "juris_advogados": "JOAO DA SILVA",
        },
    ])

    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "fetched_at": "2026-03-15T00:00:00+00:00",
        "representantes": [
            {
                "lawyer_name": "Ana Costa",
                "oab_number": "12345/SP",
                "oab_state": "SP",
                "party_name": "Estado X",
                "party_role": "REQTE",
                "firm_name": "Costa Advogados",
                "affiliation_confidence": "low",
            },
        ],
        "oral_arguments": [
            {
                "lawyer_name": "Ana Costa",
                "party_represented": "Estado X",
                "session_date": "2026-03-15",
                "session_type": "Plenario",
            },
        ],
    })

    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()

    results = build_representation_jsonl(
        process_path=process_path,
        portal_dir=portal_dir,
        curated_dir=curated_dir,
    )

    assert "lawyer_entity" in results
    assert "law_firm_entity" in results
    assert "representation_edge" in results
    assert "representation_event" in results
    assert "source_evidence" in results

    # Files should exist
    for artifact_path in results.values():
        assert artifact_path.exists()


def test_build_representation_jsonl_with_progress(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [{"process_id": "proc_1"}])

    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()

    progress_calls: list[tuple[int, int, str]] = []

    def on_progress(current: int, total: int, desc: str) -> None:
        progress_calls.append((current, total, desc))

    build_representation_jsonl(
        process_path=process_path,
        portal_dir=tmp_path / "portal",
        curated_dir=curated_dir,
        on_progress=on_progress,
    )

    assert len(progress_calls) == 6  # 5 steps + final
    assert progress_calls[0][0] == 0
    assert progress_calls[-1][0] == progress_calls[-1][1]


def test_lawyer_rekey_no_duplicate(tmp_path: Path):
    """Lawyer enters by name, then OAB upgrades identity → no duplicate."""
    process_path = tmp_path / "process.jsonl"
    # Source 1: name-only entry for "ANA COSTA"
    _write_process_jsonl(process_path, [
        {"process_id": "proc_1", "juris_advogados": "ANA COSTA"},
    ])

    # Source 2: portal provides OAB for the same person
    portal_dir = tmp_path / "portal"
    _write_portal_json(portal_dir, {
        "process_number": "ADI 1234",
        "source_url": "https://portal.stf.jus.br/x",
        "representantes": [
            {
                "lawyer_name": "Ana Costa",
                "oab_number": "12345/SP",
                "oab_state": "SP",
            },
        ],
    })

    records = build_lawyer_entity_records(process_path, portal_dir, tmp_path)

    # Must have exactly 1 record (no duplicate from stale name key)
    normalized_names = [r["lawyer_name_normalized"] for r in records if r["lawyer_name_normalized"] == "ANA COSTA"]
    assert len(normalized_names) == 1, f"Expected 1 ANA COSTA record, got {len(normalized_names)}"

    # The surviving record must use OAB identity
    ana = [r for r in records if r["lawyer_name_normalized"] == "ANA COSTA"][0]
    assert ana["identity_strategy"] == "oab"
    assert ana["oab_number"] == "12345/SP"


def test_build_representation_jsonl_no_portal(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    _write_process_jsonl(process_path, [
        {"process_id": "proc_1", "juris_advogados": "JOAO DA SILVA"},
    ])

    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()

    results = build_representation_jsonl(
        process_path=process_path,
        portal_dir=tmp_path / "portal",
        curated_dir=curated_dir,
    )

    # Lawyers from process data should still be built
    lawyer_path = results["lawyer_entity"]
    assert lawyer_path.exists()
    with lawyer_path.open(encoding="utf-8") as fh:
        lawyer_records = [json.loads(line) for line in fh if line.strip()]
    assert len(lawyer_records) >= 1

    # Firms should be empty
    firm_path = results["law_firm_entity"]
    assert firm_path.exists()
    with firm_path.open(encoding="utf-8") as fh:
        firm_records = [json.loads(line) for line in fh if line.strip()]
    assert firm_records == []
