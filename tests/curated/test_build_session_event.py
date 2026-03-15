from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.build_session_event import (
    build_session_event_jsonl,
    build_session_event_records,
)


def _write_movements(path: Path, movements: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for m in movements:
            fh.write(json.dumps(m, ensure_ascii=False) + "\n")


def _write_portal_json(portal_dir: Path, filename: str, doc: dict) -> None:
    portal_dir.mkdir(parents=True, exist_ok=True)
    (portal_dir / filename).write_text(
        json.dumps(doc, ensure_ascii=False),
        encoding="utf-8",
    )


def _make_movement(
    *,
    process_id: str = "proc_abc123",
    movement_id: str = "mov_001",
    category: str = "pauta",
    description: str = "Incluído em pauta do Plenário",
    date: str = "2020-05-10",
    rapporteur: str | None = "Min. X",
) -> dict:
    return {
        "movement_id": movement_id,
        "process_id": process_id,
        "source_system": "stf_portal",
        "tpu_code": None,
        "tpu_name": None,
        "movement_category": category,
        "movement_raw_description": description,
        "movement_date": date,
        "movement_detail": None,
        "rapporteur_at_event": rapporteur,
        "tpu_match_confidence": "fuzzy",
        "normalization_method": "regex_rule",
        "created_at": "2026-03-15T00:00:00+00:00",
    }


def test_build_session_events_from_movements(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="pauta",
            description="Incluído em pauta do Plenário",
            date="2020-05-10",
        ),
        _make_movement(
            category="decisao",
            description="Julgamento finalizado",
            date="2020-06-01",
            movement_id="mov_002",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert len(records) == 2


def test_event_type_classification_vista(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="vista",
            description="Pedido de vista dos autos pelo Min. Y",
            date="2020-05-10",
            movement_id="mov_vista",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert len(records) == 1
    assert records[0]["event_type"] == "pedido_de_vista"


def test_event_type_classification_devolvido_vista(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="vista",
            description="Devolvidos autos após pedido de vista",
            date="2020-06-10",
            movement_id="mov_dev",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert len(records) == 1
    assert records[0]["event_type"] == "devolvido_vista"


def test_event_type_pauta_withdrawal(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="pauta",
            description="Retirado de pauta",
            date="2020-05-15",
            movement_id="mov_ret",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert len(records) == 1
    assert records[0]["event_type"] == "pauta_withdrawal"


def test_event_type_fallback_by_category(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="decisao",
            description="Provido o recurso",
            date="2020-07-01",
            movement_id="mov_dec",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert len(records) == 1
    assert records[0]["event_type"] == "julgamento"


def test_session_type_detection_plenario(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="pauta",
            description="Incluído em pauta do Plenário",
            date="2020-05-10",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert records[0]["session_type"] == "plenario"


def test_session_type_detection_turma_1(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="pauta",
            description="Incluído em pauta da 1ª Turma",
            date="2020-05-10",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert records[0]["session_type"] == "turma_1"


def test_session_type_detection_turma_2(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="pauta",
            description="Incluído em pauta da 2ª Turma",
            date="2020-05-10",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert records[0]["session_type"] == "turma_2"


def test_session_type_detection_virtual(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="pauta",
            description="Inclusão em Pauta de Sessão Virtual",
            date="2020-05-10",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert records[0]["session_type"] == "plenario_virtual"


def test_session_type_none_when_unknown(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="decisao",
            description="Provido o recurso",
            date="2020-05-10",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert records[0]["session_type"] is None


def test_vista_duration_days_calculation(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="vista",
            description="Pedido de vista dos autos",
            date="2020-05-10",
            movement_id="mov_vista1",
        ),
        _make_movement(
            category="vista",
            description="Devolvidos autos após pedido de vista",
            date="2020-05-20",
            movement_id="mov_dev1",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    vista = next(r for r in records if r["event_type"] == "pedido_de_vista")
    assert vista["vista_duration_days"] == 10


def test_sessao_virtual_from_portal(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    movement_path.write_text("", encoding="utf-8")

    portal_dir = tmp_path / "stf_portal"
    doc = {
        "process_number": "ADI 5678",
        "source_system": "stf_portal",
        "source_url": "https://portal.stf.jus.br",
        "fetched_at": "2026-03-15T00:00:00+00:00",
        "raw_html_hash": "sha256:def456",
        "andamentos": [],
        "deslocamentos": [],
        "peticoes": [],
        "sessao_virtual": [
            {
                "start_date": "2020-06-01",
                "end_date": "2020-06-05",
                "result": "Procedente",
                "tab_name": "Sessão Virtual",
            }
        ],
        "informacoes": {"classe": "ADI", "relator_atual": "Min. Z"},
    }
    _write_portal_json(portal_dir, "ADI_5678.json", doc)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert len(records) == 1
    record = records[0]
    assert record["session_event_id"].startswith("se_")
    assert record["event_type"] == "julgamento"
    assert record["session_type"] == "plenario_virtual"
    assert record["event_date"] == "2020-06-01"
    assert record["rapporteur_at_event"] == "Min. Z"
    assert record["movement_id"] is None


def test_empty_input_produces_empty_list(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    movement_path.write_text("", encoding="utf-8")
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)
    assert records == []


def test_nonexistent_movement_file(tmp_path: Path):
    movement_path = tmp_path / "does_not_exist.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)
    assert records == []


def test_non_session_categories_filtered_out(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(category="distribuicao", description="Distribuído por sorteio", date="2020-01-01"),
        _make_movement(category="publicacao", description="Publicado no DJe", date="2020-01-02", movement_id="mov_pub"),
        _make_movement(category="baixa", description="Arquivado", date="2020-01-03", movement_id="mov_baixa"),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)
    assert records == []


def test_build_session_event_jsonl_writes_file(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(category="decisao", description="Julgamento", date="2020-06-01"),
    ]
    _write_movements(movement_path, movements)

    output = tmp_path / "session_event.jsonl"
    result = build_session_event_jsonl(
        movement_path=movement_path,
        portal_dir=portal_dir,
        output_path=output,
    )

    assert result == output
    assert output.exists()
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["session_event_id"].startswith("se_")


def test_movement_id_linked(tmp_path: Path):
    movement_path = tmp_path / "movement.jsonl"
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()

    movements = [
        _make_movement(
            category="pauta",
            description="Incluído em pauta",
            date="2020-05-10",
            movement_id="mov_link_test",
        ),
    ]
    _write_movements(movement_path, movements)

    records = build_session_event_records(movement_path=movement_path, portal_dir=portal_dir)

    assert len(records) == 1
    assert records[0]["movement_id"] == "mov_link_test"
