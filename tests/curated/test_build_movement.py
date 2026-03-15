from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.build_movement import (
    build_movement_jsonl,
    build_movement_records,
)


def _write_portal_json(portal_dir: Path, filename: str, doc: dict) -> None:
    portal_dir.mkdir(parents=True, exist_ok=True)
    (portal_dir / filename).write_text(
        json.dumps(doc, ensure_ascii=False),
        encoding="utf-8",
    )


def _sample_portal_doc(
    process_number: str = "ADI 1234",
    andamentos: list | None = None,
    deslocamentos: list | None = None,
    informacoes: dict | None = None,
) -> dict:
    return {
        "process_number": process_number,
        "source_system": "stf_portal",
        "source_url": "https://portal.stf.jus.br",
        "fetched_at": "2026-03-15T00:00:00+00:00",
        "raw_html_hash": "sha256:abc123",
        "andamentos": andamentos or [],
        "deslocamentos": deslocamentos or [],
        "peticoes": [],
        "sessao_virtual": [],
        "informacoes": informacoes or {"classe": "ADI", "relator_atual": "Min. X"},
    }


def _andamento(date: str, description: str, detail: str | None = None) -> dict:
    return {
        "date": date,
        "description": description,
        "detail": detail,
        "tab_name": "Andamentos",
    }


def test_build_movements_from_andamentos(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    doc = _sample_portal_doc(
        andamentos=[
            _andamento("2020-03-15", "Distribuído por sorteio", "Classe ADI"),
            _andamento("2020-04-10", "Julgamento finalizado"),
        ],
    )
    _write_portal_json(portal_dir, "ADI_1234.json", doc)

    records = build_movement_records(portal_dir=portal_dir)

    assert len(records) == 2
    dist = next(r for r in records if "sorteio" in (r["movement_raw_description"] or "").lower())
    assert dist["movement_category"] == "distribuicao"
    assert dist["process_id"].startswith("proc_")
    assert dist["source_system"] == "stf_portal"
    assert dist["movement_date"] == "2020-03-15"
    assert dist["movement_detail"] == "Classe ADI"
    assert dist["rapporteur_at_event"] == "Min. X"

    julg = next(r for r in records if "julgamento" in (r["movement_raw_description"] or "").lower())
    assert julg["movement_category"] == "decisao"


def test_build_movements_from_deslocamentos(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    doc = _sample_portal_doc(
        deslocamentos=[
            {
                "date": "2020-05-01",
                "origin": "Gabinete Min. X",
                "destination": "Gabinete Min. Y",
                "reason": "Redistribuição",
                "tab_name": "Deslocamentos",
            }
        ],
    )
    _write_portal_json(portal_dir, "ADI_1234.json", doc)

    records = build_movement_records(portal_dir=portal_dir)

    assert len(records) == 1
    record = records[0]
    assert record["movement_category"] == "deslocamento"
    assert "Gabinete Min. X" in (record["movement_raw_description"] or "")
    assert "Gabinete Min. Y" in (record["movement_raw_description"] or "")
    assert record["movement_detail"] == "Redistribuição"


def test_movement_categorization_sets_confidence(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    doc = _sample_portal_doc(
        andamentos=[
            _andamento("2020-01-01", "Pedido de vista dos autos"),
            _andamento("2020-02-01", "Evento sem classificação xyz"),
        ],
    )
    _write_portal_json(portal_dir, "ADI_1234.json", doc)

    records = build_movement_records(portal_dir=portal_dir)

    vista = next(r for r in records if r["movement_category"] == "vista")
    assert vista["tpu_match_confidence"] == "fuzzy"
    assert vista["normalization_method"] == "regex_rule"

    outros = next(r for r in records if r["movement_category"] == "outros")
    assert outros["tpu_match_confidence"] is None
    assert outros["normalization_method"] is None


def test_empty_portal_directory(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    portal_dir.mkdir()
    records = build_movement_records(portal_dir=portal_dir)
    assert records == []


def test_nonexistent_portal_directory(tmp_path: Path):
    portal_dir = tmp_path / "does_not_exist"
    records = build_movement_records(portal_dir=portal_dir)
    assert records == []


def test_tpu_code_and_name_are_none(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    doc = _sample_portal_doc(
        andamentos=[_andamento("2020-01-01", "Distribuído")],
    )
    _write_portal_json(portal_dir, "ADI_1234.json", doc)

    records = build_movement_records(portal_dir=portal_dir)

    assert len(records) == 1
    assert records[0]["tpu_code"] is None
    assert records[0]["tpu_name"] is None


def test_build_movement_jsonl_writes_file(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    doc = _sample_portal_doc(
        andamentos=[_andamento("2020-01-01", "Distribuído por sorteio")],
    )
    _write_portal_json(portal_dir, "ADI_1234.json", doc)

    output = tmp_path / "movement.jsonl"
    result = build_movement_jsonl(portal_dir=portal_dir, output_path=output)

    assert result == output
    assert output.exists()
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["movement_id"].startswith("mov_")
    assert payload["source_system"] == "stf_portal"


def test_multiple_portal_files(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    doc1 = _sample_portal_doc(
        process_number="ADI 1111",
        andamentos=[_andamento("2020-01-01", "Distribuído")],
    )
    doc2 = _sample_portal_doc(
        process_number="RE 2222",
        andamentos=[_andamento("2020-06-01", "Julgado procedente")],
    )
    _write_portal_json(portal_dir, "ADI_1111.json", doc1)
    _write_portal_json(portal_dir, "RE_2222.json", doc2)

    records = build_movement_records(portal_dir=portal_dir)

    assert len(records) == 2
    process_numbers = {r["process_id"] for r in records}
    assert len(process_numbers) == 2


def test_portal_json_without_process_number_skipped(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    doc = {
        "process_number": "",
        "andamentos": [_andamento("2020-01-01", "X")],
    }
    _write_portal_json(portal_dir, "empty.json", doc)

    records = build_movement_records(portal_dir=portal_dir)
    assert records == []


def test_movement_ids_are_deterministic(tmp_path: Path):
    portal_dir = tmp_path / "stf_portal"
    doc = _sample_portal_doc(
        andamentos=[_andamento("2020-01-01", "Distribuído")],
    )
    _write_portal_json(portal_dir, "ADI_1234.json", doc)

    records1 = build_movement_records(portal_dir=portal_dir)
    records2 = build_movement_records(portal_dir=portal_dir)

    assert records1[0]["movement_id"] == records2[0]["movement_id"]
