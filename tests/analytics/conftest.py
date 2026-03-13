from __future__ import annotations

import json
from pathlib import Path


def make_decision_event(**overrides) -> dict:
    defaults = {
        "decision_event_id": "de_0",
        "process_id": "proc_0",
        "decision_date": "2026-01-10",
        "current_rapporteur": "MIN. DIAS TOFFOLI",
        "decision_type": "Despacho",
        "decision_progress": "DESPACHO",
        "judging_body": "MONOCRÁTICA",
        "is_collegiate": False,
    }
    defaults.update(overrides)
    return defaults


def make_process(**overrides) -> dict:
    defaults = {
        "process_id": "proc_0",
        "process_class": "AC",
        "subjects_normalized": ["TEMA A"],
        "branch_of_law": None,
    }
    defaults.update(overrides)
    return defaults


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def corporate_network_setup(tmp_path: Path) -> dict[str, Path]:
    """Create standard test data for corporate network tests."""
    rfb_dir = tmp_path / "rfb"
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

    # Minister bio
    bio_path = curated_dir / "minister_bio.json"
    write_json(bio_path, {"m1": {"minister_name": "MIN. TESTE"}})

    # RFB partners: minister + co-partner who is a party
    write_jsonl(
        rfb_dir / "partners_raw.jsonl",
        [
            {
                "cnpj_basico": "11111111",
                "partner_name": "MIN. TESTE",
                "partner_name_normalized": "MIN. TESTE",
                "qualification_code": "49",
            },
            {
                "cnpj_basico": "11111111",
                "partner_name": "AUTOR A",
                "partner_name_normalized": "AUTOR A",
                "qualification_code": "22",
            },
        ],
    )
    write_jsonl(
        rfb_dir / "companies_raw.jsonl",
        [
            {"cnpj_basico": "11111111", "razao_social": "EMPRESA XYZ LTDA"},
        ],
    )

    # Curated parties
    write_jsonl(
        curated_dir / "party.jsonl",
        [
            {"party_id": "p1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A"},
        ],
    )
    write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {"counsel_id": "c1", "counsel_name_raw": "ADV B", "counsel_name_normalized": "ADV B"},
        ],
    )

    # Process + decision event (minister is rapporteur, party is involved)
    write_jsonl(
        curated_dir / "process.jsonl",
        [
            {"process_id": "proc_1", "process_class": "ADI"},
            {"process_id": "proc_2", "process_class": "ADI"},
            {"process_id": "proc_3", "process_class": "ADI"},
        ],
    )
    write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {
                "decision_event_id": "e1",
                "process_id": "proc_1",
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Procedente",
            },
            {
                "decision_event_id": "e2",
                "process_id": "proc_2",
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Procedente",
            },
            {
                "decision_event_id": "e3",
                "process_id": "proc_3",
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Improcedente",
            },
        ],
    )
    write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {"link_id": "pp1", "process_id": "proc_1", "party_id": "p1", "role_in_case": "REQTE.(S)"},
            {"link_id": "pp2", "process_id": "proc_2", "party_id": "p1", "role_in_case": "REQTE.(S)"},
            {"link_id": "pp3", "process_id": "proc_3", "party_id": "p1", "role_in_case": "REQTE.(S)"},
        ],
    )
    write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {"link_id": "pc1", "process_id": "proc_1", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
        ],
    )

    return {
        "rfb_dir": rfb_dir,
        "minister_bio_path": bio_path,
        "party_path": curated_dir / "party.jsonl",
        "counsel_path": curated_dir / "counsel.jsonl",
        "process_path": curated_dir / "process.jsonl",
        "decision_event_path": curated_dir / "decision_event.jsonl",
        "process_party_link_path": curated_dir / "process_party_link.jsonl",
        "process_counsel_link_path": curated_dir / "process_counsel_link.jsonl",
        "output_dir": analytics_dir,
    }
