"""Shared helpers and setup functions for donation_match tests."""

from __future__ import annotations

import json
from pathlib import Path


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _setup_test_data(tmp_path: Path) -> dict[str, Path]:
    tse_dir = tmp_path / "tse"
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

    _write_jsonl(
        tse_dir / "donations_raw.jsonl",
        [
            {
                "election_year": 2022,
                "state": "SP",
                "position": "SENADOR",
                "candidate_name": "FULANO",
                "candidate_cpf": "12345678901",
                "candidate_number": "123",
                "party_abbrev": "PT",
                "party_name": "PARTIDO DOS TRABALHADORES",
                "donor_name": "ACME CORP",
                "donor_name_rfb": "ACME CORP",
                "donor_cpf_cnpj": "12345678000199",
                "donor_name_normalized": "ACME CORP",
                "donation_amount": 50000.0,
                "donation_description": "Doacao em dinheiro",
                "donor_cnae_code": "4110700",
                "donor_cnae_description": "Incorporacao",
                "donor_state": "SP",
            },
        ],
    )

    _write_jsonl(
        curated_dir / "party.jsonl",
        [
            {"party_id": "p1", "party_name_raw": "ACME Corp", "party_name_normalized": "ACME CORP"},
            {"party_id": "p2", "party_name_raw": "Clean Co", "party_name_normalized": "CLEAN CO"},
        ],
    )

    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {"counsel_id": "c1", "counsel_name_raw": "Adv Silva", "counsel_name_normalized": "ADV SILVA"},
        ],
    )

    _write_jsonl(
        curated_dir / "process.jsonl",
        [
            {"process_id": "proc1", "process_class": "RE"},
            {"process_id": "proc2", "process_class": "RE"},
            {"process_id": "proc3", "process_class": "RE"},
            {"process_id": "proc4", "process_class": "RE"},
        ],
    )

    # ACME (p1) has 3 processes, CLEAN (p2) has 2 — ensures min_cases >= 3 for red_flag
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {"link_id": "ppl1", "process_id": "proc1", "party_id": "p1"},
            {"link_id": "ppl3", "process_id": "proc3", "party_id": "p1"},
            {"link_id": "ppl4", "process_id": "proc4", "party_id": "p1"},
            {"link_id": "ppl2", "process_id": "proc2", "party_id": "p2"},
        ],
    )

    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {"link_id": "pcl1", "process_id": "proc1", "counsel_id": "c1"},
            {"link_id": "pcl2", "process_id": "proc2", "counsel_id": "c1"},
            {"link_id": "pcl3", "process_id": "proc3", "counsel_id": "c1"},
            {"link_id": "pcl4", "process_id": "proc4", "counsel_id": "c1"},
        ],
    )

    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {"decision_event_id": "de1", "process_id": "proc1", "decision_progress": "Provido"},
            {"decision_event_id": "de2", "process_id": "proc1", "decision_progress": "Provido"},
            {"decision_event_id": "de3", "process_id": "proc2", "decision_progress": "Desprovido"},
            {"decision_event_id": "de4", "process_id": "proc2", "decision_progress": "Desprovido"},
            {"decision_event_id": "de5", "process_id": "proc3", "decision_progress": "Provido"},
            {"decision_event_id": "de6", "process_id": "proc4", "decision_progress": "Provido"},
        ],
    )

    return {
        "tse_dir": tse_dir,
        "party_path": curated_dir / "party.jsonl",
        "counsel_path": curated_dir / "counsel.jsonl",
        "process_path": curated_dir / "process.jsonl",
        "decision_event_path": curated_dir / "decision_event.jsonl",
        "process_party_link_path": curated_dir / "process_party_link.jsonl",
        "process_counsel_link_path": curated_dir / "process_counsel_link.jsonl",
        "output_dir": analytics_dir,
    }
