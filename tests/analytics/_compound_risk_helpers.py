"""Shared helpers and setup for compound risk tests."""

from __future__ import annotations

import json
from pathlib import Path


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _build_setup(tmp_path: Path) -> dict[str, Path]:
    """Build standard compound risk test fixtures in tmp_path."""
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "party.jsonl",
        [
            {"party_id": "p1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A"},
            {"party_id": "p2", "party_name_raw": "REU B", "party_name_normalized": "REU B"},
        ],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {"counsel_id": "c1", "counsel_name_raw": "ADV SILVA", "counsel_name_normalized": "ADV SILVA"},
        ],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {"link_id": "pp1", "process_id": "proc_1", "party_id": "p1", "role_in_case": "REQTE.(S)"},
            {"link_id": "pp2", "process_id": "proc_2", "party_id": "p1", "role_in_case": "REQTE.(S)"},
            {"link_id": "pp3", "process_id": "proc_3", "party_id": "p2", "role_in_case": "REQDO.(A/S)"},
        ],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {"link_id": "pc1", "process_id": "proc_1", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
            {"link_id": "pc2", "process_id": "proc_2", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
        ],
    )
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {
                "decision_event_id": "evt_1",
                "process_id": "proc_1",
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Procedente",
                "decision_date": "2021-03-15",
            },
            {
                "decision_event_id": "evt_2",
                "process_id": "proc_2",
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Procedente",
                "decision_date": "2023-07-10",
            },
            {
                "decision_event_id": "evt_3",
                "process_id": "proc_3",
                "current_rapporteur": "MIN. OUTRO",
                "decision_progress": "Improcedente",
                "decision_date": "2022-01-05",
            },
        ],
    )

    _write_jsonl(
        analytics_dir / "sanction_match.jsonl",
        [
            {
                "match_id": "sm1",
                "entity_type": "party",
                "entity_id": "p1",
                "entity_name_normalized": "AUTOR A",
                "party_id": "p1",
                "party_name_normalized": "AUTOR A",
                "sanction_source": "CGU",
                "sanction_id": "s1",
                "favorable_rate_delta": 0.33,
                "red_flag": True,
            }
        ],
    )
    _write_json(analytics_dir / "sanction_match_summary.json", {"red_flag_count": 1})

    _write_jsonl(
        analytics_dir / "donation_match.jsonl",
        [
            {
                "match_id": "dm1",
                "entity_type": "party",
                "entity_id": "p1",
                "entity_name_normalized": "AUTOR A",
                "party_id": "p1",
                "party_name_normalized": "AUTOR A",
                "donor_cpf_cnpj": "123",
                "total_donated_brl": 100000.0,
                "favorable_rate_delta": 0.24,
                "red_flag": True,
            }
        ],
    )
    _write_json(analytics_dir / "donation_match_summary.json", {"red_flag_count": 1})

    _write_jsonl(
        analytics_dir / "corporate_network.jsonl",
        [
            {
                "conflict_id": "cn1",
                "minister_name": "MIN. TESTE",
                "company_cnpj_basico": "12345678",
                "company_name": "EMPRESA X",
                "linked_entity_type": "party",
                "linked_entity_id": "p1",
                "linked_entity_name": "AUTOR A",
                "shared_process_ids": ["proc_1", "proc_2"],
                "shared_process_count": 2,
                "favorable_rate_delta": 0.28,
                "red_flag": True,
                "link_degree": 1,
                "link_chain": "MIN. TESTE -> EMPRESA X -> AUTOR A",
            }
        ],
    )
    _write_json(analytics_dir / "corporate_network_summary.json", {"red_flag_count": 1})

    _write_jsonl(
        analytics_dir / "counsel_affinity.jsonl",
        [
            {
                "affinity_id": "ca1",
                "rapporteur": "MIN. TESTE",
                "counsel_id": "c1",
                "counsel_name_normalized": "ADV SILVA",
                "shared_case_count": 2,
                "pair_delta_vs_minister": 0.21,
                "pair_delta_vs_counsel": 0.19,
                "top_process_classes": ["ADI"],
                "red_flag": True,
            }
        ],
    )
    _write_json(analytics_dir / "counsel_affinity_summary.json", {"red_flag_count": 1})

    _write_jsonl(
        analytics_dir / "outlier_alert.jsonl",
        [
            {
                "alert_id": "alert-1",
                "process_id": "proc_1",
                "decision_event_id": "evt_1",
                "alert_type": "atipico",
                "alert_score": 0.92,
                "status": "novo",
            }
        ],
    )
    _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.92})

    return {
        "curated_dir": curated_dir,
        "analytics_dir": analytics_dir,
        "output_dir": analytics_dir,
    }
