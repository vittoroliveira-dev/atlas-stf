from __future__ import annotations

from pathlib import Path

from atlas_stf.cli import main

from .conftest import _write_json, _write_jsonl


def test_cli_analytics_compound_risk(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"

    _write_jsonl(
        curated_dir / "party.jsonl",
        [{"party_id": "p1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A"}],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [{"counsel_id": "c1", "counsel_name_raw": "ADV", "counsel_name_normalized": "ADV"}],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [{"link_id": "pp1", "process_id": "proc_1", "party_id": "p1", "role_in_case": "REQTE.(S)"}],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [{"link_id": "pc1", "process_id": "proc_1", "counsel_id": "c1", "side_in_case": "REQTE.(S)"}],
    )
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {
                "decision_event_id": "evt_1",
                "process_id": "proc_1",
                "current_rapporteur": "MIN. TESTE",
                "decision_progress": "Procedente",
            }
        ],
    )
    _write_jsonl(
        analytics_dir / "sanction_match.jsonl",
        [
            {
                "match_id": "sm1",
                "party_id": "p1",
                "party_name_normalized": "AUTOR A",
                "sanction_source": "CGU",
                "red_flag": True,
            }
        ],
    )
    _write_jsonl(
        analytics_dir / "donation_match.jsonl",
        [
            {
                "match_id": "dm1",
                "party_id": "p1",
                "party_name_normalized": "AUTOR A",
                "total_donated_brl": 10.0,
                "red_flag": True,
            }
        ],
    )
    _write_jsonl(
        analytics_dir / "corporate_network.jsonl",
        [
            {
                "conflict_id": "cn1",
                "minister_name": "MIN. TESTE",
                "linked_entity_type": "party",
                "linked_entity_id": "p1",
                "linked_entity_name": "AUTOR A",
                "shared_process_ids": ["proc_1"],
                "red_flag": True,
            }
        ],
    )
    _write_jsonl(analytics_dir / "counsel_affinity.jsonl", [])
    _write_jsonl(
        analytics_dir / "outlier_alert.jsonl",
        [{"alert_id": "a1", "process_id": "proc_1", "decision_event_id": "evt_1", "alert_score": 0.9}],
    )
    _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.9})

    code = main(
        [
            "analytics",
            "compound-risk",
            "--curated-dir",
            str(curated_dir),
            "--analytics-dir",
            str(analytics_dir),
            "--output-dir",
            str(analytics_dir),
        ]
    )

    assert code == 0
    assert (analytics_dir / "compound_risk.jsonl").exists()
    assert (analytics_dir / "compound_risk_summary.json").exists()
