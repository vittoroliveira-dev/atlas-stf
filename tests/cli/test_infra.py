from __future__ import annotations

import os
from pathlib import Path

from atlas_stf.cli import main

from .conftest import _write_json, _write_jsonl


def test_cli_serving_build(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    db_path = tmp_path / "serving.db"

    _write_jsonl(
        curated_dir / "process.jsonl",
        [
            {
                "process_id": "proc_1",
                "process_number": "ADI 1",
                "process_class": "ADI",
                "branch_of_law": "DIREITO ADMINISTRATIVO",
                "subjects_normalized": ["DIREITO ADMINISTRATIVO"],
                "origin_description": "DISTRITO FEDERAL",
                "juris_inteiro_teor_url": "https://example.com/adi1.pdf",
                "juris_doc_count": 2,
                "juris_has_acordao": True,
                "juris_has_decisao_monocratica": False,
            }
        ],
    )
    _write_jsonl(
        curated_dir / "decision_event.jsonl",
        [
            {
                "decision_event_id": "evt_1",
                "process_id": "proc_1",
                "decision_date": "2026-01-05",
                "current_rapporteur": "MIN. TESTE",
                "decision_type": "Decisão Final",
                "decision_progress": "Procedente",
                "decision_origin": "JULGAMENTO",
                "judging_body": "PLENO",
                "is_collegiate": True,
                "decision_note": "Decisão colegiada materializada.",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [{"party_id": "party_1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A", "notes": None}],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {
                "link_id": "pp_1",
                "process_id": "proc_1",
                "party_id": "party_1",
                "role_in_case": "REQTE.(S)",
                "source_id": "juris",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {
                "counsel_id": "coun_1",
                "counsel_name_raw": "ADVOGADO A",
                "counsel_name_normalized": "ADVOGADO A",
                "notes": None,
            }
        ],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {
                "link_id": "pc_1",
                "process_id": "proc_1",
                "counsel_id": "coun_1",
                "side_in_case": "REQTE.(S)",
                "source_id": "juris",
            }
        ],
    )
    _write_jsonl(
        analytics_dir / "outlier_alert.jsonl",
        [
            {
                "alert_id": "alert_1",
                "process_id": "proc_1",
                "decision_event_id": "evt_1",
                "comparison_group_id": "grp_1",
                "alert_type": "atipicidade",
                "alert_score": 0.87,
                "expected_pattern": "Esperado colegiado homogêneo.",
                "observed_pattern": "Observado desvio pontual.",
                "evidence_summary": "Desvio comparativo materializado.",
                "uncertainty_note": None,
                "status": "novo",
                "created_at": "2026-01-31T12:00:00",
                "updated_at": "2026-01-31T12:05:00",
            }
        ],
    )
    _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.87})
    _write_json(analytics_dir / "comparison_group_summary.json", {"valid_group_count": 1})
    _write_json(analytics_dir / "baseline_summary.json", {"baseline_count": 1})

    code = main(
        [
            "serving",
            "build",
            "--database-url",
            f"sqlite+pysqlite:///{db_path}",
            "--curated-dir",
            str(curated_dir),
            "--analytics-dir",
            str(analytics_dir),
        ]
    )

    assert code == 0
    assert db_path.exists()


def test_cli_api_serve(monkeypatch, tmp_path: Path):
    captured: dict[str, object] = {}

    def fake_run(app_str, *, factory, host, port, reload):
        captured["app_str"] = app_str
        captured["factory"] = factory
        captured["host"] = host
        captured["port"] = port
        captured["reload"] = reload

    monkeypatch.setattr("uvicorn.run", fake_run)

    db_url = f"sqlite+pysqlite:///{tmp_path / 'serve.db'}"
    code = main(
        [
            "api",
            "serve",
            "--database-url",
            db_url,
            "--host",
            "0.0.0.0",
            "--port",
            "9001",
            "--reload",
        ]
    )

    assert code == 0
    assert captured["app_str"] == "atlas_stf.api.app:create_app"
    assert captured["factory"] is True
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 9001
    assert captured["reload"] is True
    assert os.environ.get("ATLAS_STF_DATABASE_URL") == db_url
