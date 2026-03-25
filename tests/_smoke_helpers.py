"""Shared helpers and data builders for smoke validation tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _sm_record(
    match_id: str,
    *,
    strategy: str | None = None,
    score: float | None = None,
    party_id: str = "p1",
    name: str = "A",
    source: str = "ceis",
    fav_rate: float | None = None,
) -> dict[str, Any]:
    """Build a minimal sanction_match JSONL record."""
    rec: dict[str, Any] = {
        "match_id": match_id,
        "party_id": party_id,
        "party_name_normalized": name,
        "sanction_source": source,
        "sanction_id": f"s_{match_id}",
        "stf_case_count": 1,
        "red_flag": False,
        "entity_type": "party",
    }
    if strategy is not None:
        rec["match_strategy"] = strategy
    if score is not None:
        rec["match_score"] = score
    if fav_rate is not None:
        rec["favorable_rate"] = fav_rate
    return rec


def _dm_record(
    match_id: str,
    *,
    strategy: str = "tax_id",
    score: float = 1.0,
    party_id: str = "p1",
    name: str = "A",
    fav_rate: float | None = None,
) -> dict[str, Any]:
    """Build a minimal donation_match JSONL record."""
    rec: dict[str, Any] = {
        "match_id": match_id,
        "party_id": party_id,
        "party_name_normalized": name,
        "donor_cpf_cnpj": "12345678900",
        "total_donated_brl": 1000.0,
        "donation_count": 1,
        "stf_case_count": 1,
        "red_flag": False,
        "entity_type": "party",
        "match_strategy": strategy,
        "match_score": score,
    }
    if fav_rate is not None:
        rec["favorable_rate"] = fav_rate
    return rec


def _write_minimal_curated(curated_dir: Path) -> None:
    """Write the minimal curated fixtures required by build_serving_database."""
    _write_jsonl(
        curated_dir / "process.jsonl",
        [
            {
                "process_id": "proc_1",
                "process_number": "RE 1",
                "process_class": "RE",
                "branch_of_law": "DIREITO",
                "subjects_normalized": ["DIREITO"],
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
                "current_rapporteur": "MIN. X",
                "decision_type": "Final",
                "decision_progress": "Provido",
                "decision_origin": "JULGAMENTO",
                "judging_body": "TURMA",
                "is_collegiate": True,
            }
        ],
    )
    _write_jsonl(
        curated_dir / "party.jsonl",
        [
            {
                "party_id": "p1",
                "party_name_raw": "PARTE A",
                "party_name_normalized": "PARTE A",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "process_party_link.jsonl",
        [
            {
                "link_id": "pp1",
                "process_id": "proc_1",
                "party_id": "p1",
                "role_in_case": "RECTE.(S)",
                "source_id": "juris",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "counsel.jsonl",
        [
            {
                "counsel_id": "c1",
                "counsel_name_raw": "ADV A",
                "counsel_name_normalized": "ADV A",
            }
        ],
    )
    _write_jsonl(
        curated_dir / "process_counsel_link.jsonl",
        [
            {
                "link_id": "pc1",
                "process_id": "proc_1",
                "counsel_id": "c1",
                "side_in_case": "RECTE.(S)",
                "source_id": "juris",
            }
        ],
    )


def _write_minimal_analytics(analytics_dir: Path) -> None:
    """Write the minimal analytics fixtures for serving builder."""
    _write_jsonl(
        analytics_dir / "outlier_alert.jsonl",
        [
            {
                "alert_id": "alert_smoke_1",
                "process_id": "proc_1",
                "decision_event_id": "evt_1",
                "comparison_group_id": "grp_1",
                "alert_type": "atipicidade",
                "alert_score": 0.5,
                "expected_pattern": "Esperado.",
                "observed_pattern": "Observado.",
                "evidence_summary": "Smoke.",
                "status": "novo",
                "created_at": "2026-01-31T12:00:00",
                "updated_at": "2026-01-31T12:05:00",
            }
        ],
    )
    _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.5})
    _write_json(analytics_dir / "comparison_group_summary.json", {"valid_group_count": 1})
    _write_json(analytics_dir / "baseline_summary.json", {"baseline_count": 1})


def _make_ceis_row(
    *,
    name: str = "ACME CORP",
    cpf_cnpj: str = "",
    tipo: str = "J",
    start: str = "",
    end: str = "",
    body: str = "CGU",
    desc: str = "Impedimento",
    uf: str = "DF",
    sanction_id: str = "SAN001",
) -> list[str]:
    """Build a minimal CEIS-format CSV row (19 columns, 0-indexed)."""
    row = [""] * 19
    row[0] = "CEIS"
    row[1] = sanction_id
    row[2] = tipo
    row[3] = cpf_cnpj
    row[4] = name
    row[9] = "Impedimento de Licitar e Contratar"
    row[10] = start
    row[11] = end
    row[13] = desc
    row[17] = body
    row[18] = uf
    return row


def _make_leniencia_row(
    *,
    name: str = "LENIENCIA CORP",
    cnpj: str = "12345678000190",
    start: str = "01/01/2023",
    end: str = "31/12/2025",
) -> list[str]:
    """Build a minimal Leniencia-format CSV row (11 columns)."""
    row = [""] * 11
    row[0] = "1"
    row[1] = cnpj
    row[2] = name
    row[3] = "FANTASIA"
    row[4] = start
    row[5] = end
    row[6] = "Vigente"
    row[7] = "01/01/2023"
    row[8] = "PROC-123"
    row[9] = "Termos do acordo"
    row[10] = "CGU"
    return row
