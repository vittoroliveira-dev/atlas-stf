"""Smoke validation tests — real entrypoints, minimal fixtures, no mocks.

Validates three phases of the saneamento v1.1 implementation:
  A. classify_outcome_materiality + compute_favorable_rate_substantive + builder output
  B. match_strategy / match_score / match_confidence in serving + API
  C. CGU CSV normalization (CPF/CNPJ, TIPO PESSOA, dates)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, inspect

from atlas_stf.analytics._match_helpers import compute_favorable_rate_substantive
from atlas_stf.analytics.sanction_match import build_sanction_matches
from atlas_stf.api.app import create_app
from atlas_stf.cgu._runner import (
    _CEIS_COL,
    _load_csv_sanctions,
    _normalize_csv_record,
    _normalize_leniencia_record,
)
from atlas_stf.core.rules import classify_outcome_materiality
from atlas_stf.serving._builder_loaders_analytics import (
    load_sanction_matches as load_serving_sanction_matches,
)
from atlas_stf.serving.builder import build_serving_database

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


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
    """Write the minimal analytics fixtures (empty) for serving builder."""
    _write_jsonl(analytics_dir / "outlier_alert.jsonl", [])
    _write_json(analytics_dir / "outlier_alert_summary.json", {})
    _write_json(analytics_dir / "comparison_group_summary.json", {})
    _write_json(analytics_dir / "baseline_summary.json", {})


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


# ======================================================================
# PHASE A — Materiality classification + substantive rate + builder
# ======================================================================


class TestPhaseAMateriality:
    """Validate classify_outcome_materiality and builder output."""

    @pytest.mark.parametrize(
        "decision_progress, expected",
        [
            ("Liminar deferida", "provisional"),
            ("Liminar prejudicada", "provisional"),
            ("Não conhecido", "procedural"),
            ("Negado seguimento", "procedural"),
            ("Homologada a desistência", "procedural"),
            ("Provido", "substantive"),
            ("Não provido", "substantive"),
            ("Procedente", "substantive"),
            ("Concedida a ordem", "substantive"),
            ("Embargos rejeitados", "unknown"),
            ("Deferido", "unknown"),
            ("Decisão referendada", "unknown"),
            ("Agravo regimental provido", "substantive"),
            ("JULGAMENTO DO PLENO - NEGADO PROVIMENTO", "substantive"),
        ],
    )
    def test_materiality_classification_all_categories(
        self,
        decision_progress: str,
        expected: str,
    ) -> None:
        result = classify_outcome_materiality(decision_progress)
        assert result == expected

    def test_substantive_rate_excludes_non_substantive(self) -> None:
        outcomes: list[tuple[str, str | None]] = [
            ("Provido", None),
            ("Não provido", None),
            ("Procedente", None),
            ("Liminar deferida", None),
            ("Liminar prejudicada", None),
            ("Negado seguimento", None),
            ("Embargos rejeitados", None),
        ]
        rate, n_sub = compute_favorable_rate_substantive(outcomes)
        assert n_sub == 3
        assert rate is not None
        assert abs(rate - 2 / 3) < 1e-6

    def test_substantive_rate_none_when_no_substantive(self) -> None:
        outcomes: list[tuple[str, str | None]] = [
            ("Liminar deferida", None),
            ("Liminar prejudicada", None),
            ("Negado seguimento", None),
        ]
        rate, n_sub = compute_favorable_rate_substantive(outcomes)
        assert n_sub == 0
        assert rate is None

    def test_builder_output_contains_substantive_fields(
        self,
        tmp_path: Path,
    ) -> None:
        curated = tmp_path / "curated"
        cgu = tmp_path / "cgu"
        analytics = tmp_path / "analytics"

        _write_jsonl(
            cgu / "sanctions_raw.jsonl",
            [
                {
                    "entity_name": "ACME CORP",
                    "sanction_source": "ceis",
                    "sanction_id": "s1",
                    "entity_cnpj_cpf": "",
                }
            ],
        )
        _write_jsonl(
            curated / "party.jsonl",
            [
                {
                    "party_id": "p1",
                    "party_name_raw": "ACME CORP",
                    "party_name_normalized": "ACME CORP",
                }
            ],
        )
        _write_jsonl(
            curated / "process.jsonl",
            [{"process_id": f"proc{i}", "process_number": f"RE {i}", "process_class": "RE"} for i in range(1, 5)],
        )
        _write_jsonl(
            curated / "decision_event.jsonl",
            [
                {"decision_event_id": "e1", "process_id": "proc1", "decision_progress": "Provido"},
                {"decision_event_id": "e2", "process_id": "proc2", "decision_progress": "Liminar deferida"},
                {"decision_event_id": "e3", "process_id": "proc3", "decision_progress": "Não provido"},
                {"decision_event_id": "e4", "process_id": "proc4", "decision_progress": "Provido"},
            ],
        )
        _write_jsonl(
            curated / "process_party_link.jsonl",
            [
                {"link_id": f"lnk{i}", "process_id": f"proc{i}", "party_id": "p1", "role_in_case": "RECTE.(S)"}
                for i in range(1, 5)
            ],
        )
        _write_jsonl(curated / "counsel.jsonl", [])
        _write_jsonl(curated / "process_counsel_link.jsonl", [])

        result_path = build_sanction_matches(
            cgu_dir=cgu,
            cvm_dir=tmp_path / "cvm_empty",
            party_path=curated / "party.jsonl",
            counsel_path=curated / "counsel.jsonl",
            process_path=curated / "process.jsonl",
            decision_event_path=curated / "decision_event.jsonl",
            process_party_link_path=curated / "process_party_link.jsonl",
            process_counsel_link_path=curated / "process_counsel_link.jsonl",
            output_dir=analytics,
            alias_path=curated / "entity_alias.jsonl",
        )

        lines = result_path.read_text(encoding="utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines if line]
        party_matches = [r for r in records if r["entity_type"] == "party"]
        assert len(party_matches) >= 1
        m = party_matches[0]

        assert m["favorable_rate"] is not None
        assert abs(m["favorable_rate"] - 0.75) < 1e-6

        assert m["favorable_rate_substantive"] is not None
        assert abs(m["favorable_rate_substantive"] - 2 / 3) < 1e-6

        assert m["substantive_decision_count"] == 3
        assert m["red_flag_substantive"] is not None
        assert isinstance(m["red_flag_substantive"], bool)
        assert "red_flag" in m
        assert isinstance(m["red_flag"], bool)

    def test_red_flag_substantive_none_below_threshold(
        self,
        tmp_path: Path,
    ) -> None:
        curated = tmp_path / "curated"
        cgu = tmp_path / "cgu"
        analytics = tmp_path / "analytics"

        _write_jsonl(
            cgu / "sanctions_raw.jsonl",
            [
                {
                    "entity_name": "SMALLCO",
                    "sanction_source": "ceis",
                    "sanction_id": "s2",
                    "entity_cnpj_cpf": "",
                }
            ],
        )
        _write_jsonl(
            curated / "party.jsonl",
            [
                {
                    "party_id": "p2",
                    "party_name_raw": "SMALLCO",
                    "party_name_normalized": "SMALLCO",
                }
            ],
        )
        _write_jsonl(
            curated / "process.jsonl",
            [{"process_id": f"sp{i}", "process_number": f"RE {i}", "process_class": "RE"} for i in range(1, 3)],
        )
        _write_jsonl(
            curated / "decision_event.jsonl",
            [
                {"decision_event_id": "se1", "process_id": "sp1", "decision_progress": "Provido"},
                {"decision_event_id": "se2", "process_id": "sp2", "decision_progress": "Não provido"},
            ],
        )
        _write_jsonl(
            curated / "process_party_link.jsonl",
            [
                {"link_id": f"sl{i}", "process_id": f"sp{i}", "party_id": "p2", "role_in_case": "RECTE.(S)"}
                for i in range(1, 3)
            ],
        )
        _write_jsonl(curated / "counsel.jsonl", [])
        _write_jsonl(curated / "process_counsel_link.jsonl", [])

        result_path = build_sanction_matches(
            cgu_dir=cgu,
            cvm_dir=tmp_path / "cvm_empty",
            party_path=curated / "party.jsonl",
            counsel_path=curated / "counsel.jsonl",
            process_path=curated / "process.jsonl",
            decision_event_path=curated / "decision_event.jsonl",
            process_party_link_path=curated / "process_party_link.jsonl",
            process_counsel_link_path=curated / "process_counsel_link.jsonl",
            output_dir=analytics,
            alias_path=curated / "entity_alias.jsonl",
        )

        lines = result_path.read_text(encoding="utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines if line]
        party_matches = [r for r in records if r["entity_type"] == "party"]
        assert len(party_matches) >= 1
        m = party_matches[0]

        assert m["substantive_decision_count"] == 2
        assert m["red_flag_substantive"] is None

    def test_favorable_rate_legacy_vs_substantive_differ(
        self,
        tmp_path: Path,
    ) -> None:
        curated = tmp_path / "curated"
        cgu = tmp_path / "cgu"
        analytics = tmp_path / "analytics"

        _write_jsonl(
            cgu / "sanctions_raw.jsonl",
            [
                {
                    "entity_name": "MIXCORP",
                    "sanction_source": "ceis",
                    "sanction_id": "s3",
                    "entity_cnpj_cpf": "",
                }
            ],
        )
        _write_jsonl(
            curated / "party.jsonl",
            [
                {
                    "party_id": "p3",
                    "party_name_raw": "MIXCORP",
                    "party_name_normalized": "MIXCORP",
                }
            ],
        )
        _write_jsonl(
            curated / "process.jsonl",
            [{"process_id": f"mp{i}", "process_number": f"RE {i}", "process_class": "RE"} for i in range(1, 5)],
        )
        _write_jsonl(
            curated / "decision_event.jsonl",
            [
                {"decision_event_id": "me1", "process_id": "mp1", "decision_progress": "Provido"},
                {"decision_event_id": "me2", "process_id": "mp2", "decision_progress": "Liminar deferida"},
                {"decision_event_id": "me3", "process_id": "mp3", "decision_progress": "Não provido"},
                {"decision_event_id": "me4", "process_id": "mp4", "decision_progress": "Provido"},
            ],
        )
        _write_jsonl(
            curated / "process_party_link.jsonl",
            [
                {"link_id": f"ml{i}", "process_id": f"mp{i}", "party_id": "p3", "role_in_case": "RECTE.(S)"}
                for i in range(1, 5)
            ],
        )
        _write_jsonl(curated / "counsel.jsonl", [])
        _write_jsonl(curated / "process_counsel_link.jsonl", [])

        result_path = build_sanction_matches(
            cgu_dir=cgu,
            cvm_dir=tmp_path / "cvm_empty",
            party_path=curated / "party.jsonl",
            counsel_path=curated / "counsel.jsonl",
            process_path=curated / "process.jsonl",
            decision_event_path=curated / "decision_event.jsonl",
            process_party_link_path=curated / "process_party_link.jsonl",
            process_counsel_link_path=curated / "process_counsel_link.jsonl",
            output_dir=analytics,
            alias_path=curated / "entity_alias.jsonl",
        )

        lines = result_path.read_text(encoding="utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines if line]
        party_matches = [r for r in records if r["entity_type"] == "party"]
        assert len(party_matches) >= 1
        m = party_matches[0]

        assert m["favorable_rate"] is not None
        assert m["favorable_rate_substantive"] is not None
        assert m["favorable_rate"] != m["favorable_rate_substantive"]


# ======================================================================
# PHASE B — match_strategy / match_score / match_confidence
# ======================================================================


class TestPhaseBMatchConfidence:
    """Validate match_strategy -> match_confidence mapping."""

    def test_serving_loader_match_confidence_mapping(
        self,
        tmp_path: Path,
    ) -> None:
        analytics_dir = tmp_path / "analytics"
        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                _sm_record("sm_tax", strategy="tax_id", score=1.0, name="A"),
                _sm_record("sm_exact", strategy="exact", score=1.0, name="B", party_id="p2"),
                _sm_record("sm_jac", strategy="jaccard", score=0.85, name="C", party_id="p3"),
                _sm_record("sm_amb", strategy="ambiguous", score=0.8, name="D", party_id="p4"),
                _sm_record("sm_unk", strategy="unknown_strategy_xyz", score=0.5, name="E", party_id="p5"),
            ],
        )

        matches, _profiles = load_serving_sanction_matches(analytics_dir)
        by_id = {m.match_id: m for m in matches}

        assert by_id["sm_tax"].match_confidence == "deterministic"
        assert by_id["sm_exact"].match_confidence == "exact_name"
        assert by_id["sm_jac"].match_confidence == "fuzzy"
        assert by_id["sm_amb"].match_confidence == "nominal_review_needed"
        assert by_id["sm_unk"].match_confidence == "unknown"

    def test_serving_db_schema_has_new_columns(
        self,
        tmp_path: Path,
    ) -> None:
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        _write_minimal_curated(curated_dir)
        _write_minimal_analytics(analytics_dir)

        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [_sm_record("sm1", strategy="exact", score=1.0, name="PARTE A")],
        )
        _write_json(analytics_dir / "sanction_match_summary.json", {})

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [_dm_record("dm1", name="PARTE A")],
        )
        _write_json(analytics_dir / "donation_match_summary.json", {})

        db_url = f"sqlite+pysqlite:///{tmp_path / 'test.db'}"
        build_serving_database(
            database_url=db_url,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
        )

        engine = create_engine(db_url)
        try:
            insp = inspect(engine)
            for table in ("serving_sanction_match", "serving_donation_match"):
                cols = {c["name"] for c in insp.get_columns(table)}
                assert "match_strategy" in cols, f"{table} missing match_strategy"
                assert "match_score" in cols, f"{table} missing match_score"
                assert "match_confidence" in cols, f"{table} missing match_confidence"
        finally:
            engine.dispose()

    @pytest.mark.anyio
    async def test_api_response_includes_match_fields(
        self,
        tmp_path: Path,
    ) -> None:
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        _write_minimal_curated(curated_dir)
        _write_minimal_analytics(analytics_dir)

        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [_sm_record("sm1", strategy="exact", score=1.0, name="PARTE A", fav_rate=0.75)],
        )
        _write_json(analytics_dir / "sanction_match_summary.json", {})

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [_dm_record("dm1", name="PARTE A", fav_rate=0.5)],
        )
        _write_json(analytics_dir / "donation_match_summary.json", {})

        db_url = f"sqlite+pysqlite:///{tmp_path / 'api_test.db'}"
        build_serving_database(
            database_url=db_url,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
        )

        app = create_app(database_url=db_url)
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                resp = await client.get("/sanctions", params={"page": 1, "page_size": 10})
                assert resp.status_code == 200
                data = resp.json()
                assert data["total"] >= 1
                item = data["items"][0]
                assert item["match_strategy"] == "exact"
                assert item["match_score"] == 1.0
                assert item["match_confidence"] == "exact_name"
                assert "favorable_rate" in item
                assert "red_flag" in item

                resp = await client.get("/donations", params={"page": 1, "page_size": 10})
                assert resp.status_code == 200
                data = resp.json()
                assert data["total"] >= 1
                item = data["items"][0]
                assert item["match_strategy"] == "tax_id"
                assert item["match_score"] == 1.0
                assert item["match_confidence"] == "deterministic"
                assert "favorable_rate" in item
                assert "red_flag" in item

    def test_backward_compat_old_jsonl_without_match_strategy(
        self,
        tmp_path: Path,
    ) -> None:
        analytics_dir = tmp_path / "analytics"
        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [_sm_record("sm_old", name="OLD")],
        )

        matches, _profiles = load_serving_sanction_matches(analytics_dir)
        assert len(matches) == 1
        m = matches[0]
        assert m.match_strategy is None
        assert m.match_confidence == "unknown"


# ======================================================================
# PHASE C — CGU CSV normalization
# ======================================================================


class TestPhaseCCguNormalization:
    """Validate CGU CEIS/CNEP/Leniencia CSV normalization."""

    @pytest.mark.parametrize(
        "cpf_cnpj_input, expected_normalized",
        [
            ("123.456.789-00", "12345678900"),
            ("12.345.678/0001-90", "12345678000190"),
            ("12345678900", "12345678900"),
        ],
    )
    def test_normalize_cpf_cnpj(
        self,
        cpf_cnpj_input: str,
        expected_normalized: str,
    ) -> None:
        row = _make_ceis_row(cpf_cnpj=cpf_cnpj_input)
        result = _normalize_csv_record(row, "ceis", _CEIS_COL)
        assert result["entity_cnpj_cpf"] == expected_normalized
        assert result["entity_cnpj_cpf_raw"] == cpf_cnpj_input

    @pytest.mark.parametrize(
        "tipo_input, expected_pf_pj",
        [
            ("F", "PF"),
            ("J", "PJ"),
            ("PF", "PF"),
            ("PJ", "PJ"),
            ("X", ""),
            ("", ""),
        ],
    )
    def test_normalize_tipo_pessoa(
        self,
        tipo_input: str,
        expected_pf_pj: str,
    ) -> None:
        row = _make_ceis_row(tipo=tipo_input)
        result = _normalize_csv_record(row, "ceis", _CEIS_COL)
        assert result["entity_type_pf_pj"] == expected_pf_pj
        assert result["entity_type_pf_pj_raw"] == tipo_input

    @pytest.mark.parametrize(
        "date_input, expected_date",
        [
            ("25/03/2023", "2023-03-25"),
            ("2023-03-25", "2023-03-25"),
            ("", None),
            ("03/2023", None),
            ("abc", None),
        ],
    )
    def test_normalize_dates(
        self,
        date_input: str,
        expected_date: str | None,
    ) -> None:
        row = _make_ceis_row(start=date_input)
        result = _normalize_csv_record(row, "ceis", _CEIS_COL)
        assert result["sanction_start_date"] == expected_date
        assert result["sanction_start_date_raw"] == date_input

    def test_load_csv_sanctions_normalizes_real_csv(
        self,
        tmp_path: Path,
    ) -> None:
        cols = [
            "CADASTRO",
            "SANCAO",
            "TIPO PESSOA",
            "CPF/CNPJ",
            "NOME",
            "ORG",
            "RAZAO",
            "FANTASIA",
            "PROCESSO",
            "CATEGORIA",
            "INICIO",
            "FINAL",
            "COL12",
            "DESCRICAO",
            "COL14",
            "COL15",
            "COL16",
            "ORGAO",
            "UF",
        ]
        header = ";".join(cols)
        row1 = ";".join(
            [
                "CEIS",
                "SAN001",
                "F",
                "123.456.789-00",
                "JOAO SILVA",
                "CGU",
                "",
                "",
                "PROC001",
                "Impedimento",
                "25/03/2023",
                "31/12/2025",
                "",
                "Impedido",
                "",
                "",
                "",
                "CGU",
                "DF",
            ]
        )
        row2 = ";".join(
            [
                "CEIS",
                "SAN002",
                "J",
                "12.345.678/0001-90",
                "EMPRESA LTDA",
                "CGU",
                "",
                "",
                "PROC002",
                "Suspensao",
                "2023-03-25",
                "2025-12-31",
                "",
                "Suspenso",
                "",
                "",
                "",
                "TCU",
                "SP",
            ]
        )
        csv_path = tmp_path / "ceis.csv"
        csv_path.write_text(f"{header}\n{row1}\n{row2}\n", encoding="utf-8")

        records = _load_csv_sanctions(csv_path, "ceis")
        assert len(records) == 2

        r1 = records[0]
        assert r1["entity_cnpj_cpf"] == "12345678900"
        assert r1["entity_cnpj_cpf_raw"] == "123.456.789-00"
        assert r1["entity_type_pf_pj"] == "PF"
        assert r1["entity_type_pf_pj_raw"] == "F"
        assert r1["sanction_start_date"] == "2023-03-25"
        assert r1["sanction_start_date_raw"] == "25/03/2023"
        assert r1["entity_name"] == "JOAO SILVA"

        r2 = records[1]
        assert r2["entity_cnpj_cpf"] == "12345678000190"
        assert r2["entity_cnpj_cpf_raw"] == "12.345.678/0001-90"
        assert r2["entity_type_pf_pj"] == "PJ"
        assert r2["entity_type_pf_pj_raw"] == "J"
        assert r2["sanction_start_date"] == "2023-03-25"
        assert r2["sanction_start_date_raw"] == "2023-03-25"

    def test_leniencia_always_pj(self) -> None:
        row = _make_leniencia_row()
        result = _normalize_leniencia_record(row)
        assert result["entity_type_pf_pj"] == "PJ"
        assert result["entity_type_pf_pj_raw"] == ""
        assert result["entity_name"] == "LENIENCIA CORP"
        assert result["sanction_source"] == "leniencia"
        assert result["sanction_id"] == "PROC-123"
