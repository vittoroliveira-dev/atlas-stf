"""Smoke validation tests — Phases B and C: match confidence and CGU normalization.

Validates:
  B. match_strategy / match_score / match_confidence in serving + API
  C. CGU CSV normalization (CPF/CNPJ, TIPO PESSOA, dates)
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine, inspect

from atlas_stf.api.app import create_app
from atlas_stf.cgu._runner import (
    _CEIS_COL,
    _load_csv_sanctions,
    _normalize_csv_record,
    _normalize_leniencia_record,
)
from atlas_stf.serving._builder_loaders_analytics import (
    load_sanction_matches as load_serving_sanction_matches,
)
from atlas_stf.serving.builder import build_serving_database
from tests._smoke_helpers import (
    _dm_record,
    _make_ceis_row,
    _make_leniencia_row,
    _sm_record,
    _write_json,
    _write_jsonl,
    _write_minimal_analytics,
    _write_minimal_curated,
)

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
            "CÓDIGO DA SANÇÃO",
            "TIPO DE PESSOA",
            "CPF OU CNPJ DO SANCIONADO",
            "NOME DO SANCIONADO",
            "NOME INFORMADO PELO ÓRGÃO SANCIONADOR",
            "RAZÃO SOCIAL - CADASTRO RECEITA",
            "NOME FANTASIA - CADASTRO RECEITA",
            "NÚMERO DO PROCESSO",
            "CATEGORIA DA SANÇÃO",
            "DATA INÍCIO SANÇÃO",
            "DATA FINAL SANÇÃO",
            "DATA PUBLICAÇÃO",
            "PUBLICAÇÃO",
            "DETALHAMENTO DO MEIO DE PUBLICAÇÃO",
            "DATA DO TRÂNSITO EM JULGADO",
            "ABRAGÊNCIA DA SANÇÃO",
            "ÓRGÃO SANCIONADOR",
            "UF ÓRGÃO SANCIONADOR",
            "ESFERA ÓRGÃO SANCIONADOR",
            "FUNDAMENTAÇÃO LEGAL",
            "DATA ORIGEM INFORMAÇÃO",
            "ORIGEM INFORMAÇÕES",
            "OBSERVAÇÕES",
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
                "",
                "",
                "",
                "",
                "",
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
                "",
                "",
                "",
                "",
                "",
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
