"""E2E smoke test: TSE raw CSV → parse → aggregate → match → events → serving → API.

Proves the full pipeline end-to-end using in-memory data (no network, no disk I/O
outside tmp_path).  Covers the complete data flow from CSV parsing through to the
API response layer.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from atlas_stf.analytics.donation_match import build_donation_matches
from atlas_stf.api.app import create_app
from atlas_stf.serving._builder_loaders_analytics import load_donation_events, load_donation_matches
from atlas_stf.serving.models import Base, ServingMetric, ServingSchemaMeta
from atlas_stf.tse._parser import normalize_donation_record, parse_receitas_csv
from tests.api.conftest import managed_engine


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _make_csv(tmp_path: Path) -> Path:
    """Build a minimal TSE receitas CSV with two donations from the same donor."""
    header = ";".join(
        [
            '"ANO_ELEICAO"',
            '"SG_UF"',
            '"DS_CARGO"',
            '"NM_CANDIDATO"',
            '"NR_CPF_CANDIDATO"',
            '"NR_CANDIDATO"',
            '"SG_PARTIDO"',
            '"NM_PARTIDO"',
            '"NM_DOADOR"',
            '"NM_DOADOR_RFB"',
            '"NM_DOADOR_ORIGINARIO"',
            '"NR_CPF_CNPJ_DOADOR"',
            '"CD_CNAE_DOADOR"',
            '"DS_CNAE_DOADOR"',
            '"SG_UF_DOADOR"',
            '"VR_RECEITA"',
            '"DT_RECEITA"',
            '"DS_RECEITA"',
        ]
    )
    rows = [
        ";".join(
            [
                '"2022"', '"SP"', '"SENADOR"', '"FULANO"', '"11111111111"', '"123"',
                '"PT"', '"PARTIDO DOS TRABALHADORES"',
                '"ACME CORP"', '"ACME CORP LTDA"', '"FUNDO ORIGEM SA"',
                '"12.345.678/0001-99"', '""', '""', '"SP"',
                '"50.000,00"', '"15/06/2022"', '"Doacao em dinheiro"',
            ]
        ),
        ";".join(
            [
                '"2018"', '"RJ"', '"DEPUTADO"', '"CICLANO"', '"22222222222"', '"456"',
                '"MDB"', '"MDB"',
                '"ACME CORP"', '"ACME CORP LTDA"', '""',
                '"12345678000199"', '""', '""', '"RJ"',
                '"30.000,00"', '"10/10/2018"', '"Transferencia"',
            ]
        ),
    ]
    csv_path = tmp_path / "receitas.csv"
    csv_path.write_text(f"{header}\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return csv_path


class TestTseEndToEnd:
    """Full pipeline: CSV → parse → normalize → JSONL → match → events → serving → API."""

    def test_full_pipeline(self, tmp_path: Path) -> None:
        # --- Phase 1: Parse CSV ---
        csv_path = _make_csv(tmp_path)
        raw_records = parse_receitas_csv(csv_path)
        assert len(raw_records) == 2

        # --- Phase 2: Normalize ---
        normalized = [normalize_donation_record(r, int(r["election_year_raw"])) for r in raw_records]
        assert normalized[0]["donor_name"] == "ACME CORP"
        assert normalized[0]["donor_name_originator"] == "FUNDO ORIGEM SA"
        assert normalized[0]["donation_date"] == "2022-06-15"
        # Same CPF with different formatting must normalize to same value
        assert normalized[0]["donor_cpf_cnpj"] == "12.345.678/0001-99"
        assert normalized[1]["donor_cpf_cnpj"] == "12345678000199"

        # --- Phase 3: Write raw JSONL ---
        tse_dir = tmp_path / "tse"
        tse_dir.mkdir()
        _write_jsonl(tse_dir / "donations_raw.jsonl", normalized)

        # --- Phase 4: Curated data ---
        curated_dir = tmp_path / "curated"
        _write_jsonl(
            curated_dir / "party.jsonl",
            [{"party_id": "p1", "party_name_raw": "ACME Corp", "party_name_normalized": "ACME CORP"}],
        )
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(
            curated_dir / "process.jsonl",
            [
                {"process_id": "proc1", "process_class": "RE"},
                {"process_id": "proc2", "process_class": "RE"},
                {"process_id": "proc3", "process_class": "RE"},
            ],
        )
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {"decision_event_id": "de1", "process_id": "proc1", "decision_progress": "Provido"},
                {"decision_event_id": "de2", "process_id": "proc2", "decision_progress": "Provido"},
                {"decision_event_id": "de3", "process_id": "proc3", "decision_progress": "Desprovido"},
            ],
        )
        _write_jsonl(
            curated_dir / "process_party_link.jsonl",
            [
                {"link_id": "ppl1", "process_id": "proc1", "party_id": "p1"},
                {"link_id": "ppl2", "process_id": "proc2", "party_id": "p1"},
                {"link_id": "ppl3", "process_id": "proc3", "party_id": "p1"},
            ],
        )
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])

        # --- Phase 5: Build matches ---
        analytics_dir = tmp_path / "analytics"
        build_donation_matches(
            tse_dir=tse_dir,
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
            process_path=curated_dir / "process.jsonl",
            decision_event_path=curated_dir / "decision_event.jsonl",
            process_party_link_path=curated_dir / "process_party_link.jsonl",
            process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
            output_dir=analytics_dir,
        )

        # Verify match output
        match_path = analytics_dir / "donation_match.jsonl"
        matches = [json.loads(line) for line in match_path.read_text().strip().split("\n") if line.strip()]
        party_matches = [m for m in matches if m["entity_type"] == "party"]
        assert len(party_matches) == 1
        m = party_matches[0]
        # P2: different formatting of same CPF → single aggregate
        assert m["total_donated_brl"] == 80000.0
        assert m["donation_count"] == 2
        # P1: originator preserved
        assert m["donor_name_originator"] == "FUNDO ORIGEM SA"
        assert m["donor_name_normalized"] == "ACME CORP LTDA"
        # P2: identity key uses normalized CPF
        assert m["donor_identity_key"] == "cpf:12345678000199"
        # P4: audit fields present
        assert "match_strategy" in m
        assert "matched_alias" in m
        assert "favorable_rate_substantive" in m

        # Verify events
        event_path = analytics_dir / "donation_event.jsonl"
        events = [json.loads(line) for line in event_path.read_text().strip().split("\n") if line.strip()]
        assert len(events) == 2
        # P3: dates preserved
        dates = sorted(e["donation_date"] for e in events if e.get("donation_date"))
        assert dates == ["2018-10-10", "2022-06-15"]
        # P1: originator in events
        originators = [e["donor_name_originator"] for e in events if e.get("donor_name_originator")]
        assert "FUNDO ORIGEM SA" in originators

        # --- Phase 6: Load into serving ---
        donation_match_rows, counsel_profiles = load_donation_matches(analytics_dir)
        assert len(donation_match_rows) == 1
        row = donation_match_rows[0]
        # P5: entity_id populated
        assert row.entity_id == "p1"
        # P4: audit fields loaded
        assert row.matched_alias is not None
        assert row.uncertainty_note is not None
        assert row.donor_name_normalized == "ACME CORP LTDA"
        assert row.donor_name_originator == "FUNDO ORIGEM SA"

        donation_event_rows = load_donation_events(analytics_dir)
        assert len(donation_event_rows) == 2

        # --- Phase 7: API layer ---
        db_path = tmp_path / "test_e2e.db"
        db_url = f"sqlite:///{db_path}"
        with managed_engine(db_url) as engine:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                with session.begin():
                    session.add_all(donation_match_rows)
                    session.add_all(donation_event_rows)
                    session.add(ServingMetric(key="alert_count", value_integer=0))
                    session.add(ServingMetric(key="avg_alert_score", value_float=0.0))
                    session.add(ServingMetric(key="valid_group_count", value_integer=0))
                    session.add(ServingMetric(key="baseline_count", value_integer=0))
                    session.add(
                        ServingSchemaMeta(
                            singleton_key="serving",
                            schema_version=8,
                            schema_fingerprint="e2e",
                            built_at=datetime.now(timezone.utc),
                        )
                    )

        app = create_app(database_url=db_url)
        with TestClient(app) as client:
            # List donations
            resp = client.get("/donations", params={"page": 1, "page_size": 10})
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] == 1
            item = data["items"][0]
            assert item["total_donated_brl"] == 80000.0
            assert item["entity_id"] == "p1"
            assert item["donor_name_normalized"] == "ACME CORP LTDA"
            assert item["donor_name_originator"] == "FUNDO ORIGEM SA"
            assert item["election_years"] == [2018, 2022]
            match_id = item["match_id"]

            # Get individual events
            resp2 = client.get(f"/donations/{match_id}/events")
            assert resp2.status_code == 200
            ev_data = resp2.json()
            assert ev_data["total"] == 2
            assert ev_data["items"][0]["election_year"] == 2022
            assert ev_data["items"][0]["donation_date"] == "2022-06-15"
            assert ev_data["items"][1]["election_year"] == 2018

            # Events only for this match
            resp3 = client.get("/donations/nonexistent/events")
            assert resp3.json()["total"] == 0
