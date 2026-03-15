"""Tests for economic group analytics builder (Union-Find based)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from atlas_stf.analytics.economic_group import (
    _derive_pj_partner_basico,
    _UnionFind,
    build_economic_groups,
)
from tests.analytics.conftest import write_json, write_jsonl

# ---------------------------------------------------------------------------
# Union-Find unit tests
# ---------------------------------------------------------------------------


class TestUnionFind:
    def test_basic_operations(self) -> None:
        uf = _UnionFind()
        uf.make_set("a")
        uf.make_set("b")
        assert uf.find("a") != uf.find("b")
        uf.union("a", "b")
        assert uf.find("a") == uf.find("b")

    def test_connected_components(self) -> None:
        uf = _UnionFind()
        for x in ("a", "b", "c", "d"):
            uf.make_set(x)
        uf.union("a", "b")
        uf.union("c", "d")
        components = uf.components()
        assert len(components) == 2
        members = [sorted(v) for v in components.values()]
        assert sorted(members) == [["a", "b"], ["c", "d"]]

    def test_make_set_idempotent(self) -> None:
        uf = _UnionFind()
        uf.make_set("x")
        uf.make_set("x")  # duplicate — should not change state
        components = uf.components()
        assert len(components) == 1
        assert list(components.values())[0] == ["x"]


# ---------------------------------------------------------------------------
# _derive_pj_partner_basico unit tests
# ---------------------------------------------------------------------------


class TestDerivePjPartnerBasico:
    def test_14_digit_cnpj(self) -> None:
        counter: dict[str, int] = {}
        result = _derive_pj_partner_basico("12345678000195", discard_counter=counter)
        assert result == "12345678"
        assert counter.get("invalid_pj_cpf_cnpj_length", 0) == 0

    def test_8_digit_cnpj(self) -> None:
        counter: dict[str, int] = {}
        result = _derive_pj_partner_basico("12345678", discard_counter=counter)
        assert result == "12345678"

    def test_invalid_length(self) -> None:
        counter: dict[str, int] = {}
        result = _derive_pj_partner_basico("12345", discard_counter=counter)
        assert result is None
        assert counter["invalid_pj_cpf_cnpj_length"] == 1


# ---------------------------------------------------------------------------
# build_economic_groups integration tests
# ---------------------------------------------------------------------------


def _setup_rfb(
    tmp_path: Path,
    *,
    partners: list[dict],
    companies: list[dict] | None = None,
    establishments: list[dict] | None = None,
) -> Path:
    rfb_dir = tmp_path / "rfb"
    write_jsonl(rfb_dir / "partners_raw.jsonl", partners)
    if companies is not None:
        write_jsonl(rfb_dir / "companies_raw.jsonl", companies)
    if establishments is not None:
        write_jsonl(rfb_dir / "establishments_raw.jsonl", establishments)
    return rfb_dir


def _setup_curated(
    tmp_path: Path,
    *,
    minister_bio: dict | None = None,
    parties: list[dict] | None = None,
    counsels: list[dict] | None = None,
) -> Path:
    curated_dir = tmp_path / "curated"
    write_json(curated_dir / "minister_bio.json", minister_bio or {})
    write_jsonl(curated_dir / "party.jsonl", parties or [])
    write_jsonl(curated_dir / "counsel.jsonl", counsels or [])
    return curated_dir


def _read_groups(output_dir: Path) -> list[dict]:
    path = output_dir / "economic_group.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").strip().split("\n") if line.strip()]


class TestBuildEconomicGroups:
    def test_basic_pj_partner_link(self, tmp_path: Path) -> None:
        """Two companies linked through a PJ partner form one group."""
        rfb_dir = _setup_rfb(
            tmp_path,
            partners=[
                {
                    "cnpj_basico": "11111111",
                    "partner_type": "1",
                    "partner_name": "EMPRESA B LTDA",
                    "partner_name_normalized": "EMPRESA B LTDA",
                    "partner_cpf_cnpj": "22222222000199",
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": "11111111", "razao_social": "EMPRESA A LTDA", "capital_social": 100000.0},
                {"cnpj_basico": "22222222", "razao_social": "EMPRESA B LTDA", "capital_social": 50000.0},
            ],
        )
        curated_dir = _setup_curated(tmp_path)
        output_dir = tmp_path / "analytics"

        build_economic_groups(
            rfb_dir=rfb_dir,
            output_dir=output_dir,
            minister_bio_path=curated_dir / "minister_bio.json",
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
        )

        groups = _read_groups(output_dir)
        multi = [g for g in groups if g["member_count"] > 1]
        assert len(multi) >= 1
        linked = multi[0]
        assert set(linked["member_cnpjs"]) == {"11111111", "22222222"}

    def test_singleton_groups(self, tmp_path: Path) -> None:
        """Company with no PJ links produces a singleton group."""
        rfb_dir = _setup_rfb(
            tmp_path,
            partners=[
                {
                    "cnpj_basico": "33333333",
                    "partner_type": "2",
                    "partner_name": "JOSE DA SILVA",
                    "partner_name_normalized": "JOSE DA SILVA",
                    "partner_cpf_cnpj": "12345678901",
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": "33333333", "razao_social": "EMPRESA SOLO LTDA"},
            ],
        )
        curated_dir = _setup_curated(tmp_path)
        output_dir = tmp_path / "analytics"

        build_economic_groups(
            rfb_dir=rfb_dir,
            output_dir=output_dir,
            minister_bio_path=curated_dir / "minister_bio.json",
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
        )

        groups = _read_groups(output_dir)
        singletons = [g for g in groups if g["member_count"] == 1]
        assert len(singletons) >= 1

    def test_law_firm_detection(self, tmp_path: Path) -> None:
        """Establishment with CNAE starting with 6911 flags group as law firm."""
        rfb_dir = _setup_rfb(
            tmp_path,
            partners=[
                {
                    "cnpj_basico": "44444444",
                    "partner_type": "2",
                    "partner_name": "ADV X",
                    "partner_name_normalized": "ADV X",
                    "partner_cpf_cnpj": "99988877766",
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": "44444444", "razao_social": "ESCRITORIO X ADVOGADOS"},
            ],
            establishments=[
                {"cnpj_basico": "44444444", "cnae_fiscal": "6911702", "situacao_cadastral": "02", "uf": "SP"},
            ],
        )
        curated_dir = _setup_curated(tmp_path)
        output_dir = tmp_path / "analytics"

        build_economic_groups(
            rfb_dir=rfb_dir,
            output_dir=output_dir,
            minister_bio_path=curated_dir / "minister_bio.json",
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
        )

        groups = _read_groups(output_dir)
        law_firms = [g for g in groups if g["is_law_firm_group"]]
        assert len(law_firms) >= 1

    def test_minister_party_counsel_flags(self, tmp_path: Path) -> None:
        """Partner names matching curated entities set the correct flags."""
        rfb_dir = _setup_rfb(
            tmp_path,
            partners=[
                {
                    "cnpj_basico": "55555555",
                    "partner_type": "2",
                    "partner_name": "MIN TESTE",
                    "partner_name_normalized": "MIN TESTE",
                    "partner_cpf_cnpj": "11122233344",
                    "qualification_code": "49",
                },
                {
                    "cnpj_basico": "55555555",
                    "partner_type": "2",
                    "partner_name": "AUTOR A",
                    "partner_name_normalized": "AUTOR A",
                    "partner_cpf_cnpj": "55566677788",
                    "qualification_code": "22",
                },
                {
                    "cnpj_basico": "55555555",
                    "partner_type": "2",
                    "partner_name": "ADV B",
                    "partner_name_normalized": "ADV B",
                    "partner_cpf_cnpj": "99988877766",
                    "qualification_code": "22",
                },
            ],
            companies=[
                {"cnpj_basico": "55555555", "razao_social": "EMPRESA MISTA LTDA"},
            ],
        )
        curated_dir = _setup_curated(
            tmp_path,
            minister_bio={"m1": {"minister_name": "MIN TESTE"}},
            parties=[{"party_id": "p1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A"}],
            counsels=[{"counsel_id": "c1", "counsel_name_raw": "ADV B", "counsel_name_normalized": "ADV B"}],
        )
        output_dir = tmp_path / "analytics"

        build_economic_groups(
            rfb_dir=rfb_dir,
            output_dir=output_dir,
            minister_bio_path=curated_dir / "minister_bio.json",
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
        )

        groups = _read_groups(output_dir)
        group = [g for g in groups if "55555555" in g["member_cnpjs"]][0]
        assert group["has_minister_partner"] is True
        assert group["has_party_partner"] is True
        assert group["has_counsel_partner"] is True

    def test_establishment_counting(self, tmp_path: Path) -> None:
        """Active and total establishment counts are aggregated correctly."""
        rfb_dir = _setup_rfb(
            tmp_path,
            partners=[
                {
                    "cnpj_basico": "66666666",
                    "partner_type": "2",
                    "partner_name": "PESSOA X",
                    "partner_name_normalized": "PESSOA X",
                    "partner_cpf_cnpj": "12312312300",
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": "66666666", "razao_social": "EMPRESA F LTDA"},
            ],
            establishments=[
                {"cnpj_basico": "66666666", "cnae_fiscal": "4711301", "situacao_cadastral": "02", "uf": "SP"},
                {"cnpj_basico": "66666666", "cnae_fiscal": "4711301", "situacao_cadastral": "08", "uf": "RJ"},
                {"cnpj_basico": "66666666", "cnae_fiscal": "4711301", "situacao_cadastral": "02", "uf": "MG"},
            ],
        )
        curated_dir = _setup_curated(tmp_path)
        output_dir = tmp_path / "analytics"

        build_economic_groups(
            rfb_dir=rfb_dir,
            output_dir=output_dir,
            minister_bio_path=curated_dir / "minister_bio.json",
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
        )

        groups = _read_groups(output_dir)
        group = [g for g in groups if "66666666" in g["member_cnpjs"]][0]
        assert group["total_establishment_count"] == 3
        assert group["active_establishment_count"] == 2

    def test_no_partners_file_returns_early(self, tmp_path: Path) -> None:
        """When partners_raw.jsonl does not exist, builder returns output_dir."""
        rfb_dir = tmp_path / "rfb"
        rfb_dir.mkdir(parents=True, exist_ok=True)
        output_dir = tmp_path / "analytics"
        result = build_economic_groups(rfb_dir=rfb_dir, output_dir=output_dir)
        assert result == output_dir

    def test_large_group_warning(self, tmp_path: Path, caplog) -> None:
        """Groups with > 200 members emit a warning."""
        # Build 201 partners each at a different company, all linked via PJ to a hub
        hub_cnpj = "00000000"
        partners: list[dict] = []
        companies: list[dict] = [{"cnpj_basico": hub_cnpj, "razao_social": "HUB LTDA"}]
        for i in range(201):
            cnpj = f"{i + 1:08d}"
            partners.append(
                {
                    "cnpj_basico": cnpj,
                    "partner_type": "1",
                    "partner_name": f"PJ {i}",
                    "partner_name_normalized": f"PJ {i}",
                    "partner_cpf_cnpj": f"{hub_cnpj}000199",
                    "qualification_code": "49",
                },
            )
            companies.append({"cnpj_basico": cnpj, "razao_social": f"EMPRESA {i} LTDA"})

        rfb_dir = _setup_rfb(tmp_path, partners=partners, companies=companies)
        curated_dir = _setup_curated(tmp_path)
        output_dir = tmp_path / "analytics"

        with caplog.at_level(logging.WARNING, logger="atlas_stf.analytics.economic_group"):
            build_economic_groups(
                rfb_dir=rfb_dir,
                output_dir=output_dir,
                minister_bio_path=curated_dir / "minister_bio.json",
                party_path=curated_dir / "party.jsonl",
                counsel_path=curated_dir / "counsel.jsonl",
            )

        assert any("Large economic group detected" in r.message for r in caplog.records)
