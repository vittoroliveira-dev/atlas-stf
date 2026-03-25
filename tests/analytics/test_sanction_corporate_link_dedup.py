"""Regression tests for SCL deduplication, truncation, and multi-bridge routing."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.analytics.sanction_corporate_link import build_sanction_corporate_links
from tests.analytics._scl_helpers import (
    PARTY_CPF,
    PARTY_NAME,
    SANCTION_CNPJ,
    SANCTION_CNPJ_BASICO,
    _base_curated,
    _read_jsonl,
    _setup_rfb,
    _setup_sanctions,
    _write_jsonl,
)


class TestMultipleBridgesSameGroup:
    """A scan_cnpj belonging to a group reachable from multiple bridges
    should emit records for ALL relevant bridges, not just the first."""

    def test_all_bridges_emitted(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        # Sanction has CNPJ 11222333000181 (valid, cnpj_basico=11222333).
        # Path A: direct company 11222333 (co-partner matches STF party).
        # Path B: sanction CNPJ appears as PJ partner at a second company (44556677).
        # Both companies belong to the same economic group.
        # A co-partner at a third group member should produce records
        # attributed to BOTH bridges.
        bridge_a = SANCTION_CNPJ_BASICO  # 11222333 — direct (path A)
        bridge_b = "44556677"            # path B: sanction is PJ partner here
        group_member = "88990011"

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        _setup_rfb(
            rfb_dir,
            partners=[
                # Path B: sanction appears as PJ partner at bridge_b
                {"cnpj_basico": bridge_b, "partner_type": "1", "partner_cpf_cnpj": SANCTION_CNPJ,
                 "partner_name_normalized": "SANCIONADO"},
                # Co-partner at bridge_a matches STF party
                {"cnpj_basico": bridge_a, "partner_type": "2", "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
                # Co-partner at group_member also matches STF party
                {"cnpj_basico": group_member, "partner_type": "2", "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": bridge_a, "razao_social": "BRIDGE A LTDA"},
                {"cnpj_basico": bridge_b, "razao_social": "BRIDGE B LTDA"},
                {"cnpj_basico": group_member, "razao_social": "GROUP MEMBER LTDA"},
            ],
        )
        _base_curated(curated_dir)

        # Economic group links bridge_a, bridge_b, and group_member
        _write_jsonl(analytics_dir / "economic_group.jsonl", [
            {
                "group_id": "eg-test-multi-bridge",
                "member_cnpjs": [bridge_a, bridge_b, group_member],
                "member_count": 3,
                "is_law_firm_group": False,
            },
        ])

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=output_dir,
        )

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        bridge_cnpjs_emitted = {r["bridge_company_cnpj_basico"] for r in records}

        # Both bridges should appear, not just the first one
        assert bridge_a in bridge_cnpjs_emitted
        assert bridge_b in bridge_cnpjs_emitted


class TestTruncationSkipsExpansion:
    """When truncation triggers, degree-3 expansion must NOT happen.
    Only degree-2 (direct bridge) CNPJs should be scanned."""

    def test_truncated_sanction_only_scans_degree2(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        bridge_cnpj = SANCTION_CNPJ_BASICO

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])

        # Create a massive economic group (> 5000 members)
        group_members = [f"{i:08d}" for i in range(6000)]
        group_members[0] = bridge_cnpj

        _setup_rfb(
            rfb_dir,
            partners=[
                # Co-partner at bridge company matches STF party
                {"cnpj_basico": bridge_cnpj, "partner_type": "2", "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
                # Co-partner at a distant group member — should NOT be scanned if truncated
                {"cnpj_basico": "00005999", "partner_type": "2", "partner_cpf_cnpj": "99999999999",
                 "partner_name_normalized": "DISTANT PARTNER"},
            ],
            companies=[
                {"cnpj_basico": bridge_cnpj, "razao_social": "BRIDGE CO"},
            ],
        )
        _base_curated(curated_dir)

        _write_jsonl(analytics_dir / "economic_group.jsonl", [
            {
                "group_id": "eg-huge",
                "member_cnpjs": group_members,
                "member_count": len(group_members),
                "is_law_firm_group": False,
            },
        ])

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=output_dir,
        )

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")

        # Should find JOAO DA SILVA at degree 2 (direct bridge)
        degree2_records = [r for r in records if r["link_degree"] == 2]
        assert len(degree2_records) >= 1
        assert degree2_records[0]["stf_entity_name"] == PARTY_NAME

        # Should NOT find DISTANT PARTNER — group expansion was truncated
        distant = [r for r in records if "DISTANT" in str(r.get("stf_entity_name", ""))]
        assert len(distant) == 0

        # Truncation should be marked on records
        for r in records:
            assert r.get("truncated") is True
            assert r.get("pre_truncation_cnpj_count") is not None
            assert r["pre_truncation_cnpj_count"] > 5000


class TestNoSilentFallback:
    """When no co-partner matches any STF entity, zero records should be emitted."""

    def test_no_records_when_no_matching_partners(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": "99999999999",
                 "partner_name_normalized": "DESCONHECIDO TOTAL"},
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "BRIDGE CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [])

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=output_dir,
        )

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        assert len(records) == 0


class TestSchemaSemantics:
    """Validate semantic correctness of record fields."""

    def test_bridge_partner_role_from_qualification(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": "44556677", "partner_type": "1",
                 "partner_cpf_cnpj": SANCTION_CNPJ,
                 "partner_name_normalized": "SANCIONADO",
                 "qualification_label": "Sócio-Administrador"},
                {"cnpj_basico": "44556677", "partner_type": "2",
                 "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": "44556677", "razao_social": "EMPRESA B"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [])

        build_sanction_corporate_links(
            cgu_dir=cgu_dir, cvm_dir=tmp_path / "cvm", rfb_dir=rfb_dir,
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=output_dir,
        )

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        path_b = [r for r in records if r["bridge_link_basis"] == "exact_partner_cnpj"]
        assert len(path_b) >= 1
        assert path_b[0]["bridge_partner_role"] == "Sócio-Administrador"

    def test_bridge_confidence_deterministic(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "BRIDGE CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [])

        build_sanction_corporate_links(
            cgu_dir=cgu_dir, cvm_dir=tmp_path / "cvm", rfb_dir=rfb_dir,
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=output_dir,
        )

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        for r in records:
            assert r["bridge_confidence"] == "deterministic"

    def test_path_a_semantics_preserved(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "BRIDGE CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [])

        build_sanction_corporate_links(
            cgu_dir=cgu_dir, cvm_dir=tmp_path / "cvm", rfb_dir=rfb_dir,
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=output_dir,
        )

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        path_a = [r for r in records if r["bridge_link_basis"] == "exact_cnpj_basico"]
        assert len(path_a) >= 1
        for r in path_a:
            assert r["bridge_partner_role"] is None

    def test_truncation_fields_populated(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        group_members = [f"{i:08d}" for i in range(6000)]
        group_members[0] = SANCTION_CNPJ_BASICO
        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "BRIDGE CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [
            {
                "group_id": "eg-huge",
                "member_cnpjs": group_members,
                "member_count": len(group_members),
                "is_law_firm_group": False,
            },
        ])

        build_sanction_corporate_links(
            cgu_dir=cgu_dir, cvm_dir=tmp_path / "cvm", rfb_dir=rfb_dir,
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=output_dir,
        )

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        for r in records:
            assert r["truncation_reason"] is not None
            assert "exceeds" in r["truncation_reason"]
            assert r["post_truncation_cnpj_count"] is not None
            assert r["post_truncation_cnpj_count"] < r["pre_truncation_cnpj_count"]
            assert r["estimated_degree3_count"] > 0


class TestDedupPreservesDifferentBridgePartners:
    """One sanction that reaches the same bridge_cnpj via two different
    PJ partner records (path B) should emit records for both routes."""

    def test_two_path_b_routes_same_bridge(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        # Sanction CNPJ appears as PJ partner at TWO companies (bridge_a, bridge_b).
        # Both bridges are in the same group, and a co-partner at a group member
        # matches an STF party.
        bridge_a = SANCTION_CNPJ_BASICO  # direct (path A)
        bridge_b = "44556677"  # via path B

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        _setup_rfb(
            rfb_dir,
            partners=[
                # Path B: sanction appears as PJ partner at bridge_b
                {"cnpj_basico": bridge_b, "partner_type": "1", "partner_cpf_cnpj": SANCTION_CNPJ,
                 "partner_name_normalized": "SANCIONADO"},
                # Co-partner at bridge_a matches STF party (for path A)
                {"cnpj_basico": bridge_a, "partner_type": "2", "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
                # Co-partner at bridge_b also matches STF party (for path B)
                {"cnpj_basico": bridge_b, "partner_type": "2", "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": bridge_a, "razao_social": "BRIDGE A CO"},
                {"cnpj_basico": bridge_b, "razao_social": "BRIDGE B CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [])

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=output_dir,
        )

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        bridge_cnpjs = {r["bridge_company_cnpj_basico"] for r in records}
        link_bases = {r["bridge_link_basis"] for r in records}

        # Should have records from both bridges (path A + path B)
        assert len(records) >= 2
        assert bridge_a in bridge_cnpjs
        assert bridge_b in bridge_cnpjs
        assert "exact_cnpj_basico" in link_bases
        assert "exact_partner_cnpj" in link_bases
