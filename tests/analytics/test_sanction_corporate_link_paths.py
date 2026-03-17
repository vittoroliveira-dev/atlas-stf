"""Tests for sanction_corporate_link: bridge paths A/B/C, no fuzzy, graceful missing, non-regression."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.analytics.sanction_corporate_link import build_sanction_corporate_links
from tests.analytics._scl_helpers import (
    OTHER_COMPANY_CNPJ_BASICO,
    PARTY_CPF,
    PARTY_NAME,
    SANCTION_CNPJ,
    SANCTION_CNPJ_BASICO,
    _base_curated,
    _read_jsonl,
    _setup_curated,
    _setup_rfb,
    _setup_sanctions,
    _write_jsonl,
)


class TestPathACnpjBasico:
    """Path A: sanction CNPJ → company → co-partner = party STF."""

    def test_path_a_cnpj_basico(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S001",
                    "sanction_source": "ceis",
                    "entity_name": "EMPRESA SANCIONADA",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": "OUTRO SOCIO",
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                    "qualification_label": "Socio-Administrador",
                },
            ],
            companies=[
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "razao_social": "EMPRESA SANCIONADA LTDA",
                    "natureza_juridica": "2062",
                },
            ],
            establishments=[],
        )
        _base_curated(curated_dir)

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        assert len(records) >= 1
        exact = [r for r in records if r["bridge_link_basis"] == "exact_cnpj_basico"]
        assert len(exact) == 1
        assert exact[0]["stf_entity_name"] == PARTY_NAME
        assert exact[0]["bridge_confidence"] == "deterministic"
        assert exact[0]["link_degree"] == 2


class TestPathBPartnerCnpj:
    """Path B: sanction CNPJ appears as partner_cpf_cnpj (PJ partner) in another company."""

    def test_path_b_partner_cnpj(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S002",
                    "sanction_source": "ceis",
                    "entity_name": "PJ SANCIONADA",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                # The sanctioned CNPJ is PJ partner in another company
                {
                    "cnpj_basico": OTHER_COMPANY_CNPJ_BASICO,
                    "partner_name": "PJ SANCIONADA",
                    "partner_cpf_cnpj": SANCTION_CNPJ,
                    "qualification_code": "22",
                },
                # That other company has co-partner JOAO DA SILVA
                {
                    "cnpj_basico": OTHER_COMPANY_CNPJ_BASICO,
                    "partner_name": "JOAO DA SILVA CO",
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {
                    "cnpj_basico": OTHER_COMPANY_CNPJ_BASICO,
                    "razao_social": "HOLDING MASTER",
                    "natureza_juridica": "2062",
                },
            ],
            establishments=[],
        )
        _base_curated(curated_dir)

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        partner_links = [r for r in records if r["bridge_link_basis"] == "exact_partner_cnpj"]
        assert len(partner_links) >= 1
        assert partner_links[0]["bridge_company_name"] == "HOLDING MASTER"
        assert partner_links[0]["link_degree"] == 2


class TestPathCPartnerCpf:
    """Path C: sanction CPF appears as partner_cpf_cnpj (PF partner)."""

    def test_path_c_partner_cpf(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        # Valid CPF: 529.982.247-25
        sanction_cpf = "52998224725"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S003",
                    "sanction_source": "ceis",
                    "entity_name": "PESSOA SANCIONADA",
                    "entity_cnpj_cpf": sanction_cpf,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        # Use a different party name to avoid matching the sanction entity itself
        stf_party_name = "MARIA OLIVEIRA"
        stf_party_cpf = "39053344705"  # valid CPF

        _setup_rfb(
            rfb_dir,
            partners=[
                # Sanctioned person is partner at company
                {
                    "cnpj_basico": "55667788",
                    "partner_name": "PESSOA SANCIONADA",
                    "partner_cpf_cnpj": sanction_cpf,
                    "qualification_code": "49",
                },
                # Co-partner = STF party
                {
                    "cnpj_basico": "55667788",
                    "partner_name": "MARIA OLIVEIRA",
                    "partner_name_normalized": stf_party_name,
                    "partner_cpf_cnpj": stf_party_cpf,
                    "qualification_code": "22",
                },
            ],
            companies=[
                {
                    "cnpj_basico": "55667788",
                    "razao_social": "EMPRESA DO SOCIO",
                    "natureza_juridica": "2062",
                },
            ],
            establishments=[],
        )
        _setup_curated(
            curated_dir,
            parties=[{"party_id": "p1", "party_name_normalized": stf_party_name}],
            processes=[{"process_id": "proc0", "process_class": "RE"}],
            decision_events=[
                {"decision_event_id": "de0", "process_id": "proc0", "decision_progress": "Provido"},
            ],
            process_party_links=[{"link_id": "ppl0", "process_id": "proc0", "party_id": "p1"}],
        )

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        cpf_links = [r for r in records if r["bridge_link_basis"] == "exact_partner_cpf"]
        assert len(cpf_links) >= 1
        assert cpf_links[0]["stf_entity_name"] == stf_party_name
        assert cpf_links[0]["bridge_company_name"] == "EMPRESA DO SOCIO"


class TestNoFuzzyOnBridge:
    """Sanctions without valid doc should produce 0 records (no fuzzy on bridge)."""

    def test_no_fuzzy_on_bridge(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S004",
                    "sanction_source": "ceis",
                    "entity_name": "SEM DOCUMENTO",
                    "entity_cnpj_cpf": "",
                    "sanction_type": "Inidoneidade",
                },
                {
                    "sanction_id": "S005",
                    "sanction_source": "ceis",
                    "entity_name": "DOC INVALIDO",
                    "entity_cnpj_cpf": "12345678900",  # invalid CPF
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(rfb_dir, partners=[], companies=[], establishments=[])
        _base_curated(curated_dir)

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        records = _read_jsonl(out_dir / "sanction_corporate_link.jsonl")
        assert len(records) == 0


class TestNoSanctionFileGraceful:
    """Missing sanctions file → returns early without error."""

    def test_no_sanction_file_graceful(self, tmp_path: Path) -> None:
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_rfb(rfb_dir, partners=[], companies=[], establishments=[])
        _base_curated(curated_dir)

        result = build_sanction_corporate_links(
            cgu_dir=tmp_path / "empty_cgu",
            cvm_dir=tmp_path / "empty_cvm",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )
        # Returns output_dir (not output_path) when no sanctions found
        assert result == out_dir


class TestNonRegressionSanctionMatch:
    """Running builder must not modify sanction_match.jsonl."""

    def test_non_regression_sanction_match(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S099",
                    "sanction_source": "ceis",
                    "entity_name": "NOREGRESSAO CORP",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "NOREGRESSAO LTDA"},
            ],
            establishments=[],
        )
        _base_curated(curated_dir)

        # Pre-seed a sanction_match.jsonl in analytics dir
        sentinel = [{"match_id": "m1", "sanction_id": "S099", "sentinel": True}]
        _write_jsonl(analytics_dir / "sanction_match.jsonl", sentinel)
        original_content = (analytics_dir / "sanction_match.jsonl").read_text()

        build_sanction_corporate_links(
            cgu_dir=cgu_dir,
            cvm_dir=tmp_path / "cvm_empty",
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=out_dir,
        )

        # sanction_match.jsonl must be untouched
        assert (analytics_dir / "sanction_match.jsonl").read_text() == original_content
