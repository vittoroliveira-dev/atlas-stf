"""Tests for analytics/sanction_corporate_link.py."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.sanction_corporate_link import (
    RED_FLAG_DELTA_THRESHOLD,
    _degree_decay,
    _record_hash,
    build_sanction_corporate_links,
)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


# ---------------------------------------------------------------------------
# Shared fixtures helpers
# ---------------------------------------------------------------------------


def _setup_rfb(
    rfb_dir: Path,
    *,
    partners: list[dict] | None = None,
    companies: list[dict] | None = None,
    establishments: list[dict] | None = None,
) -> None:
    rfb_dir.mkdir(parents=True, exist_ok=True)
    if partners is not None:
        _write_jsonl(rfb_dir / "partners_raw.jsonl", partners)
    if companies is not None:
        _write_jsonl(rfb_dir / "companies_raw.jsonl", companies)
    if establishments is not None:
        _write_jsonl(rfb_dir / "establishments_raw.jsonl", establishments)


def _setup_curated(
    curated_dir: Path,
    *,
    parties: list[dict] | None = None,
    counsels: list[dict] | None = None,
    processes: list[dict] | None = None,
    decision_events: list[dict] | None = None,
    process_party_links: list[dict] | None = None,
    process_counsel_links: list[dict] | None = None,
) -> None:
    _write_jsonl(curated_dir / "party.jsonl", parties or [])
    _write_jsonl(curated_dir / "counsel.jsonl", counsels or [])
    _write_jsonl(curated_dir / "process.jsonl", processes or [])
    _write_jsonl(curated_dir / "decision_event.jsonl", decision_events or [])
    _write_jsonl(curated_dir / "process_party_link.jsonl", process_party_links or [])
    _write_jsonl(curated_dir / "process_counsel_link.jsonl", process_counsel_links or [])


def _setup_sanctions(cgu_dir: Path, sanctions: list[dict]) -> None:
    _write_jsonl(cgu_dir / "sanctions_raw.jsonl", sanctions)


# Shared partner fixture: co-partner "JOAO DA SILVA" (CPF 52998224725) at company 44556677.
# That name matches party p1 = "JOAO DA SILVA" in curated.
#
# Sanction CNPJ 11222333000181 → company 11222333 → partner list includes JOAO DA SILVA.
# So path A: sanction CNPJ → company 11222333 → co-partner JOAO DA SILVA = party STF.

PARTY_NAME = "JOAO DA SILVA"
PARTY_CPF = "52998224725"

# Valid CNPJ: 11222333000181
SANCTION_CNPJ = "11222333000181"
SANCTION_CNPJ_BASICO = "11222333"

# Another company where sanction appears as PJ partner
OTHER_COMPANY_CNPJ_BASICO = "99887766"


def _base_curated(curated_dir: Path, *, process_count: int = 4) -> None:
    """Set up curated data so JOAO DA SILVA is a party with `process_count` processes."""
    processes = [{"process_id": f"proc{i}", "process_class": "RE"} for i in range(process_count)]
    links = [
        {"link_id": f"ppl{i}", "process_id": f"proc{i}", "party_id": "p1"}
        for i in range(process_count)
    ]
    events = [
        {
            "decision_event_id": f"de{i}",
            "process_id": f"proc{i}",
            "decision_progress": "Provido",
            "judging_body": "Segunda Turma",
            "is_collegiate": True,
        }
        for i in range(process_count)
    ]
    _setup_curated(
        curated_dir,
        parties=[{"party_id": "p1", "party_name_normalized": PARTY_NAME}],
        processes=processes,
        decision_events=events,
        process_party_links=links,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


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


class TestEvidenceChainComplete:
    """evidence_chain should be a non-empty list of readable strings."""

    def test_evidence_chain_complete(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S010",
                    "sanction_source": "ceis",
                    "entity_name": "EMPRESA EVIDENCIA",
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
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "EVIDENCIA LTDA"},
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
        chain = records[0]["evidence_chain"]
        assert isinstance(chain, list)
        assert len(chain) >= 2
        assert all(isinstance(s, str) and len(s) > 0 for s in chain)
        # First element mentions the sanction source
        assert "CEIS" in chain[0].upper() or "ceis" in chain[0].lower()


class TestAuditFieldsPreserved:
    """matched_alias, matched_tax_id and uncertainty_note should be present."""

    def test_audit_fields_preserved(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S020",
                    "sanction_source": "ceis",
                    "entity_name": "EMPRESA AUDIT",
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
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "AUDIT LTDA"},
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
        rec = records[0]
        # These fields must be present in every record (even if None)
        assert "matched_alias" in rec
        assert "matched_tax_id" in rec
        assert "uncertainty_note" in rec
        assert "stf_match_strategy" in rec
        assert "stf_match_confidence" in rec


class TestDegreeAlwaysGe2:
    """All records must have link_degree >= 2."""

    def test_degree_always_ge_2(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S030",
                    "sanction_source": "ceis",
                    "entity_name": "GRAU MINIMO",
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
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "GRAU LTDA"},
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
        for rec in records:
            assert rec["link_degree"] >= 2


class TestRiskScoreDecay:
    """risk_score should equal delta * 0.5^(degree-2) for degree >= 2."""

    def test_degree_decay_function(self) -> None:
        assert _degree_decay(1) == 1.0
        assert _degree_decay(2) == 1.0
        assert _degree_decay(3) == 0.5
        assert _degree_decay(4) == 0.25

    def test_risk_score_decay(self, tmp_path: Path) -> None:
        """Degree-3 links (economic group) should have half the risk_score of degree-2."""
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S040",
                    "sanction_source": "ceis",
                    "entity_name": "EMPRESA DECAY",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )

        # Economic group: SANCTION_CNPJ_BASICO + "33445566" are in the same group.
        # Party is co-partner at "33445566" (reached via group expansion → degree=3).
        _write_jsonl(
            analytics_dir / "economic_group.jsonl",
            [
                {
                    "group_id": "G001",
                    "member_cnpjs": [SANCTION_CNPJ_BASICO, "33445566"],
                    "member_count": 2,
                    "is_law_firm_group": False,
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                # Co-partner at the GROUP member company (not the bridge itself)
                {
                    "cnpj_basico": "33445566",
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "EMPRESA DECAY LTDA"},
                {"cnpj_basico": "33445566", "razao_social": "GRUPO MEMBRO LTDA"},
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
        degree3 = [r for r in records if r["link_degree"] == 3]
        assert len(degree3) >= 1
        for rec in degree3:
            if rec["risk_score"] is not None and rec["favorable_rate_delta"] is not None:
                expected = rec["favorable_rate_delta"] * 0.5
                assert abs(rec["risk_score"] - expected) < 1e-9


class TestRedFlagThreshold:
    """red_flag=True iff risk_score > 0.15 AND process_count >= 3."""

    def test_red_flag_threshold(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S050",
                    "sanction_source": "ceis",
                    "entity_name": "REDFLAG CORP",
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
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "REDFLAG LTDA"},
            ],
            establishments=[],
        )
        # 4 processes all favorable → favorable_rate=1.0
        # Baseline with other unfavorable cases so delta > 0.15
        processes = [{"process_id": f"proc{i}", "process_class": "RE"} for i in range(12)]
        party_links = [
            {"link_id": f"ppl{i}", "process_id": f"proc{i}", "party_id": "p1"} for i in range(4)
        ]
        events = []
        # p1's cases: all Provido
        for i in range(4):
            events.append({
                "decision_event_id": f"de{i}",
                "process_id": f"proc{i}",
                "decision_progress": "Provido",
                "judging_body": "Segunda Turma",
                "is_collegiate": True,
            })
        # Other cases: mix (baseline ~50%)
        for i in range(4, 8):
            events.append({
                "decision_event_id": f"de{i}",
                "process_id": f"proc{i}",
                "decision_progress": "Provido",
                "judging_body": "Segunda Turma",
                "is_collegiate": True,
            })
        for i in range(8, 12):
            events.append({
                "decision_event_id": f"de{i}",
                "process_id": f"proc{i}",
                "decision_progress": "Desprovido",
                "judging_body": "Segunda Turma",
                "is_collegiate": True,
            })

        _setup_curated(
            curated_dir,
            parties=[{"party_id": "p1", "party_name_normalized": PARTY_NAME}],
            processes=processes,
            decision_events=events,
            process_party_links=party_links,
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
        assert len(records) >= 1
        rec = records[0]
        # favorable_rate=1.0, baseline≈0.667, delta≈0.333 > 0.15 AND process_count=4 >= 3
        assert rec["red_flag"] is True
        assert rec["risk_score"] is not None
        assert rec["risk_score"] > RED_FLAG_DELTA_THRESHOLD
        assert rec["stf_process_count"] >= 3
        # Power analysis fields
        assert "red_flag_power" in rec
        assert "red_flag_confidence" in rec

    def test_red_flag_false_insufficient_cases(self, tmp_path: Path) -> None:
        """With only 2 processes, red_flag should be False even with high delta."""
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S051",
                    "sanction_source": "ceis",
                    "entity_name": "FEW CASES",
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
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "FEW LTDA"},
            ],
            establishments=[],
        )
        # Only 2 processes for the party
        _base_curated(curated_dir, process_count=2)

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
        assert records[0]["red_flag"] is False


class TestEconomicGroupExpansion:
    """Company in economic group → group members generate matches."""

    def test_economic_group_expansion(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S060",
                    "sanction_source": "ceis",
                    "entity_name": "GRUPO SANCIONADO",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )

        group_member_cnpj = "77889900"
        _write_jsonl(
            analytics_dir / "economic_group.jsonl",
            [
                {
                    "group_id": "G100",
                    "member_cnpjs": [SANCTION_CNPJ_BASICO, group_member_cnpj],
                    "member_count": 2,
                    "is_law_firm_group": False,
                },
            ],
        )
        _setup_rfb(
            rfb_dir,
            partners=[
                {
                    "cnpj_basico": group_member_cnpj,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "GRUPO SANCIONADO LTDA"},
                {"cnpj_basico": group_member_cnpj, "razao_social": "MEMBRO GRUPO LTDA"},
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
        # Should have a degree-3 link via economic group
        group_links = [r for r in records if r["link_degree"] == 3]
        assert len(group_links) >= 1
        assert group_links[0]["economic_group_id"] == "G100"
        # source_datasets should include economic_group
        assert "economic_group" in group_links[0]["source_datasets"]


class TestRecordHashChangesWithPayload:
    """record_hash must change when risk_score or evidence_chain changes."""

    def test_record_hash_changes_with_payload(self) -> None:
        base = {
            "link_id": "test",
            "sanction_id": "S1",
            "risk_score": 0.3,
            "evidence_chain": ["step1", "step2"],
        }
        h1 = _record_hash(base)

        modified = {**base, "risk_score": 0.5}
        h2 = _record_hash(modified)
        assert h1 != h2

        modified2 = {**base, "evidence_chain": ["step1", "step2", "step3"]}
        h3 = _record_hash(modified2)
        assert h1 != h3


class TestDedupPreservesDistinctRoutes:
    """Same STF target with different bridge_link_basis → both preserved."""

    def test_dedup_preserves_distinct_routes(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        out_dir = tmp_path / "output"

        _setup_sanctions(
            cgu_dir,
            [
                {
                    "sanction_id": "S070",
                    "sanction_source": "ceis",
                    "entity_name": "DUAL PATH CORP",
                    "entity_cnpj_cpf": SANCTION_CNPJ,
                    "sanction_type": "Inidoneidade",
                },
            ],
        )
        # Path A: CNPJ → company directly → co-partner JOAO
        # Path B: CNPJ is PJ partner in another company → co-partner JOAO there too
        _setup_rfb(
            rfb_dir,
            partners=[
                # Co-partner at the direct company (path A)
                {
                    "cnpj_basico": SANCTION_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
                # Sanctioned CNPJ is partner at another company (path B)
                {
                    "cnpj_basico": OTHER_COMPANY_CNPJ_BASICO,
                    "partner_name": "DUAL PATH CORP",
                    "partner_cpf_cnpj": SANCTION_CNPJ,
                    "qualification_code": "22",
                },
                # Co-partner at that other company (path B)
                {
                    "cnpj_basico": OTHER_COMPANY_CNPJ_BASICO,
                    "partner_name": PARTY_NAME,
                    "partner_name_normalized": PARTY_NAME,
                    "partner_cpf_cnpj": PARTY_CPF,
                    "qualification_code": "49",
                },
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "DUAL PATH LTDA"},
                {"cnpj_basico": OTHER_COMPANY_CNPJ_BASICO, "razao_social": "OUTRA EMPRESA"},
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
        bases = {r["bridge_link_basis"] for r in records}
        # Both routes should be preserved
        assert "exact_cnpj_basico" in bases
        assert "exact_partner_cnpj" in bases


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
