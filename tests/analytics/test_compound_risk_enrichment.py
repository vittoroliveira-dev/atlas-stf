"""Enrichment compound risk tests: SCL, adjusted_rate_delta, sorting, metadata,
anti-inflation."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.compound_risk import build_compound_risk
from tests.analytics._compound_risk_helpers import _build_setup, _write_jsonl


class TestBuildCompoundRiskEnrichment:
    def test_scl_alone_adds_sanction_family(self, tmp_path: Path) -> None:
        """Par only with SCL (no direct sanction) should have 'sanction' in signals."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(analytics_dir / "sanction_match.jsonl", [])
        _write_jsonl(
            analytics_dir / "sanction_corporate_link.jsonl",
            [
                {
                    "link_id": "scl-1",
                    "stf_entity_type": "party",
                    "stf_entity_id": "p1",
                    "stf_entity_name": "AUTOR A",
                    "link_degree": 2,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        assert "sanction" in party_pair["signals"]
        assert party_pair["sanction_corporate_link_count"] == 1

    def test_direct_sanction_plus_scl_no_inflation(self, tmp_path: Path) -> None:
        """Par with direct sanction + SCL: 'sanction' appears once, signal_count not inflated."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "sanction_corporate_link.jsonl",
            [
                {
                    "link_id": "scl-1",
                    "stf_entity_type": "party",
                    "stf_entity_id": "p1",
                    "stf_entity_name": "AUTOR A",
                    "link_degree": 3,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        assert party_pair["signals"].count("sanction") == 1
        # signal_count = 4 (sanction, donation, corporate, alert) — SCL does NOT add a 5th signal
        assert party_pair["signal_count"] == 4

    def test_adjusted_rate_delta_law_firm_group(self, tmp_path: Path) -> None:
        """Donation with is_law_firm_group=True: adjusted_rate_delta == max_rate_delta * 1.5."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": "dm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "donor_cpf_cnpj": "123",
                    "total_donated_brl": 100000.0,
                    "favorable_rate_delta": 0.20,
                    "red_flag": True,
                    "is_law_firm_group": True,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        # max_rate_delta comes from sanction (0.33) which is larger than donation (0.20)
        # but adjusted_rate_delta uses max_rate_delta * 1.5
        assert party_pair["has_law_firm_group"] is True
        expected = party_pair["max_rate_delta"] * 1.5
        assert abs(party_pair["adjusted_rate_delta"] - expected) < 1e-9

    def test_adjusted_rate_delta_donor_group_has_minister_partner(self, tmp_path: Path) -> None:
        """Donation with donor_group_has_minister_partner=True: multiplier 2.0."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": "dm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "donor_cpf_cnpj": "123",
                    "total_donated_brl": 50000.0,
                    "favorable_rate_delta": 0.10,
                    "red_flag": True,
                    "donor_group_has_minister_partner": True,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        expected = party_pair["max_rate_delta"] * 2.0
        assert abs(party_pair["adjusted_rate_delta"] - expected) < 1e-9

    def test_adjusted_rate_delta_combined_multipliers(self, tmp_path: Path) -> None:
        """is_law_firm_group + donor_group_has_minister_partner: multiplier 3.0."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": "dm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "donor_cpf_cnpj": "123",
                    "total_donated_brl": 50000.0,
                    "favorable_rate_delta": 0.10,
                    "red_flag": True,
                    "is_law_firm_group": True,
                    "donor_group_has_minister_partner": True,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        expected = party_pair["max_rate_delta"] * 3.0
        assert abs(party_pair["adjusted_rate_delta"] - expected) < 1e-9

    def test_adjusted_rate_delta_attenuation_high_degree(self, tmp_path: Path) -> None:
        """min_link_degree_to_minister=4: multiplier 0.25."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": "dm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "donor_cpf_cnpj": "123",
                    "total_donated_brl": 50000.0,
                    "favorable_rate_delta": 0.10,
                    "red_flag": True,
                    "min_link_degree_to_minister": 4,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        # 0.5^(4-2) = 0.25
        expected = party_pair["max_rate_delta"] * 0.25
        assert abs(party_pair["adjusted_rate_delta"] - expected) < 1e-9

    def test_enrichment_does_not_change_signal_count(self, tmp_path: Path) -> None:
        """is_law_firm_group=True does NOT create an extra signal."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": "dm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "donor_cpf_cnpj": "123",
                    "total_donated_brl": 100000.0,
                    "favorable_rate_delta": 0.24,
                    "red_flag": True,
                    "is_law_firm_group": True,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        # Signals: sanction, donation, corporate, alert — exactly 4
        assert party_pair["signal_count"] == 4

    def test_sorting_uses_adjusted_rate_delta(self, tmp_path: Path) -> None:
        """2 pairs with same signal_count but different adjusted_rate_delta: higher comes first."""
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {"party_id": "pa", "party_name_normalized": "PARTE A"},
                {"party_id": "pb", "party_name_normalized": "PARTE B"},
            ],
        )
        _write_jsonl(curated_dir / "counsel.jsonl", [])
        _write_jsonl(
            curated_dir / "process_party_link.jsonl",
            [
                {"process_id": "proc_1", "party_id": "pa"},
                {"process_id": "proc_2", "party_id": "pb"},
            ],
        )
        _write_jsonl(curated_dir / "process_counsel_link.jsonl", [])
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {"decision_event_id": "e1", "process_id": "proc_1", "current_rapporteur": "MIN. X"},
                {"decision_event_id": "e2", "process_id": "proc_2", "current_rapporteur": "MIN. X"},
            ],
        )

        # Both have sanction + donation (2 signals each)
        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                {
                    "match_id": "s1",
                    "party_id": "pa",
                    "sanction_source": "CGU",
                    "sanction_id": "x",
                    "favorable_rate_delta": 0.10,
                    "red_flag": True,
                },
                {
                    "match_id": "s2",
                    "party_id": "pb",
                    "sanction_source": "CGU",
                    "sanction_id": "y",
                    "favorable_rate_delta": 0.10,
                    "red_flag": True,
                },
            ],
        )
        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": "d1",
                    "party_id": "pa",
                    "donor_cpf_cnpj": "111",
                    "total_donated_brl": 1000,
                    "favorable_rate_delta": 0.10,
                    "red_flag": True,
                    "is_law_firm_group": True,
                    "donor_group_has_minister_partner": True,
                },
                {
                    "match_id": "d2",
                    "party_id": "pb",
                    "donor_cpf_cnpj": "222",
                    "total_donated_brl": 1000,
                    "favorable_rate_delta": 0.10,
                    "red_flag": True,
                },
            ],
        )
        _write_jsonl(analytics_dir / "outlier_alert.jsonl", [])

        output_path = build_compound_risk(
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=analytics_dir,
        )
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert len(rows) == 2
        # PARTE A has higher adjusted_rate_delta (multiplier 3.0) so comes first
        assert rows[0]["entity_id"] == "pa"
        assert rows[1]["entity_id"] == "pb"
        assert rows[0]["adjusted_rate_delta"] > rows[1]["adjusted_rate_delta"]

    def test_signal_details_donation_enrichment_metadata(self, tmp_path: Path) -> None:
        """signal_details['donation'] includes enrichment metadata."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": "dm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "donor_cpf_cnpj": "123",
                    "total_donated_brl": 100000.0,
                    "favorable_rate_delta": 0.24,
                    "red_flag": True,
                    "is_law_firm_group": True,
                    "donor_group_has_minister_partner": True,
                    "donor_group_has_party_partner": True,
                    "donor_group_has_counsel_partner": False,
                    "min_link_degree_to_minister": 1,
                    "economic_group_member_count": 5,
                    "red_flag_power": 0.85,
                    "red_flag_confidence": "high",
                    "corporate_link_red_flag": True,
                    "economic_group_id": "eg-001",
                    "donor_cnpj_basico": "12345678",
                    "donor_company_name": "ACME CORP",
                    "match_strategy": "tax_id",
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        sd = party_pair["signal_details"]["donation"]
        assert sd["is_law_firm_group"] is True
        assert sd["donor_group_has_minister_partner"] is True
        assert sd["donor_group_has_party_partner"] is True
        assert "donor_group_has_counsel_partner" not in sd  # False is not emitted
        assert sd["min_link_degree_to_minister"] == 1
        assert sd["economic_group_member_count"] == 5
        assert sd["red_flag_power"] == 0.85
        assert sd["corporate_link_red_flag"] is True
        assert sd["economic_group_ids"] == ["eg-001"]
        assert sd["donor_cnpj_basicos"] == ["12345678"]
        assert sd["donor_company_names"] == ["ACME CORP"]
        assert sd["match_strategies"] == ["tax_id"]
        assert sd["red_flag_confidences"] == ["high"]

    def test_multiple_donation_rows_same_flag_no_multiplier_inflation(self, tmp_path: Path) -> None:
        """3 donation rows with is_law_firm_group=True: adjusted == max_rate_delta * 1.5, not more."""
        paths = _build_setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": f"dm{i}",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "donor_cpf_cnpj": f"{i}",
                    "total_donated_brl": 10000.0,
                    "favorable_rate_delta": 0.20,
                    "red_flag": True,
                    "is_law_firm_group": True,
                }
                for i in range(3)
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next(row for row in rows if row["entity_type"] == "party" and row["entity_id"] == "p1")
        # max_rate_delta is max of sanction(0.33) and donation(0.20) = 0.33
        # is_law_firm_group is binary OR: True regardless of how many rows
        # so adjusted = 0.33 * 1.5 = 0.495
        expected = party_pair["max_rate_delta"] * 1.5
        assert abs(party_pair["adjusted_rate_delta"] - expected) < 1e-9
