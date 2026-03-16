"""Tests for compound risk analytics builder."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.compound_risk import build_compound_risk


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class TestBuildCompoundRisk:
    def _setup(self, tmp_path: Path) -> dict[str, Path]:
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"

        _write_jsonl(
            curated_dir / "party.jsonl",
            [
                {"party_id": "p1", "party_name_raw": "AUTOR A", "party_name_normalized": "AUTOR A"},
                {"party_id": "p2", "party_name_raw": "REU B", "party_name_normalized": "REU B"},
            ],
        )
        _write_jsonl(
            curated_dir / "counsel.jsonl",
            [
                {"counsel_id": "c1", "counsel_name_raw": "ADV SILVA", "counsel_name_normalized": "ADV SILVA"},
            ],
        )
        _write_jsonl(
            curated_dir / "process_party_link.jsonl",
            [
                {"link_id": "pp1", "process_id": "proc_1", "party_id": "p1", "role_in_case": "REQTE.(S)"},
                {"link_id": "pp2", "process_id": "proc_2", "party_id": "p1", "role_in_case": "REQTE.(S)"},
                {"link_id": "pp3", "process_id": "proc_3", "party_id": "p2", "role_in_case": "REQDO.(A/S)"},
            ],
        )
        _write_jsonl(
            curated_dir / "process_counsel_link.jsonl",
            [
                {"link_id": "pc1", "process_id": "proc_1", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
                {"link_id": "pc2", "process_id": "proc_2", "counsel_id": "c1", "side_in_case": "REQTE.(S)"},
            ],
        )
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {
                    "decision_event_id": "evt_1",
                    "process_id": "proc_1",
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente",
                    "decision_date": "2021-03-15",
                },
                {
                    "decision_event_id": "evt_2",
                    "process_id": "proc_2",
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente",
                    "decision_date": "2023-07-10",
                },
                {
                    "decision_event_id": "evt_3",
                    "process_id": "proc_3",
                    "current_rapporteur": "MIN. OUTRO",
                    "decision_progress": "Improcedente",
                    "decision_date": "2022-01-05",
                },
            ],
        )

        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                {
                    "match_id": "sm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "entity_name_normalized": "AUTOR A",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "sanction_source": "CGU",
                    "sanction_id": "s1",
                    "favorable_rate_delta": 0.33,
                    "red_flag": True,
                }
            ],
        )
        _write_json(analytics_dir / "sanction_match_summary.json", {"red_flag_count": 1})

        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {
                    "match_id": "dm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "entity_name_normalized": "AUTOR A",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "donor_cpf_cnpj": "123",
                    "total_donated_brl": 100000.0,
                    "favorable_rate_delta": 0.24,
                    "red_flag": True,
                }
            ],
        )
        _write_json(analytics_dir / "donation_match_summary.json", {"red_flag_count": 1})

        _write_jsonl(
            analytics_dir / "corporate_network.jsonl",
            [
                {
                    "conflict_id": "cn1",
                    "minister_name": "MIN. TESTE",
                    "company_cnpj_basico": "12345678",
                    "company_name": "EMPRESA X",
                    "linked_entity_type": "party",
                    "linked_entity_id": "p1",
                    "linked_entity_name": "AUTOR A",
                    "shared_process_ids": ["proc_1", "proc_2"],
                    "shared_process_count": 2,
                    "favorable_rate_delta": 0.28,
                    "red_flag": True,
                    "link_degree": 1,
                    "link_chain": "MIN. TESTE -> EMPRESA X -> AUTOR A",
                }
            ],
        )
        _write_json(analytics_dir / "corporate_network_summary.json", {"red_flag_count": 1})

        _write_jsonl(
            analytics_dir / "counsel_affinity.jsonl",
            [
                {
                    "affinity_id": "ca1",
                    "rapporteur": "MIN. TESTE",
                    "counsel_id": "c1",
                    "counsel_name_normalized": "ADV SILVA",
                    "shared_case_count": 2,
                    "pair_delta_vs_minister": 0.21,
                    "pair_delta_vs_counsel": 0.19,
                    "top_process_classes": ["ADI"],
                    "red_flag": True,
                }
            ],
        )
        _write_json(analytics_dir / "counsel_affinity_summary.json", {"red_flag_count": 1})

        _write_jsonl(
            analytics_dir / "outlier_alert.jsonl",
            [
                {
                    "alert_id": "alert-1",
                    "process_id": "proc_1",
                    "decision_event_id": "evt_1",
                    "alert_type": "atipico",
                    "alert_score": 0.92,
                    "status": "novo",
                }
            ],
        )
        _write_json(analytics_dir / "outlier_alert_summary.json", {"alert_count": 1, "avg_score": 0.92})

        return {
            "curated_dir": curated_dir,
            "analytics_dir": analytics_dir,
            "output_dir": analytics_dir,
        }

    def test_builds_compound_pairs_with_converging_signals(self, tmp_path: Path) -> None:
        paths = self._setup(tmp_path)

        output_path = build_compound_risk(**paths)

        assert output_path.exists()
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert len(rows) == 2

        party_pair = next(row for row in rows if row["entity_type"] == "party")
        assert party_pair["minister_name"] == "MIN. TESTE"
        assert party_pair["entity_id"] == "p1"
        assert party_pair["signal_count"] == 4
        assert party_pair["signals"] == ["alert", "corporate", "donation", "sanction"]
        assert party_pair["red_flag"] is True
        assert party_pair["shared_process_count"] == 2
        assert party_pair["max_alert_score"] == 0.92

        counsel_pair = next(row for row in rows if row["entity_type"] == "counsel")
        assert counsel_pair["minister_name"] == "MIN. TESTE"
        assert counsel_pair["entity_id"] == "c1"
        assert counsel_pair["signal_count"] == 3
        assert counsel_pair["signals"] == ["affinity", "alert", "donation"]
        assert counsel_pair["supporting_party_ids"] == ["p1"]
        assert counsel_pair["donation_match_count"] == 1
        assert counsel_pair["donation_total_brl"] == 100000.0
        assert counsel_pair["red_flag"] is True

    def test_materializes_summary_with_top_pairs(self, tmp_path: Path) -> None:
        paths = self._setup(tmp_path)

        build_compound_risk(**paths)

        summary_path = paths["output_dir"] / "compound_risk_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["pair_count"] == 2
        assert summary["red_flag_count"] == 2
        assert summary["top_pairs"][0]["entity_id"] == "p1"
        assert summary["top_pairs"][0]["signal_count"] == 4

    def test_direct_counsel_sanction_creates_counsel_pair(self, tmp_path: Path) -> None:
        """When sanction_match has entity_type=counsel, it creates a direct counsel pair."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        # Add a direct counsel sanction match
        sm_text = (analytics_dir / "sanction_match.jsonl").read_text().strip()
        existing_sanctions = [json.loads(line) for line in sm_text.split("\n")]
        existing_sanctions.append(
            {
                "match_id": "sm2",
                "entity_type": "counsel",
                "entity_id": "c1",
                "entity_name_normalized": "ADV SILVA",
                "sanction_source": "ceis",
                "sanction_id": "s2",
                "favorable_rate_delta": 0.30,
                "red_flag": True,
            }
        )
        _write_jsonl(analytics_dir / "sanction_match.jsonl", existing_sanctions)

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        counsel_pair = next(row for row in rows if row["entity_type"] == "counsel")
        assert "sanction" in counsel_pair["signals"]

    def test_direct_counsel_donation_skips_cross_entity_inference(self, tmp_path: Path) -> None:
        """When counsel has a direct donation match, cross-entity inference is skipped."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        # Add a direct counsel donation match
        dm_text = (analytics_dir / "donation_match.jsonl").read_text().strip()
        existing_donations = [json.loads(line) for line in dm_text.split("\n")]
        existing_donations.append(
            {
                "match_id": "dm2",
                "entity_type": "counsel",
                "entity_id": "c1",
                "entity_name_normalized": "ADV SILVA",
                "donor_cpf_cnpj": "456",
                "total_donated_brl": 50000.0,
                "favorable_rate_delta": 0.20,
                "red_flag": True,
            }
        )
        _write_jsonl(analytics_dir / "donation_match.jsonl", existing_donations)

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        counsel_pair = next(row for row in rows if row["entity_type"] == "counsel")
        assert "donation" in counsel_pair["signals"]
        # With direct match, counsel should NOT have supporting_party_ids
        # because cross-entity inference is skipped for counsels with direct matches
        assert counsel_pair["supporting_party_ids"] == []

    def test_red_flag_substantive_true_qualifies_as_signal(self, tmp_path: Path) -> None:
        """When red_flag_substantive=True, row qualifies even if legacy red_flag=False."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                {
                    "match_id": "sm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "entity_name_normalized": "AUTOR A",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "sanction_source": "CGU",
                    "sanction_id": "s1",
                    "favorable_rate_delta": 0.33,
                    "red_flag": False,
                    "red_flag_substantive": True,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next((row for row in rows if row["entity_type"] == "party"), None)
        assert party_pair is not None
        assert "sanction" in party_pair["signals"]

    def test_red_flag_substantive_false_excludes_despite_legacy_true(self, tmp_path: Path) -> None:
        """When red_flag_substantive=False, row is excluded even if legacy red_flag=True."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                {
                    "match_id": "sm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "entity_name_normalized": "AUTOR A",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "sanction_source": "CGU",
                    "sanction_id": "s1",
                    "favorable_rate_delta": 0.33,
                    "red_flag": True,
                    "red_flag_substantive": False,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next((row for row in rows if row["entity_type"] == "party"), None)
        # Sanction signal should NOT be present — substantive governs
        if party_pair is not None:
            assert "sanction" not in party_pair["signals"]

    def test_red_flag_substantive_none_excludes_despite_legacy_true(self, tmp_path: Path) -> None:
        """When red_flag_substantive=None (insufficient data), row is excluded conservatively."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                {
                    "match_id": "sm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "entity_name_normalized": "AUTOR A",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "sanction_source": "CGU",
                    "sanction_id": "s1",
                    "favorable_rate_delta": 0.33,
                    "red_flag": True,
                    "red_flag_substantive": None,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next((row for row in rows if row["entity_type"] == "party"), None)
        # Sanction signal should NOT be present — None is not True
        if party_pair is not None:
            assert "sanction" not in party_pair["signals"]

    def test_missing_red_flag_substantive_field_uses_legacy_true(self, tmp_path: Path) -> None:
        """When red_flag_substantive key is absent, legacy red_flag=True governs (backward compat)."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        # Write sanction without red_flag_substantive key at all
        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                {
                    "match_id": "sm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "entity_name_normalized": "AUTOR A",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "sanction_source": "CGU",
                    "sanction_id": "s1",
                    "favorable_rate_delta": 0.33,
                    "red_flag": True,
                    # NO red_flag_substantive key
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next((row for row in rows if row["entity_type"] == "party"), None)
        assert party_pair is not None
        assert "sanction" in party_pair["signals"]

    def test_missing_red_flag_substantive_field_uses_legacy_false(self, tmp_path: Path) -> None:
        """When red_flag_substantive key is absent, legacy red_flag=False excludes the row."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                {
                    "match_id": "sm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "entity_name_normalized": "AUTOR A",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "sanction_source": "CGU",
                    "sanction_id": "s1",
                    "favorable_rate_delta": 0.33,
                    "red_flag": False,
                    # NO red_flag_substantive key
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next((row for row in rows if row["entity_type"] == "party"), None)
        if party_pair is not None:
            assert "sanction" not in party_pair["signals"]

    def test_red_flag_substantive_true_with_legacy_true(self, tmp_path: Path) -> None:
        """When both red_flag=True and red_flag_substantive=True, row qualifies."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        _write_jsonl(
            analytics_dir / "sanction_match.jsonl",
            [
                {
                    "match_id": "sm1",
                    "entity_type": "party",
                    "entity_id": "p1",
                    "entity_name_normalized": "AUTOR A",
                    "party_id": "p1",
                    "party_name_normalized": "AUTOR A",
                    "sanction_source": "CGU",
                    "sanction_id": "s1",
                    "favorable_rate_delta": 0.33,
                    "red_flag": True,
                    "red_flag_substantive": True,
                }
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        party_pair = next((row for row in rows if row["entity_type"] == "party"), None)
        assert party_pair is not None
        assert "sanction" in party_pair["signals"]

    def test_signal_details_keys_match_signals_list(self, tmp_path: Path) -> None:
        """signal_details keys must equal the signals present in the pair."""
        paths = self._setup(tmp_path)

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        for row in rows:
            assert set(row["signal_details"].keys()) == set(row["signals"])

    def test_signal_details_structure_per_type(self, tmp_path: Path) -> None:
        """Each signal type in signal_details has expected sub-fields."""
        paths = self._setup(tmp_path)

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        party_pair = next(row for row in rows if row["entity_type"] == "party")
        sd = party_pair["signal_details"]
        assert sd["sanction"]["count"] == 1
        assert sd["sanction"]["sources"] == ["CGU"]
        assert sd["donation"]["count"] == 1
        assert sd["donation"]["total_brl"] == 100000.0
        assert sd["corporate"]["count"] == 1
        assert sd["corporate"]["company_count"] == 1
        assert sd["corporate"]["min_link_degree"] == 1
        assert sd["alert"]["count"] == 1
        assert sd["alert"]["max_score"] == 0.92

        counsel_pair = next(row for row in rows if row["entity_type"] == "counsel")
        sd_c = counsel_pair["signal_details"]
        assert sd_c["affinity"]["count"] == 1
        assert sd_c["affinity"]["affinity_ids"] == ["ca1"]
        assert sd_c["donation"]["count"] == 1

    def test_earliest_latest_year_from_decision_dates(self, tmp_path: Path) -> None:
        """earliest_year/latest_year computed from decision_date of shared processes."""
        paths = self._setup(tmp_path)

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        party_pair = next(row for row in rows if row["entity_type"] == "party")
        # proc_1 -> 2021, proc_2 -> 2023
        assert party_pair["earliest_year"] == 2021
        assert party_pair["latest_year"] == 2023

        counsel_pair = next(row for row in rows if row["entity_type"] == "counsel")
        # proc_1 -> 2021, proc_2 -> 2023
        assert counsel_pair["earliest_year"] == 2021
        assert counsel_pair["latest_year"] == 2023

    def test_earliest_latest_year_none_when_no_dates(self, tmp_path: Path) -> None:
        """When decision_date is absent, earliest/latest_year are None."""
        paths = self._setup(tmp_path)
        curated_dir = paths["curated_dir"]

        # Rewrite decision events without decision_date
        _write_jsonl(
            curated_dir / "decision_event.jsonl",
            [
                {
                    "decision_event_id": "evt_1",
                    "process_id": "proc_1",
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente",
                },
                {
                    "decision_event_id": "evt_2",
                    "process_id": "proc_2",
                    "current_rapporteur": "MIN. TESTE",
                    "decision_progress": "Procedente",
                },
                {
                    "decision_event_id": "evt_3",
                    "process_id": "proc_3",
                    "current_rapporteur": "MIN. OUTRO",
                    "decision_progress": "Improcedente",
                },
            ],
        )

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        party_pair = next(row for row in rows if row["entity_type"] == "party")
        assert party_pair["earliest_year"] is None
        assert party_pair["latest_year"] is None

    def test_scl_alone_adds_sanction_family(self, tmp_path: Path) -> None:
        """Par only with SCL (no direct sanction) should have 'sanction' in signals."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        # Remove direct sanctions; keep only SCL
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
        paths = self._setup(tmp_path)
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
        paths = self._setup(tmp_path)
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
        paths = self._setup(tmp_path)
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
        paths = self._setup(tmp_path)
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
        paths = self._setup(tmp_path)
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

    def test_adjusted_rate_delta_none_when_no_rate_delta(self, tmp_path: Path) -> None:
        """When there is no rate delta at all, adjusted_rate_delta is None."""
        paths = self._setup(tmp_path)
        analytics_dir = paths["analytics_dir"]

        # Remove sources that provide favorable_rate_delta
        _write_jsonl(analytics_dir / "sanction_match.jsonl", [])
        _write_jsonl(analytics_dir / "donation_match.jsonl", [])
        _write_jsonl(analytics_dir / "corporate_network.jsonl", [])
        _write_jsonl(analytics_dir / "counsel_affinity.jsonl", [])

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        for row in rows:
            assert row["adjusted_rate_delta"] is None

    def test_enrichment_does_not_change_signal_count(self, tmp_path: Path) -> None:
        """is_law_firm_group=True does NOT create an extra signal."""
        paths = self._setup(tmp_path)
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
                {"match_id": "s1", "party_id": "pa", "sanction_source": "CGU", "sanction_id": "x",
                 "favorable_rate_delta": 0.10, "red_flag": True},
                {"match_id": "s2", "party_id": "pb", "sanction_source": "CGU", "sanction_id": "y",
                 "favorable_rate_delta": 0.10, "red_flag": True},
            ],
        )
        _write_jsonl(
            analytics_dir / "donation_match.jsonl",
            [
                {"match_id": "d1", "party_id": "pa", "donor_cpf_cnpj": "111",
                 "total_donated_brl": 1000, "favorable_rate_delta": 0.10, "red_flag": True,
                 "is_law_firm_group": True, "donor_group_has_minister_partner": True},
                {"match_id": "d2", "party_id": "pb", "donor_cpf_cnpj": "222",
                 "total_donated_brl": 1000, "favorable_rate_delta": 0.10, "red_flag": True},
            ],
        )
        _write_jsonl(analytics_dir / "outlier_alert.jsonl", [])

        output_path = build_compound_risk(
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=analytics_dir,
        )
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert len(rows) == 2
        # PARTE A has higher adjusted_rate_delta (multiplier 3.0) so comes first
        assert rows[0]["entity_id"] == "pa"
        assert rows[1]["entity_id"] == "pb"
        assert rows[0]["adjusted_rate_delta"] > rows[1]["adjusted_rate_delta"]

    def test_signal_details_donation_enrichment_metadata(self, tmp_path: Path) -> None:
        """signal_details['donation'] includes enrichment metadata."""
        paths = self._setup(tmp_path)
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

    def test_signal_details_sanction_includes_scl_metadata(self, tmp_path: Path) -> None:
        """signal_details['sanction'] includes scl_count and scl_min_degree when SCL present."""
        paths = self._setup(tmp_path)
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
        sd = party_pair["signal_details"]["sanction"]
        assert sd["scl_count"] == 1
        assert sd["scl_min_degree"] == 3

    def test_red_flag_threshold_regression(self, tmp_path: Path) -> None:
        """red_flag = signal_count >= 2 is unchanged by enrichment."""
        paths = self._setup(tmp_path)

        output_path = build_compound_risk(**paths)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        for row in rows:
            assert row["red_flag"] == (row["signal_count"] >= 2)

    def test_multiple_donation_rows_same_flag_no_multiplier_inflation(self, tmp_path: Path) -> None:
        """3 donation rows with is_law_firm_group=True: adjusted == max_rate_delta * 1.5, not more."""
        paths = self._setup(tmp_path)
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

    def test_returns_output_dir_when_required_inputs_are_missing(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "analytics"
        output_dir.mkdir(parents=True, exist_ok=True)

        result = build_compound_risk(
            curated_dir=tmp_path / "curated",
            analytics_dir=tmp_path / "analytics",
            output_dir=output_dir,
        )

        assert result == output_dir
