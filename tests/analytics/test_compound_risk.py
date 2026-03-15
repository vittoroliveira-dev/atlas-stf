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

    def test_returns_output_dir_when_required_inputs_are_missing(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "analytics"
        output_dir.mkdir(parents=True, exist_ok=True)

        result = build_compound_risk(
            curated_dir=tmp_path / "curated",
            analytics_dir=tmp_path / "analytics",
            output_dir=output_dir,
        )

        assert result == output_dir
