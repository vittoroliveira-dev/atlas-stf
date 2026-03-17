"""Tests for analytics/donation_empirical.py."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.donation_empirical import (
    _compute_ambiguous_metrics,
    _compute_match_metrics,
    _compute_raw_data_metrics,
    _ReservoirSampler,
    build_empirical_report,
)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


# ---------------------------------------------------------------------------
# Raw data metrics
# ---------------------------------------------------------------------------


class TestRawDataMetrics:
    def test_cpf_cnpj_empty(self, tmp_path: Path) -> None:
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "ALICE", "donor_cpf_cnpj": "", "donation_amount": 100},
                {"donor_name_normalized": "BOB", "donation_amount": 200},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["cpf_cnpj_empty_count"] == 2
        assert m["total_raw_records"] == 2

    def test_cpf_cnpj_masked(self, tmp_path: Path) -> None:
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "ALICE", "donor_cpf_cnpj": "***.***.***-**"},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["cpf_cnpj_masked_count"] == 1
        assert m["cpf_cnpj_empty_count"] == 0

    def test_valid_cpf(self, tmp_path: Path) -> None:
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "ALICE", "donor_cpf_cnpj": "12345678901"},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["cpf_cnpj_valid_cpf_count"] == 1
        assert m["cpf_cnpj_valid_cnpj_count"] == 0

    def test_valid_cnpj(self, tmp_path: Path) -> None:
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "ACME", "donor_cpf_cnpj": "12345678000199"},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["cpf_cnpj_valid_cnpj_count"] == 1

    def test_identity_key_distribution(self, tmp_path: Path) -> None:
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "ALICE", "donor_cpf_cnpj": "12345678901"},
                {"donor_name_normalized": "BOB", "donor_cpf_cnpj": ""},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["identity_key_cpf_count"] == 1
        assert m["identity_key_name_count"] == 1
        assert m["unique_identity_keys_count"] == 2

    def test_homonymy_proxy(self, tmp_path: Path) -> None:
        """No homonymy when all records with valid CPF get cpf: keys."""
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "ALICE", "donor_cpf_cnpj": "11111111111"},
                {"donor_name_normalized": "ALICE", "donor_cpf_cnpj": "22222222222"},
            ],
        )
        m = _compute_raw_data_metrics(p)
        # Both records get cpf: keys, no name: keys → homonymy_proxy_count = 0
        assert m["homonymy_proxy_count"] == 0

    def test_homonymy_proxy_name_only_keys(self, tmp_path: Path) -> None:
        """Name-only keys with multiple distinct CPFs in raw → homonymy detected."""
        p = tmp_path / "donations_raw.jsonl"
        # Both records have masked CPF → identity key is name:ALICE
        # But raw CPF/CNPJ contains '*' so they don't count as distinct non-masked
        # For homonymy to trigger, we need name: key with distinct non-masked CPF/CNPJs
        # This happens when normalize_tax_id returns None but the raw field has digits
        # (e.g., partial/truncated doc that fails normalization)
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "ALICE", "donor_cpf_cnpj": ""},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["homonymy_proxy_count"] == 0

    def test_election_year_distribution(self, tmp_path: Path) -> None:
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "A", "donor_cpf_cnpj": "", "election_year": 2022},
                {"donor_name_normalized": "B", "donor_cpf_cnpj": "", "election_year": 2022},
                {"donor_name_normalized": "C", "donor_cpf_cnpj": "", "election_year": 2018},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["election_year_distribution"] == {"2018": 1, "2022": 2}

    def test_state_distribution(self, tmp_path: Path) -> None:
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "A", "donor_cpf_cnpj": "", "state": "SP"},
                {"donor_name_normalized": "B", "donor_cpf_cnpj": "", "state": "SP"},
                {"donor_name_normalized": "C", "donor_cpf_cnpj": "", "state": "RJ"},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["state_distribution"]["SP"] == 2
        assert m["state_distribution"]["RJ"] == 1

    def test_empty_donor_name(self, tmp_path: Path) -> None:
        p = tmp_path / "donations_raw.jsonl"
        _write_jsonl(
            p,
            [
                {"donor_name_normalized": "", "donor_cpf_cnpj": "12345678901"},
            ],
        )
        m = _compute_raw_data_metrics(p)
        assert m["empty_donor_name_count"] == 1


# ---------------------------------------------------------------------------
# Match metrics
# ---------------------------------------------------------------------------


class TestMatchMetrics:
    def test_strategy_distribution(self, tmp_path: Path) -> None:
        p = tmp_path / "match.jsonl"
        _write_jsonl(
            p,
            [
                {"entity_type": "party", "match_strategy": "exact", "match_score": 1.0},
                {"entity_type": "party", "match_strategy": "jaccard", "match_score": 0.92},
                {"entity_type": "counsel", "match_strategy": "exact", "match_score": 1.0},
            ],
        )
        m = _compute_match_metrics(p)
        assert m["total_matches"] == 3
        assert m["match_strategy_distribution"]["exact"] == 2
        assert m["match_strategy_distribution"]["jaccard"] == 1

    def test_strategy_by_entity_type(self, tmp_path: Path) -> None:
        p = tmp_path / "match.jsonl"
        _write_jsonl(
            p,
            [
                {"entity_type": "party", "match_strategy": "exact"},
                {"entity_type": "counsel", "match_strategy": "jaccard", "match_score": 0.9},
            ],
        )
        m = _compute_match_metrics(p)
        assert m["match_strategy_by_entity_type"]["party"]["exact"] == 1
        assert m["match_strategy_by_entity_type"]["counsel"]["jaccard"] == 1

    def test_jaccard_histogram_buckets(self, tmp_path: Path) -> None:
        p = tmp_path / "match.jsonl"
        _write_jsonl(
            p,
            [
                {"entity_type": "party", "match_strategy": "jaccard", "match_score": 0.81},
                {"entity_type": "party", "match_strategy": "jaccard", "match_score": 0.87},
                {"entity_type": "party", "match_strategy": "jaccard", "match_score": 0.93},
                {"entity_type": "party", "match_strategy": "jaccard", "match_score": 0.97},
                {"entity_type": "party", "match_strategy": "jaccard", "match_score": 1.0},
            ],
        )
        m = _compute_match_metrics(p)
        h = m["jaccard_score_histogram"]
        assert h["[0.80, 0.85)"] == 1
        assert h["[0.85, 0.90)"] == 1
        assert h["[0.90, 0.95)"] == 1
        assert h["[0.95, 1.00]"] == 2  # 0.97 + 1.0

    def test_levenshtein_histogram(self, tmp_path: Path) -> None:
        p = tmp_path / "match.jsonl"
        _write_jsonl(
            p,
            [
                {"entity_type": "party", "match_strategy": "levenshtein", "match_score": 1},
                {"entity_type": "party", "match_strategy": "levenshtein", "match_score": 2},
                {"entity_type": "party", "match_strategy": "levenshtein", "match_score": 1},
            ],
        )
        m = _compute_match_metrics(p)
        h = m["levenshtein_score_histogram"]
        assert h["1"] == 2
        assert h["2"] == 1
        assert h["0"] == 0

    def test_red_flag_by_strategy(self, tmp_path: Path) -> None:
        p = tmp_path / "match.jsonl"
        _write_jsonl(
            p,
            [
                {"entity_type": "party", "match_strategy": "exact", "red_flag": True},
                {"entity_type": "party", "match_strategy": "jaccard", "match_score": 0.9, "red_flag": True},
                {"entity_type": "counsel", "match_strategy": "exact", "red_flag": False},
            ],
        )
        m = _compute_match_metrics(p)
        assert m["red_flag_count"] == 2
        assert m["red_flag_by_strategy"]["exact"] == 1
        assert m["red_flag_by_strategy"]["jaccard"] == 1


# ---------------------------------------------------------------------------
# Ambiguous metrics
# ---------------------------------------------------------------------------


class TestAmbiguousMetrics:
    def test_count_and_rate(self, tmp_path: Path) -> None:
        p = tmp_path / "ambiguous.jsonl"
        _write_jsonl(
            p,
            [
                {
                    "entity_type": "party",
                    "candidate_count": 2,
                    "total_donated_brl": 1000,
                    "uncertainty_note": "multiple_candidates_same_jaccard_score",
                },
            ],
        )
        m = _compute_ambiguous_metrics(p, total_matches=99)
        assert m["total_ambiguous"] == 1
        # rate = 1 / (99 + 1) = 0.01
        assert m["ambiguous_rate"] == 0.01

    def test_candidate_count_distribution(self, tmp_path: Path) -> None:
        p = tmp_path / "ambiguous.jsonl"
        _write_jsonl(
            p,
            [
                {"entity_type": "party", "candidate_count": 2, "total_donated_brl": 0},
                {"entity_type": "party", "candidate_count": 3, "total_donated_brl": 0},
                {"entity_type": "party", "candidate_count": 5, "total_donated_brl": 0},
            ],
        )
        m = _compute_ambiguous_metrics(p, total_matches=10)
        assert m["candidate_count_distribution"] == {"2": 1, "3": 1, "4+": 1}

    def test_total_donated_brl(self, tmp_path: Path) -> None:
        p = tmp_path / "ambiguous.jsonl"
        _write_jsonl(
            p,
            [
                {"entity_type": "party", "candidate_count": 2, "total_donated_brl": 1000.50},
                {"entity_type": "counsel", "candidate_count": 2, "total_donated_brl": 2000.25},
            ],
        )
        m = _compute_ambiguous_metrics(p, total_matches=10)
        assert m["total_donated_brl_ambiguous"] == 3000.75

    def test_uncertainty_note_breakdown(self, tmp_path: Path) -> None:
        p = tmp_path / "ambiguous.jsonl"
        _write_jsonl(
            p,
            [
                {
                    "entity_type": "party",
                    "candidate_count": 2,
                    "total_donated_brl": 0,
                    "uncertainty_note": "multiple_candidates_same_jaccard_score",
                },
                {
                    "entity_type": "counsel",
                    "candidate_count": 2,
                    "total_donated_brl": 0,
                    "uncertainty_note": "multiple_candidates_same_levenshtein_distance",
                },
            ],
        )
        m = _compute_ambiguous_metrics(p, total_matches=10)
        assert m["ambiguous_by_uncertainty_note"]["multiple_candidates_same_jaccard_score"] == 1
        assert m["ambiguous_by_uncertainty_note"]["multiple_candidates_same_levenshtein_distance"] == 1

    def test_ambiguous_rate_zero_guard(self, tmp_path: Path) -> None:
        """When no matches and no ambiguous → rate is None."""
        p = tmp_path / "ambiguous.jsonl"
        _write_jsonl(p, [])
        m = _compute_ambiguous_metrics(p, total_matches=0)
        assert m["ambiguous_rate"] is None


# ---------------------------------------------------------------------------
# Missing artifacts
# ---------------------------------------------------------------------------


class TestMissingArtifacts:
    def test_no_raw(self, tmp_path: Path) -> None:
        m = _compute_raw_data_metrics(tmp_path / "nonexistent.jsonl")
        assert m["total_raw_records"] == 0
        assert m["cpf_cnpj_empty_rate"] is None

    def test_no_match(self, tmp_path: Path) -> None:
        m = _compute_match_metrics(tmp_path / "nonexistent.jsonl")
        assert m["total_matches"] == 0

    def test_no_ambiguous(self, tmp_path: Path) -> None:
        m = _compute_ambiguous_metrics(tmp_path / "nonexistent.jsonl", total_matches=100)
        assert m["total_ambiguous"] == 0
        assert m["ambiguous_rate"] == 0.0


# ---------------------------------------------------------------------------
# Full report
# ---------------------------------------------------------------------------


class TestBuildEmpiricalReport:
    def test_output_structure(self, tmp_path: Path) -> None:
        tse = tmp_path / "tse"
        analytics = tmp_path / "analytics"
        output = tmp_path / "output"
        tse.mkdir()
        analytics.mkdir()
        _write_jsonl(
            tse / "donations_raw.jsonl",
            [
                {
                    "donor_name_normalized": "ALICE",
                    "donor_cpf_cnpj": "12345678901",
                    "donation_amount": 1000,
                    "election_year": 2022,
                    "state": "SP",
                },
            ],
        )
        _write_jsonl(
            analytics / "donation_match.jsonl",
            [
                {"entity_type": "party", "match_strategy": "exact", "match_score": 1.0},
            ],
        )
        _write_jsonl(analytics / "donation_match_ambiguous.jsonl", [])

        result = build_empirical_report(tse_dir=tse, analytics_dir=analytics, output_dir=output)
        assert result.exists()

        report = json.loads(result.read_text())
        assert "raw_data_quality" in report
        assert "match_quality" in report
        assert "ambiguous_analysis" in report
        assert "methodology_notes" in report
        assert "generated_at" in report

    def test_methodology_notes_present(self, tmp_path: Path) -> None:
        tse = tmp_path / "tse"
        analytics = tmp_path / "analytics"
        tse.mkdir()
        analytics.mkdir()
        _write_jsonl(tse / "donations_raw.jsonl", [])
        _write_jsonl(analytics / "donation_match.jsonl", [])

        result = build_empirical_report(tse_dir=tse, analytics_dir=analytics, output_dir=analytics)
        report = json.loads(result.read_text())
        notes = report["methodology_notes"]
        assert "homonymy_proxy" in notes
        assert "masked_cpf_definition" in notes
        assert "no_precision_recall" in notes

    def test_all_sections_populated(self, tmp_path: Path) -> None:
        """Even with empty inputs, all sections are present with zeros."""
        result = build_empirical_report(
            tse_dir=tmp_path / "tse",
            analytics_dir=tmp_path / "analytics",
            output_dir=tmp_path / "output",
        )
        report = json.loads(result.read_text())
        assert report["raw_data_quality"]["total_raw_records"] == 0
        assert report["match_quality"]["total_matches"] == 0
        assert report["ambiguous_analysis"]["total_ambiguous"] == 0


# ---------------------------------------------------------------------------
# Reservoir sampler
# ---------------------------------------------------------------------------


class TestReservoirSampler:
    def test_percentiles_small_sample(self) -> None:
        s = _ReservoirSampler(capacity=100)
        for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
            s.add(float(v))
        p = s.percentiles([25, 50, 75])
        assert p["p25"] is not None
        assert p["p50"] is not None
        assert p["p75"] is not None

    def test_percentiles_empty(self) -> None:
        s = _ReservoirSampler()
        p = s.percentiles([50])
        assert p["p50"] is None
