"""Tests for match calibration harness and MatchThresholds."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from atlas_stf.analytics._match_helpers import (
    DEFAULT_MATCH_THRESHOLDS,
    EntityMatchIndex,
    MatchThresholds,
    build_entity_match_index,
    match_entity_record,
)
from atlas_stf.analytics.match_calibration import (
    CALIBRATION_CONFIGS,
    MatchDiagnostic,
    match_entity_record_diagnostic,
    run_match_calibration,
)


def _build_mini_index(
    records: list[dict[str, Any]],
    name_field: str = "party_name_normalized",
) -> EntityMatchIndex:
    """Build a minimal index for testing (no aliases)."""
    return build_entity_match_index(
        records,
        name_field=name_field,
        alias_path=Path("/dev/null"),
    )


# ---------------------------------------------------------------------------
# MatchThresholds
# ---------------------------------------------------------------------------


class TestMatchThresholds:
    def test_defaults(self):
        t = MatchThresholds()
        assert t.jaccard_min == 0.8
        assert t.levenshtein_max == 2
        assert t.length_prefilter_max == 2
        assert t.max_fuzzy_candidates == 10_000

    def test_custom_thresholds(self):
        t = MatchThresholds(jaccard_min=0.75)
        assert t.jaccard_min == 0.75
        assert t.levenshtein_max == 2

    def test_frozen(self):
        t = MatchThresholds()
        with pytest.raises(AttributeError):
            t.jaccard_min = 0.5  # type: ignore[misc]

    def test_default_singleton_matches(self):
        assert DEFAULT_MATCH_THRESHOLDS == MatchThresholds()


# ---------------------------------------------------------------------------
# match_entity_record with thresholds
# ---------------------------------------------------------------------------


class TestMatchEntityRecordWithThresholds:
    def test_levenshtein_at_boundary(self):
        # Multi-word names needed so the token pre-filter finds the candidate.
        # "SILVAA FERREIRA" shares token "FERREIRA" with "SILVA FERREIRA" →
        # candidate found → levenshtein("SILVAA FERREIRA", "SILVA FERREIRA") = 1.
        records = [{"party_name_normalized": "SILVA FERREIRA", "entity_tax_id": None}]
        idx = _build_mini_index(records)
        result_strict = match_entity_record(
            query_name="SILVAA FERREIRA",
            index=idx,
            name_field="party_name_normalized",
            thresholds=MatchThresholds(levenshtein_max=0),
        )
        result_loose = match_entity_record(
            query_name="SILVAA FERREIRA",
            index=idx,
            name_field="party_name_normalized",
            thresholds=MatchThresholds(levenshtein_max=2),
        )
        assert result_strict is None
        assert result_loose is not None
        assert result_loose.strategy == "levenshtein"

    def test_thresholds_none_uses_default(self):
        records = [{"party_name_normalized": "SILVA", "entity_tax_id": None}]
        idx = _build_mini_index(records)
        result_none = match_entity_record(
            query_name="SILVA",
            index=idx,
            name_field="party_name_normalized",
            thresholds=None,
        )
        result_default = match_entity_record(
            query_name="SILVA",
            index=idx,
            name_field="party_name_normalized",
            thresholds=DEFAULT_MATCH_THRESHOLDS,
        )
        assert (result_none is None) == (result_default is None)
        if result_none is not None and result_default is not None:
            assert result_none.strategy == result_default.strategy

    def test_jaccard_threshold_boundary(self):
        # "JOAO CARLOS MARIA" vs "JOAO CARLOS MARIA DA SILVA"
        # Tokens: {JOAO, CARLOS, MARIA} vs {JOAO, CARLOS, MARIA, DA, SILVA}
        # Jaccard = 3 / 5 = 0.6
        records = [{"party_name_normalized": "JOAO CARLOS MARIA DA SILVA", "entity_tax_id": None}]
        idx = _build_mini_index(records)
        result_strict = match_entity_record(
            query_name="JOAO CARLOS MARIA",
            index=idx,
            name_field="party_name_normalized",
            thresholds=MatchThresholds(jaccard_min=0.8),
        )
        result_loose = match_entity_record(
            query_name="JOAO CARLOS MARIA",
            index=idx,
            name_field="party_name_normalized",
            thresholds=MatchThresholds(jaccard_min=0.5),
        )
        assert result_strict is None
        assert result_loose is not None


# ---------------------------------------------------------------------------
# MatchDiagnostic
# ---------------------------------------------------------------------------


class TestMatchDiagnostic:
    def test_all_stages_populated(self):
        records = [
            {"party_name_normalized": "JOAO DA SILVA", "entity_tax_id": "12345678901"},
        ]
        idx = _build_mini_index(records)
        diag = match_entity_record_diagnostic(
            query_name="JOAO DA SILVA",
            query_tax_id="12345678901",
            index=idx,
            name_field="party_name_normalized",
        )
        assert isinstance(diag, MatchDiagnostic)
        assert diag.tax_id_hit is True
        assert diag.winning_strategy == "tax_id"

    def test_deterministic_match_still_has_fuzzy_scores(self):
        records = [
            {"party_name_normalized": "JOAO DA SILVA", "entity_tax_id": "12345678901"},
        ]
        idx = _build_mini_index(records)
        diag = match_entity_record_diagnostic(
            query_name="JOAO DA SILVA",
            query_tax_id="12345678901",
            index=idx,
            name_field="party_name_normalized",
        )
        assert diag.tax_id_hit is True
        # Fuzzy scores computed even though tax_id hit
        assert diag.best_jaccard_score is not None
        assert diag.best_jaccard_score == 1.0

    def test_no_match_returns_none_strategy(self):
        records = [{"party_name_normalized": "COMPLETELY DIFFERENT", "entity_tax_id": None}]
        idx = _build_mini_index(records)
        diag = match_entity_record_diagnostic(
            query_name="ANOTHER NAME ENTIRELY",
            index=idx,
            name_field="party_name_normalized",
        )
        assert diag.winning_strategy is None
        assert diag.is_ambiguous is False


# ---------------------------------------------------------------------------
# run_match_calibration
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


@pytest.fixture()
def calibration_setup(tmp_path: Path) -> dict[str, Path]:
    """Create minimal synthetic data for calibration."""
    tse_dir = tmp_path / "tse"
    tse_dir.mkdir()
    donations = [
        {
            "donor_name_normalized": "JOAO DA SILVA",
            "donor_cpf_cnpj": "12345678901",
            "donation_amount": 1000.0,
            "election_year": 2022,
            "party_abbrev": "PT",
            "candidate_name": "CANDIDATO A",
            "position": "Deputado",
            "donation_date": "2022-08-15",
            "donation_description": "Dinheiro",
        },
        {
            "donor_name_normalized": "MARIA SOUZA",
            "donor_cpf_cnpj": "",
            "donation_amount": 500.0,
            "election_year": 2022,
            "party_abbrev": "PSDB",
            "candidate_name": "CANDIDATO B",
            "position": "Senador",
            "donation_date": "2022-09-01",
            "donation_description": "Estimado",
        },
        {
            "donor_name_normalized": "JOSÉ FERREIRA",
            "donor_cpf_cnpj": "",
            "donation_amount": 200.0,
            "election_year": 2022,
            "party_abbrev": "MDB",
            "candidate_name": "CANDIDATO C",
            "position": "Vereador",
            "donation_date": "2022-07-20",
            "donation_description": "",
        },
    ]
    _write_jsonl(tse_dir / "donations_raw.jsonl", donations)

    parties = [
        {"party_name_normalized": "JOAO DA SILVA", "party_id": "p1", "entity_tax_id": "12345678901"},
        {"party_name_normalized": "MARIA DE SOUZA", "party_id": "p2", "entity_tax_id": None},
    ]
    party_path = tmp_path / "party.jsonl"
    _write_jsonl(party_path, parties)

    counsels = [
        {"counsel_name_normalized": "JOSE FERREIRA", "counsel_id": "c1", "entity_tax_id": None},
    ]
    counsel_path = tmp_path / "counsel.jsonl"
    _write_jsonl(counsel_path, counsels)

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    return {
        "tse_dir": tse_dir,
        "party_path": party_path,
        "counsel_path": counsel_path,
        "output_dir": output_dir,
    }


class TestRunCalibration:
    def test_produces_summary_and_review(self, calibration_setup: dict[str, Path]):
        result = run_match_calibration(
            tse_dir=calibration_setup["tse_dir"],
            party_path=calibration_setup["party_path"],
            counsel_path=calibration_setup["counsel_path"],
            output_dir=calibration_setup["output_dir"],
            alias_path=Path("/dev/null"),
        )
        assert result.exists()
        summary = json.loads(result.read_text())
        assert "entity_types" in summary
        assert "party" in summary["entity_types"]
        assert "counsel" in summary["entity_types"]
        assert summary["total_donors_evaluated"] == 3

        review_path = calibration_setup["output_dir"] / "match_calibration_review.jsonl"
        assert review_path.exists()

    def test_configs_in_summary(self, calibration_setup: dict[str, Path]):
        result = run_match_calibration(
            tse_dir=calibration_setup["tse_dir"],
            party_path=calibration_setup["party_path"],
            counsel_path=calibration_setup["counsel_path"],
            output_dir=calibration_setup["output_dir"],
            alias_path=Path("/dev/null"),
        )
        summary = json.loads(result.read_text())
        party_configs = summary["entity_types"]["party"]["configs"]
        for config_name, _ in CALIBRATION_CONFIGS:
            assert config_name in party_configs

    def test_accent_impact_in_summary(self, calibration_setup: dict[str, Path]):
        result = run_match_calibration(
            tse_dir=calibration_setup["tse_dir"],
            party_path=calibration_setup["party_path"],
            counsel_path=calibration_setup["counsel_path"],
            output_dir=calibration_setup["output_dir"],
            alias_path=Path("/dev/null"),
        )
        summary = json.loads(result.read_text())
        for et in ("party", "counsel"):
            impact = summary["entity_types"][et]["accent_impact"]
            assert "accent_affected_count" in impact
            assert "accent_only_match_gain_count" in impact
            assert "accent_only_ambiguous_gain_count" in impact
            assert "accent_strategy_shift_count" in impact

    def test_missing_donations_file(self, tmp_path: Path):
        tse_dir = tmp_path / "empty_tse"
        tse_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        result = run_match_calibration(
            tse_dir=tse_dir,
            party_path=tmp_path / "nonexistent.jsonl",
            counsel_path=tmp_path / "nonexistent.jsonl",
            output_dir=output_dir,
            alias_path=Path("/dev/null"),
        )
        assert result == output_dir / "match_calibration_summary.json"
        assert result.exists()
        assert result.read_text() == "{}"

    def test_counsel_entity_type_in_summary(self, calibration_setup: dict[str, Path]):
        result = run_match_calibration(
            tse_dir=calibration_setup["tse_dir"],
            party_path=calibration_setup["party_path"],
            counsel_path=calibration_setup["counsel_path"],
            output_dir=calibration_setup["output_dir"],
            alias_path=Path("/dev/null"),
        )
        summary = json.loads(result.read_text())
        counsel = summary["entity_types"]["counsel"]
        assert counsel["index_size"] == 1
        assert "configs" in counsel

    def test_accent_donor_matches_unaccented_counsel(self, calibration_setup: dict[str, Path]):
        """JOSÉ FERREIRA donor should match JOSE FERREIRA counsel via accent normalization."""
        result = run_match_calibration(
            tse_dir=calibration_setup["tse_dir"],
            party_path=calibration_setup["party_path"],
            counsel_path=calibration_setup["counsel_path"],
            output_dir=calibration_setup["output_dir"],
            alias_path=Path("/dev/null"),
        )
        summary = json.loads(result.read_text())
        counsel_default = summary["entity_types"]["counsel"]["configs"]["default"]
        # JOSÉ FERREIRA should match JOSE FERREIRA via levenshtein (distance 0 after normalization)
        assert counsel_default["matched_count"] >= 1

    def test_histograms_present(self, calibration_setup: dict[str, Path]):
        result = run_match_calibration(
            tse_dir=calibration_setup["tse_dir"],
            party_path=calibration_setup["party_path"],
            counsel_path=calibration_setup["counsel_path"],
            output_dir=calibration_setup["output_dir"],
            alias_path=Path("/dev/null"),
        )
        summary = json.loads(result.read_text())
        for et in ("party", "counsel"):
            assert "jaccard_score_histogram" in summary["entity_types"][et]
            assert "levenshtein_distance_histogram" in summary["entity_types"][et]
