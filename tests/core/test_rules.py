"""Tests for core business rules."""

import pytest

import atlas_stf.core.rules as rules_module
from atlas_stf.core.rules import (
    GroupKey,
    _build_exact_outcome_lookup,
    classify_group_size,
    classify_judging_body_category,
    classify_outcome_for_party,
    classify_outcome_raw,
    derive_thematic_key,
)


class TestGroupKey:
    def test_frozen(self):
        key = GroupKey("ADI", "Civil", "DECISÃO", "plenario", 2024)
        with pytest.raises(AttributeError):
            key.process_class = "X"  # type: ignore[misc]

    def test_to_dict(self):
        key = GroupKey("ADI", "Civil", "DECISÃO", "plenario", 2024)
        d = key.to_dict()
        assert d["process_class"] == "ADI"
        assert d["decision_year"] == 2024
        assert d["judging_body_category"] == "plenario"


class TestClassifyGroupSize:
    @pytest.mark.parametrize(
        "count,expected_status",
        [
            (0, "insufficient_cases"),
            (4, "insufficient_cases"),
            (5, "valid"),
            (100, "valid"),
            (5000, "valid"),
            (5001, "too_broad"),
        ],
    )
    def test_classification(self, count, expected_status):
        status, _ = classify_group_size(count)
        assert status == expected_status


class TestDeriveThematicKey:
    @pytest.mark.parametrize(
        "subjects,branch,fallback,expected",
        [
            (["Civil", "Penal"], None, "X", "Civil"),
            (["", "Penal"], None, "X", "Penal"),
            (None, "Trabalhista", "X", "Trabalhista"),
            ([], "Trabalhista", "X", "Trabalhista"),
            (None, None, "INCERTO", "INCERTO"),
            (None, "", "INCERTO", "INCERTO"),
            (["  "], None, "INCERTO", "INCERTO"),
        ],
    )
    def test_derivation(self, subjects, branch, fallback, expected):
        assert derive_thematic_key(subjects, branch, fallback) == expected


class TestClassifyOutcomeRaw:
    @pytest.mark.parametrize(
        "progress,expected",
        [
            ("Provido", "favorable"),
            ("Não provido", "unfavorable"),
            ("Agravo regimental não provido", "unfavorable"),
            ("Concedida a ordem", "favorable"),
            ("Embargos rejeitados", "unfavorable"),
            ("Liminar deferida", "favorable"),
            ("Prejudicado", "neutral"),
            ("Extinto o processo", "neutral"),
            ("Baixa sem resolução", "neutral"),
            # Composite STF format (substring match)
            ("JULGAMENTO DA PRIMEIRA TURMA - NEGADO PROVIMENTO", "unfavorable"),
            ("DECISÃO DO(A) RELATOR(A) - DEFERIDO", "favorable"),
            ("DECISÃO DO(A) RELATOR(A) - PREJUDICADO", "neutral"),
            ("JULGAMENTO POR DESPACHO - PREJUDICADO", "neutral"),
            ("JULGAMENTO POR DESPACHO - NAO PROVIDO", "unfavorable"),
        ],
    )
    def test_classification(self, progress: str, expected: str | None) -> None:
        assert classify_outcome_raw(progress) == expected

    def test_negative_pattern_wins_before_positive_substring(self) -> None:
        assert classify_outcome_raw("NEGADO PROVIMENTO") == "unfavorable"

    def test_exact_lookup_builder_rejects_normalized_collisions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(rules_module, "FAVORABLE_OUTCOMES", frozenset({"PROVIDO"}))
        monkeypatch.setattr(rules_module, "UNFAVORABLE_OUTCOMES", frozenset({"PROVIDO"}))
        monkeypatch.setattr(rules_module, "NEUTRAL_OUTCOMES", frozenset())

        with pytest.raises(ValueError, match="Outcome collision"):
            _build_exact_outcome_lookup()


class TestClassifyOutcomeForParty:
    def test_active_role_no_inversion(self) -> None:
        """When the party is the petitioner, no inversion happens."""
        assert classify_outcome_for_party("Não provido", "RECTE.(S)") == "unfavorable"
        assert classify_outcome_for_party("Provido", "RECTE.(S)") == "favorable"
        assert classify_outcome_for_party("Agravo regimental não provido", "AGTE.(S)") == "unfavorable"

    def test_passive_role_inverts(self) -> None:
        """When the party is the respondent, classification is inverted."""
        # "Appeal denied" when party is respondent → favorable for party
        assert classify_outcome_for_party("Agravo regimental não provido", "RECDO.(A/S)") == "favorable"
        assert classify_outcome_for_party("Embargos rejeitados", "AGDO.(A/S)") == "favorable"
        assert classify_outcome_for_party("Não provido", "INTDO.(A/S)") == "favorable"
        # "Appeal granted" when party is respondent → unfavorable for party
        assert classify_outcome_for_party("Provido", "RECDO.(A/S)") == "unfavorable"
        assert classify_outcome_for_party("Concedida a ordem", "RECDO.(A/S)") == "unfavorable"

    def test_passive_role_is_normalized_before_inversion(self) -> None:
        assert classify_outcome_for_party("Não provido", " recdo.(a/s) ") == "favorable"

    def test_no_role_returns_raw(self) -> None:
        """When role is None or unknown, raw classification is returned."""
        assert classify_outcome_for_party("Não provido", None) == "unfavorable"
        assert classify_outcome_for_party("Provido", None) == "favorable"
        assert classify_outcome_for_party("Provido", "UNKNOWN_ROLE") == "favorable"

    def test_neutral_outcome_is_not_inverted(self) -> None:
        assert classify_outcome_for_party("Prejudicado", "RECTE.(S)") == "neutral"
        assert classify_outcome_for_party("Prejudicado", "RECDO.(A/S)") == "neutral"

    def test_unclassifiable_returns_none(self) -> None:
        """Still-unknown decision_progress values return None."""
        assert classify_outcome_for_party("Decisão do relator", "RECTE.(S)") is None
        assert classify_outcome_for_party("Decisão do relator", "RECDO.(A/S)") is None


class TestClassifyJudgingBodyCategory:
    def test_turma(self) -> None:
        assert classify_judging_body_category("1ª Turma", True) == "turma"
        assert classify_judging_body_category("2ª Turma", True) == "turma"

    def test_plenario(self) -> None:
        assert classify_judging_body_category("Plenário", True) == "plenario"
        assert classify_judging_body_category("Tribunal Pleno", True) == "plenario"

    def test_plenario_virtual(self) -> None:
        assert classify_judging_body_category("Plenário Virtual - RG", True) == "plenario_virtual"
        assert classify_judging_body_category("Plenário Virtual", True) == "plenario_virtual"

    def test_monocratico(self) -> None:
        assert classify_judging_body_category(None, False) == "monocratico"
        assert classify_judging_body_category("", False) == "monocratico"

    def test_colegiado_outro(self) -> None:
        assert classify_judging_body_category(None, True) == "colegiado_outro"

    def test_incerto(self) -> None:
        assert classify_judging_body_category(None, None) == "incerto"
        assert classify_judging_body_category("", None) == "incerto"
