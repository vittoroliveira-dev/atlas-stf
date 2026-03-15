"""Tests for core business rules."""

import pytest

import atlas_stf.core.rules as rules_module
from atlas_stf.core.rules import (
    GroupKey,
    _build_exact_outcome_lookup,
    classify_group_size,
    classify_judging_body_category,
    classify_outcome_for_party,
    classify_outcome_materiality,
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


class TestClassifyOutcomeMateriality:
    # --- Provisional ---
    def test_materiality_liminar_deferida(self) -> None:
        assert classify_outcome_materiality("Liminar deferida") == "provisional"

    def test_materiality_liminar_indeferida(self) -> None:
        assert classify_outcome_materiality("Liminar indeferida") == "provisional"

    def test_materiality_liminar_ad_referendum(self) -> None:
        assert classify_outcome_materiality("Liminar deferida ad referendum") == "provisional"

    def test_materiality_liminar_prejudicada(self) -> None:
        assert classify_outcome_materiality("Liminar prejudicada") == "provisional"

    # --- Procedural ---
    def test_materiality_nao_conhecido(self) -> None:
        assert classify_outcome_materiality("Não conhecido") == "procedural"

    def test_materiality_negado_seguimento(self) -> None:
        assert classify_outcome_materiality("Negado seguimento") == "procedural"

    def test_materiality_homologada_desistencia(self) -> None:
        assert classify_outcome_materiality("Homologada a desistência") == "procedural"

    def test_materiality_homologado_acordo(self) -> None:
        assert classify_outcome_materiality("Homologado o acordo") == "procedural"

    def test_materiality_prejudicado(self) -> None:
        assert classify_outcome_materiality("Prejudicado") == "procedural"

    def test_materiality_extinto_processo(self) -> None:
        assert classify_outcome_materiality("Extinto o processo") == "procedural"

    # --- Substantive ---
    def test_materiality_provido(self) -> None:
        assert classify_outcome_materiality("Provido") == "substantive"

    def test_materiality_desprovido(self) -> None:
        assert classify_outcome_materiality("Desprovido") == "substantive"

    def test_materiality_nao_provido(self) -> None:
        assert classify_outcome_materiality("Não provido") == "substantive"

    def test_materiality_procedente(self) -> None:
        assert classify_outcome_materiality("Procedente") == "substantive"

    def test_materiality_improcedente(self) -> None:
        assert classify_outcome_materiality("Improcedente") == "substantive"

    def test_materiality_concedida_ordem(self) -> None:
        assert classify_outcome_materiality("Concedida a ordem") == "substantive"

    def test_materiality_denegada_ordem(self) -> None:
        assert classify_outcome_materiality("Denegada a ordem") == "substantive"

    def test_materiality_concedida_seguranca(self) -> None:
        assert classify_outcome_materiality("Concedida a segurança") == "substantive"

    def test_materiality_conhecido_e_provido(self) -> None:
        assert classify_outcome_materiality("Conhecido e provido") == "substantive"

    # --- Unknown (conservatively classified) ---
    def test_materiality_embargos_rejeitados_is_unknown(self) -> None:
        assert classify_outcome_materiality("Embargos rejeitados") == "unknown"

    def test_materiality_embargos_recebidos_is_unknown(self) -> None:
        assert classify_outcome_materiality("Embargos recebidos") == "unknown"

    def test_materiality_deferido_isolado_is_unknown(self) -> None:
        assert classify_outcome_materiality("Deferido") == "unknown"

    def test_materiality_indeferido_isolado_is_unknown(self) -> None:
        assert classify_outcome_materiality("Indeferido") == "unknown"

    def test_materiality_decisao_referendada_is_unknown(self) -> None:
        assert classify_outcome_materiality("Decisão Referendada") == "unknown"

    def test_materiality_decisao_ratificada_is_unknown(self) -> None:
        assert classify_outcome_materiality("Decisão Ratificada") == "unknown"

    def test_materiality_concedido_isolado_is_unknown(self) -> None:
        assert classify_outcome_materiality("Concedido") == "unknown"

    # --- Lexical collision tests (precedence order) ---
    def test_materiality_liminar_deferida_not_substantive(self) -> None:
        """Contains 'deferida' but is provisional (provisional > substantive)."""
        assert classify_outcome_materiality("Liminar deferida") == "provisional"

    def test_materiality_liminar_indeferida_not_procedural(self) -> None:
        """Contains 'indeferida' but is provisional (provisional > procedural)."""
        assert classify_outcome_materiality("Liminar indeferida") == "provisional"

    def test_materiality_liminar_prejudicada_is_provisional(self) -> None:
        """Contains 'prejudicado' but 'liminar' captures first."""
        assert classify_outcome_materiality("Liminar prejudicada") == "provisional"

    def test_materiality_agravo_regimental_provido(self) -> None:
        """Recursal vehicle, 'provido' = substantive content."""
        assert classify_outcome_materiality("Agravo regimental provido") == "substantive"

    def test_materiality_agravo_regimental_nao_provido(self) -> None:
        assert classify_outcome_materiality("Agravo regimental não provido") == "substantive"

    def test_materiality_composite_pleno_negado_provimento(self) -> None:
        assert classify_outcome_materiality("JULGAMENTO DO PLENO - NEGADO PROVIMENTO") == "substantive"
