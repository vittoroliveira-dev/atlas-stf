from __future__ import annotations

from atlas_stf.analytics.score import (
    ScoreComponent,
    _apply_procedural_suppressions,
    _build_odds_ratio_component,
    score_event_against_baseline,
)


def test_score_event_against_baseline_returns_atipicidade_for_rare_event():
    baseline = {
        "event_count": 12,
        "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 12},
        "expected_rapporteur_distribution": {"MIN X": 12},
        "expected_judging_body_distribution": {"TURMA": 12},
    }
    event = {
        "decision_progress": "DEFERIU PEDIDO",
        "current_rapporteur": "MIN Y",
        "judging_body": "PLENARIO",
    }

    result = score_event_against_baseline(event, baseline)

    assert result.alert_type == "atipicidade"
    assert result.alert_score is not None
    assert result.alert_score > 0.9
    assert result.uncertainty_note is None
    assert result.evidence_summary is not None


def test_score_event_against_baseline_marks_small_baseline_as_inconclusive():
    baseline = {
        "event_count": 5,
        "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 5},
        "expected_rapporteur_distribution": {},
        "expected_judging_body_distribution": {},
    }
    event = {
        "decision_progress": "DEFERIU PEDIDO",
        "current_rapporteur": None,
        "judging_body": None,
    }

    result = score_event_against_baseline(event, baseline)

    assert result.alert_type == "inconclusivo"
    assert result.alert_score == 1.0
    assert result.uncertainty_note is not None


def test_score_includes_process_class_outcome_dimension():
    baseline = {
        "event_count": 100,
        "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 80, "DEFERIU PEDIDO": 20},
        "expected_rapporteur_distribution": {"MIN X": 100},
        "expected_judging_body_distribution": {"TURMA": 100},
    }
    event = {
        "decision_progress": "DEFERIU PEDIDO",
        "current_rapporteur": "MIN X",
        "judging_body": "TURMA",
        "process_class": "AC",
    }

    result = score_event_against_baseline(event, baseline)

    component_names = [c.name for c in result.components]
    assert "process_class_outcome" in component_names
    assert len(result.components) == 4


def test_build_odds_ratio_component_rare_event():
    component = _build_odds_ratio_component(
        "AC",
        "DEFERIU PEDIDO",
        {"NEGOU PROVIMENTO": 95, "DEFERIU PEDIDO": 5},
        100,
    )
    assert component is not None
    assert component.name == "process_class_outcome"
    assert component.rarity_score == 0.95


def test_build_odds_ratio_component_common_event():
    component = _build_odds_ratio_component(
        "AC",
        "NEGOU PROVIMENTO",
        {"NEGOU PROVIMENTO": 90, "DEFERIU PEDIDO": 10},
        100,
    )
    assert component is not None
    assert component.rarity_score == 0.1


def test_build_odds_ratio_component_returns_none_for_missing_data():
    assert _build_odds_ratio_component(None, "X", {"X": 10}, 10) is None
    assert _build_odds_ratio_component("AC", None, {"X": 10}, 10) is None
    assert _build_odds_ratio_component("AC", "X", {}, 0) is None


def test_score_uses_class_stratified_distribution():
    """Bug 1 fix: process_class_outcome should use class-specific distribution."""
    baseline = {
        "event_count": 200,
        "expected_decision_progress_distribution": {
            "NEGOU PROVIMENTO": 120,
            "DEFERIU PEDIDO": 80,
        },
        "expected_rapporteur_distribution": {"MIN X": 200},
        "expected_judging_body_distribution": {"TURMA": 200},
        "expected_progress_by_class": {
            "AC": {"NEGOU PROVIMENTO": 95, "DEFERIU PEDIDO": 5},
            "RE": {"NEGOU PROVIMENTO": 25, "DEFERIU PEDIDO": 75},
        },
    }
    event = {
        "decision_progress": "DEFERIU PEDIDO",
        "current_rapporteur": "MIN X",
        "judging_body": "TURMA",
        "process_class": "AC",
    }

    result = score_event_against_baseline(event, baseline)
    pco = [c for c in result.components if c.name == "process_class_outcome"]
    assert len(pco) == 1
    # AC class has 5/100 = 5% for DEFERIU PEDIDO → rarity 0.95
    assert pco[0].expected_probability == 0.05
    assert pco[0].rarity_score == 0.95


def test_score_falls_back_to_global_when_class_data_insufficient():
    """Bug 1 fix: fallback to global distribution when class data is too small."""
    baseline = {
        "event_count": 100,
        "expected_decision_progress_distribution": {
            "NEGOU PROVIMENTO": 80,
            "DEFERIU PEDIDO": 20,
        },
        "expected_rapporteur_distribution": {"MIN X": 100},
        "expected_judging_body_distribution": {"TURMA": 100},
        "expected_progress_by_class": {
            "AC": {"NEGOU PROVIMENTO": 3, "DEFERIU PEDIDO": 1},  # Only 4 events — below threshold
        },
    }
    event = {
        "decision_progress": "DEFERIU PEDIDO",
        "current_rapporteur": "MIN X",
        "judging_body": "TURMA",
        "process_class": "AC",
    }

    result = score_event_against_baseline(event, baseline)
    pco = [c for c in result.components if c.name == "process_class_outcome"]
    assert len(pco) == 1
    # Falls back to global: 20/100 = 0.2
    assert pco[0].expected_probability == 0.2


def test_score_filters_near_uniform_dimensions():
    """High-probability dimensions (prob > 0.4) should not dilute the score."""
    baseline = {
        "event_count": 100,
        "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 95, "DEFERIU PEDIDO": 5},
        "expected_rapporteur_distribution": {"MIN X": 95, "MIN Y": 5},
        # judging_body is near-uniform: observed value has 60% probability
        "expected_judging_body_distribution": {"TURMA": 60, "PLENARIO": 40},
    }
    event = {
        "decision_progress": "DEFERIU PEDIDO",  # prob=0.05 → rarity=0.95
        "current_rapporteur": "MIN Y",  # prob=0.05 → rarity=0.95
        "judging_body": "TURMA",  # prob=0.60 → rarity=0.40, FILTERED
    }

    result = score_event_against_baseline(event, baseline)

    # All 3 components are present for transparency
    assert len(result.components) == 3
    # Score should be based only on the 2 discriminative dimensions
    # (0.95 + 0.95) / 2 = 0.95, not (0.95 + 0.95 + 0.40) / 3 = 0.767
    assert result.alert_score is not None
    assert result.alert_score > 0.9
    assert "1 filtrada" in (result.evidence_summary or "")


def test_score_fallback_when_all_dimensions_filtered():
    """If all dimensions have high probability, use all of them (fallback)."""
    baseline = {
        "event_count": 100,
        "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 60, "DEFERIU PEDIDO": 40},
        "expected_rapporteur_distribution": {"MIN X": 55, "MIN Y": 45},
        "expected_judging_body_distribution": {"TURMA": 70, "PLENARIO": 30},
    }
    event = {
        "decision_progress": "NEGOU PROVIMENTO",  # prob=0.60
        "current_rapporteur": "MIN X",  # prob=0.55
        "judging_body": "TURMA",  # prob=0.70
    }

    result = score_event_against_baseline(event, baseline)

    # All filtered → fallback to all components
    assert len(result.components) == 3
    assert result.alert_score is not None
    # Score = mean of all rarities (all < 0.5)
    assert result.alert_score < 0.5


# --- Procedural suppression tests (Iteration 1) ---


def _make_component(name: str, rarity: float = 0.95) -> ScoreComponent:
    return ScoreComponent(
        name=name,
        observed_value="X",
        expected_value="Y",
        expected_probability=1.0 - rarity,
        rarity_score=rarity,
    )


def test_suppression_plenario_virtual_rg():
    """RE with judging_body='Plenário Virtual - RG' → judging_body suppressed."""
    event = {"process_class": "RE", "judging_body": "Plenário Virtual - RG", "decision_progress": "Negou provimento"}
    components = (_make_component("decision_progress"), _make_component("judging_body"))
    filtered, notes = _apply_procedural_suppressions(event, components)

    assert len(filtered) == 1
    assert filtered[0].name == "decision_progress"
    assert len(notes) == 1
    assert "Plenário Virtual" in notes[0]


def test_no_suppression_genuine_outlier():
    """AC with judging_body='Plenário Virtual' → no suppression (not RE/ARE)."""
    event = {"process_class": "AC", "judging_body": "Plenário Virtual", "decision_progress": "Negou provimento"}
    components = (_make_component("decision_progress"), _make_component("judging_body"))
    filtered, notes = _apply_procedural_suppressions(event, components)

    assert len(filtered) == 2
    assert notes == []


def test_suppression_adi_plenario():
    """ADI with judging_body='Tribunal Pleno' → judging_body suppressed."""
    event = {"process_class": "ADI", "judging_body": "Tribunal Pleno", "decision_progress": "Procedente"}
    components = (_make_component("decision_progress"), _make_component("judging_body"))
    filtered, notes = _apply_procedural_suppressions(event, components)

    assert len(filtered) == 1
    assert filtered[0].name == "decision_progress"
    assert "controle concentrado" in notes[0]


def test_repercussao_geral_full_suppression():
    """RE with decision_progress mentioning 'repercussão geral' → dp + jb suppressed."""
    event = {
        "process_class": "RE",
        "judging_body": "Plenário Virtual - RG",
        "decision_progress": "Reconhecida a repercussão geral",
    }
    components = (
        _make_component("decision_progress"),
        _make_component("judging_body"),
        _make_component("current_rapporteur"),
    )
    filtered, notes = _apply_procedural_suppressions(event, components)

    assert len(filtered) == 1
    assert filtered[0].name == "current_rapporteur"
    assert len(notes) == 2  # two rules triggered


def test_existing_scoring_unchanged():
    """Event with no procedural match → score identical to baseline."""
    baseline = {
        "event_count": 12,
        "expected_decision_progress_distribution": {"NEGOU PROVIMENTO": 12},
        "expected_rapporteur_distribution": {"MIN X": 12},
        "expected_judging_body_distribution": {"TURMA": 12},
    }
    event = {
        "decision_progress": "DEFERIU PEDIDO",
        "current_rapporteur": "MIN Y",
        "judging_body": "PLENARIO",
        "process_class": "AC",
    }
    result = score_event_against_baseline(event, baseline)

    assert result.alert_score is not None
    assert result.alert_score > 0.9
    assert result.alert_type == "atipicidade"
    # No suppression note
    assert result.uncertainty_note is None


def test_suppression_reduces_score_for_procedural_event():
    """RE going to Plenário Virtual should have lower score than without suppression."""
    baseline = {
        "event_count": 100,
        "expected_decision_progress_distribution": {"Agravo não provido": 90, "Reconhecida a repercussão geral": 10},
        "expected_rapporteur_distribution": {"MIN X": 100},
        "expected_judging_body_distribution": {"1ª Turma": 90, "Plenário Virtual - RG": 10},
    }
    event = {
        "decision_progress": "Reconhecida a repercussão geral",
        "current_rapporteur": "MIN X",
        "judging_body": "Plenário Virtual - RG",
        "process_class": "RE",
    }
    result = score_event_against_baseline(event, baseline)

    # After suppression, only current_rapporteur remains (prob=1.0, rarity=0.0)
    # So score should be low (rapporteur matches expectation)
    assert result.alert_score is not None
    assert result.uncertainty_note is not None
    assert "repercussão geral" in result.uncertainty_note.lower()


def test_suppression_adpf_plenario():
    """ADPF with judging_body='Plenário' → judging_body suppressed."""
    event = {"process_class": "ADPF", "judging_body": "Plenário", "decision_progress": "Procedente"}
    components = (_make_component("decision_progress"), _make_component("judging_body"))
    filtered, notes = _apply_procedural_suppressions(event, components)

    assert len(filtered) == 1
    assert "controle concentrado" in notes[0]
