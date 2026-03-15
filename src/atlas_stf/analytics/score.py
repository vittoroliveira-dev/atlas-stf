"""Transparent scoring helpers for outlier alerts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

ALERT_SCORE_THRESHOLD = 0.75
CONCLUSIVE_BASELINE_EVENT_COUNT = 10
_NOISE_PROBABILITY_THRESHOLD = 0.40

# Classes processuais de competência originária do Plenário
_COMPETENCIA_ORIGINARIA_PLENARIO: frozenset[str] = frozenset({"ADI", "ADC", "ADPF", "ADO"})

# Classes processuais de repercussão geral (Plenário Virtual)
_REPERCUSSAO_GERAL_CLASSES: frozenset[str] = frozenset({"RE", "ARE"})

_RE_REPERCUSSAO_GERAL = re.compile(r"repercuss[ãa]o\s+geral", re.IGNORECASE)
_RE_PLENARIO = re.compile(r"plen[áa]rio|tribunal\s+pleno", re.IGNORECASE)
_RE_PLENARIO_VIRTUAL = re.compile(r"plen[áa]rio\s+virtual", re.IGNORECASE)


@dataclass(frozen=True)
class _SuppressionRule:
    name: str
    suppress_dimensions: tuple[str, ...]
    explanation: str


def _apply_procedural_suppressions(
    event: dict[str, Any],
    scoring_components: tuple[ScoreComponent, ...],
) -> tuple[tuple[ScoreComponent, ...], list[str]]:
    """Remove scoring dimensions explained by procedural rules.

    Returns filtered components and a list of suppression explanations.
    """
    process_class = str(event.get("process_class") or "").strip().upper()
    judging_body = str(event.get("judging_body") or "")
    decision_progress = str(event.get("decision_progress") or "")

    suppressed_dims: set[str] = set()
    explanations: list[str] = []

    # Rule 1: RE/ARE in Plenário Virtual → suppress judging_body
    if process_class in _REPERCUSSAO_GERAL_CLASSES and _RE_PLENARIO_VIRTUAL.search(judging_body):
        suppressed_dims.add("judging_body")
        explanations.append("Repercussão geral é encaminhada ao Plenário Virtual por regra regimental")

    # Rule 2: ADI/ADC/ADPF/ADO in Plenário/Tribunal Pleno → suppress judging_body
    if process_class in _COMPETENCIA_ORIGINARIA_PLENARIO and _RE_PLENARIO.search(judging_body):
        suppressed_dims.add("judging_body")
        explanations.append("Ações de controle concentrado são de competência originária do Plenário")

    # Rule 3: decision_progress mentions "repercussão geral"
    if _RE_REPERCUSSAO_GERAL.search(decision_progress):
        suppressed_dims.update(("decision_progress", "judging_body"))
        explanations.append("Decisão sobre repercussão geral segue rito próprio e não é comparável ao grupo geral")

    if not suppressed_dims:
        return scoring_components, []

    filtered = tuple(c for c in scoring_components if c.name not in suppressed_dims)
    return filtered, explanations


@dataclass(frozen=True)
class ScoreComponent:
    name: str
    observed_value: str
    expected_value: str | None
    expected_probability: float
    rarity_score: float


@dataclass(frozen=True)
class ScoreResult:
    alert_score: float | None
    components: tuple[ScoreComponent, ...]
    expected_pattern: str | None
    observed_pattern: str | None
    evidence_summary: str | None
    uncertainty_note: str | None
    alert_type: str | None


def _normalize_distribution(distribution: dict[str, int], total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {str(key): float(value) / float(total) for key, value in distribution.items() if key and int(value) > 0}


def _top_value(distribution: dict[str, float]) -> tuple[str | None, float]:
    if not distribution:
        return None, 0.0
    key, probability = max(distribution.items(), key=lambda item: (item[1], item[0]))
    return key, probability


def _format_probability(value: float) -> str:
    return f"{value * 100:.1f}%"


def _build_component(name: str, observed_value: Any, distribution: dict[str, int], total: int) -> ScoreComponent | None:
    if observed_value is None:
        return None
    observed_text = str(observed_value).strip()
    if not observed_text:
        return None

    normalized = _normalize_distribution(distribution, total)
    if not normalized:
        return None

    expected_value, _ = _top_value(normalized)
    expected_probability = normalized.get(observed_text, 0.0)
    rarity_score = 1.0 - expected_probability
    return ScoreComponent(
        name=name,
        observed_value=observed_text,
        expected_value=expected_value,
        expected_probability=expected_probability,
        rarity_score=rarity_score,
    )


def _build_odds_ratio_component(
    process_class: str | None,
    decision_progress: str | None,
    progress_distribution: dict[str, int],
    total: int,
) -> ScoreComponent | None:
    if not process_class or not decision_progress or total <= 0:
        return None
    progress_text = str(decision_progress).strip()
    class_text = str(process_class).strip()
    if not progress_text or not class_text:
        return None
    observed_count = int(progress_distribution.get(progress_text, 0))
    if observed_count <= 0:
        return ScoreComponent(
            name="process_class_outcome",
            observed_value=f"{class_text}/{progress_text}",
            expected_value=None,
            expected_probability=0.0,
            rarity_score=1.0,
        )
    frequency = observed_count / total
    rarity = 1.0 - frequency
    return ScoreComponent(
        name="process_class_outcome",
        observed_value=f"{class_text}/{progress_text}",
        expected_value=None,
        expected_probability=frequency,
        rarity_score=round(rarity, 6),
    )


def _class_stratified_distribution(
    baseline: dict[str, Any],
    process_class: str | None,
    min_class_events: int = 5,
) -> tuple[dict[str, int], int]:
    """Return progress distribution stratified by process_class when available.

    Falls back to global distribution if class-specific data is unavailable or
    too small (fewer than *min_class_events*).
    """
    global_dist = baseline.get("expected_decision_progress_distribution") or {}
    global_total = int(baseline.get("event_count") or 0)

    if not process_class:
        return global_dist, global_total

    by_class = baseline.get("expected_progress_by_class") or {}
    class_dist = by_class.get(process_class)
    if not class_dist:
        return global_dist, global_total

    class_total = sum(int(v) for v in class_dist.values())
    if class_total < min_class_events:
        return global_dist, global_total

    return class_dist, class_total


def _resolve_loo_baseline(
    event: dict[str, Any],
    baseline: dict[str, Any],
) -> dict[str, Any]:
    """Return LOO baseline for the event's rapporteur if available, else the original."""
    rapporteur = event.get("current_rapporteur")
    if not rapporteur:
        return baseline
    loo = baseline.get("loo_rapporteur_distributions")
    if not loo or not isinstance(loo, dict):
        return baseline
    loo_data = loo.get(str(rapporteur))
    if not loo_data or not isinstance(loo_data, dict):
        return baseline
    loo_event_count = loo_data.get("event_count")
    if not loo_event_count or int(loo_event_count) < 5:
        return baseline
    # Merge LOO distributions into a baseline-shaped dict
    merged = dict(baseline)
    merged["event_count"] = int(loo_event_count)
    merged["expected_decision_progress_distribution"] = (
        loo_data.get("expected_decision_progress_distribution")
        or baseline.get("expected_decision_progress_distribution")
        or {}
    )
    merged["expected_rapporteur_distribution"] = (
        loo_data.get("expected_rapporteur_distribution") or baseline.get("expected_rapporteur_distribution") or {}
    )
    merged["expected_judging_body_distribution"] = (
        loo_data.get("expected_judging_body_distribution") or baseline.get("expected_judging_body_distribution") or {}
    )
    return merged


def score_event_against_baseline(event: dict[str, Any], baseline: dict[str, Any]) -> ScoreResult:
    # Use leave-one-out baseline when available (decontaminated)
    effective_baseline = _resolve_loo_baseline(event, baseline)
    total = int(effective_baseline.get("event_count") or 0)
    process_class = event.get("process_class")
    class_dist, class_total = _class_stratified_distribution(effective_baseline, process_class)
    components = tuple(
        component
        for component in (
            _build_component(
                "decision_progress",
                event.get("decision_progress"),
                effective_baseline.get("expected_decision_progress_distribution") or {},
                total,
            ),
            _build_component(
                "current_rapporteur",
                event.get("current_rapporteur"),
                effective_baseline.get("expected_rapporteur_distribution") or {},
                total,
            ),
            _build_component(
                "judging_body",
                event.get("judging_body"),
                effective_baseline.get("expected_judging_body_distribution") or {},
                total,
            ),
            _build_odds_ratio_component(
                process_class,
                event.get("decision_progress"),
                class_dist,
                class_total,
            ),
        )
        if component is not None
    )

    if not components:
        return ScoreResult(
            alert_score=None,
            components=(),
            expected_pattern=None,
            observed_pattern=None,
            evidence_summary=None,
            uncertainty_note="Sem dimensões observáveis suficientes para score reprodutível.",
            alert_type=None,
        )

    # Filter out near-uniform dimensions (high probability = low signal)
    # to avoid diluting the score with noise (e.g. judging_body).
    # All components are kept for transparency; only scoring is filtered.
    scoring_components = tuple(c for c in components if c.expected_probability < _NOISE_PROBABILITY_THRESHOLD)
    if not scoring_components:
        scoring_components = components  # fallback: use all if everything is filtered

    # Apply procedural suppression rules (e.g. RE→Plenário Virtual is expected)
    scoring_components, suppression_notes = _apply_procedural_suppressions(event, scoring_components)
    if not scoring_components:
        scoring_components = components  # fallback if all suppressed

    alert_score = sum(c.rarity_score for c in scoring_components) / len(scoring_components)
    expected_parts = []
    observed_parts = []
    evidence_parts = []
    for component in components:
        expected_label = component.expected_value or "INCERTO"
        expected_parts.append(f"{component.name} tende a '{expected_label}'")
        observed_parts.append(f"{component.name}='{component.observed_value}'")
        evidence_parts.append(
            (
                f"{component.name}='{component.observed_value}' teve frequência histórica "
                f"{_format_probability(component.expected_probability)}"
            )
        )

    n_filtered_out = len(components) - len(scoring_components)
    uncertainty_reasons: list[str] = []
    if total < CONCLUSIVE_BASELINE_EVENT_COUNT:
        uncertainty_reasons.append(f"baseline pequeno ({total} eventos no grupo)")
    if len(scoring_components) < 2:
        uncertainty_reasons.append(f"score calculado com apenas {len(scoring_components)} dimensão discriminativa")
    if suppression_notes:
        uncertainty_reasons.extend(suppression_notes)

    uncertainty_note = "; ".join(uncertainty_reasons) if uncertainty_reasons else None
    alert_type = "inconclusivo" if uncertainty_note else "atipicidade"

    return ScoreResult(
        alert_score=round(alert_score, 6),
        components=components,
        expected_pattern="; ".join(expected_parts),
        observed_pattern="; ".join(observed_parts),
        evidence_summary=(
            f"Score médio de raridade {alert_score:.3f} em {len(scoring_components)} dimensões"
            + (f" ({n_filtered_out} filtrada por baixa discriminação)" if n_filtered_out else "")
            + ": "
            + "; ".join(evidence_parts)
        ),
        uncertainty_note=uncertainty_note,
        alert_type=alert_type,
    )
