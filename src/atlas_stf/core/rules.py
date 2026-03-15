"""Deterministic business rules for comparison groups and thematic classification."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

RULE_VERSION = "comparison-group-v3"
MIN_CASE_COUNT = 5
MAX_CASE_COUNT = 5000

# Explicit mapping of decision_progress values to canonical outcomes.
# Values not listed are excluded from sequential/favorable-rate analysis.
# Covers terminology from decisoes.csv, plenario_virtual.csv and decisoes_covid.csv.
FAVORABLE_OUTCOMES: frozenset[str] = frozenset(
    {
        # -- decisoes.csv canonical terms --
        "Provido",
        "Parcialmente provido",
        "Provido em parte",
        "Deferido",
        "Deferido em parte",
        "Parcialmente deferido",
        "Procedente",
        "Procedente em parte",
        "Parcialmente procedente",
        "Julgado procedente",
        "Concedido",
        "Parcialmente concedido",
        "Homologado",
        "Homologada",
        # -- plenario_virtual.csv terms --
        "Agravo regimental provido",
        "Agravo regimental provido em parte",
        "Concedida a ordem",
        "Concedida a ordem de ofício",
        "Concedida em parte a ordem",
        "Concedida a segurança",
        "Concedida em parte a segurança",
        "Concedida a suspensão",
        "Concedida em parte a suspensão",
        "Embargos recebidos",
        "Embargos recebidos em parte",
        "Liminar deferida",
        "Liminar deferida em parte",
        "Liminar referendada",
        "Liminar referendada em parte",
        "Homologado o acordo",
        "Homologada a desistência",
        "Conhecido e provido",
        "Conhecido e provido em parte",
        "Conhecido em parte e nessa parte provido",
        "Embargos recebidos como agravo regimental desde logo provido",
        "Embargos recebidos como agravo regimental desde logo provido em parte",
        "Agravo provido e desde logo provido o RE",
        "Decisão Referendada",
        "Decisão Ratificada",
        # -- decisoes_covid.csv observed terms --
        "Concedida de ofício",
        "Liminar parcialmente deferida",
        "Liminar deferida ad referendum",
    }
)

UNFAVORABLE_OUTCOMES: frozenset[str] = frozenset(
    {
        # -- decisoes.csv canonical terms --
        "Desprovido",
        "Não provido",
        "Indeferido",
        "Improcedente",
        "Julgado improcedente",
        "Não conhecido",
        "Denegado",
        "Negado provimento",
        "Negado seguimento",
        # -- plenario_virtual.csv terms --
        "Agravo regimental não provido",
        "Agravo regimental não conhecido",
        "Agravo não provido",
        "Embargos rejeitados",
        "Embargos não conhecidos",
        "Embargos recebidos como agravo regimental desde logo não provido",
        "Embargos recebidos como agravo regimental desde logo não conhecido",
        "Denegada a ordem",
        "Denegada a segurança",
        "Denegada a suspensão",
        "Liminar não referendada",
        "Liminar indeferida",
        "Não conhecido(s)",
        "Rejeitados",
        "Conhecido e negado provimento",
        "Conhecido em parte e nessa parte negado provimento",
        # -- decisoes_covid.csv observed terms --
        "Liminar indeferida ad referendum",
    }
)

NEUTRAL_OUTCOMES: frozenset[str] = frozenset(
    {
        "Prejudicado",
        "Liminar prejudicada",
        "Extinto o processo",
        "Baixa sem resolução",
        "Baixa sem resolucao",
    }
)

ACTIVE_PARTY_ROLES: frozenset[str] = frozenset(
    {
        "RECTE.(S)",
        "RECLTE.(S)",
        "AGTE.(S)",
        "EMBTE.(S)",
        "IMPTE.(S)",
        "REQTE.(S)",
    }
)

PASSIVE_PARTY_ROLES: frozenset[str] = frozenset(
    {
        "RECDO.(A/S)",
        "AGDO.(A/S)",
        "INTDO.(A/S)",
        "BENEF.(A/S)",
    }
)

_BINARY_OUTCOME_LABELS = frozenset({"favorable", "unfavorable"})
_NORMALIZE_SPACES_RE = re.compile(r"\s+")


def _normalize_outcome_text(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    upper = ascii_only.upper()
    sanitized = re.sub(r"[^A-Z0-9]+", " ", upper)
    return _NORMALIZE_SPACES_RE.sub(" ", sanitized).strip()


def _normalize_role_label(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return _NORMALIZE_SPACES_RE.sub(" ", ascii_only.upper()).strip()


def _build_exact_lookup(values: frozenset[str], outcome: str) -> dict[str, str]:
    return {_normalize_outcome_text(value): outcome for value in values}


def _build_exact_outcome_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    for values, outcome in (
        (FAVORABLE_OUTCOMES, "favorable"),
        (UNFAVORABLE_OUTCOMES, "unfavorable"),
        (NEUTRAL_OUTCOMES, "neutral"),
    ):
        for normalized_value in _build_exact_lookup(values, outcome):
            existing = lookup.get(normalized_value)
            if existing is not None and existing != outcome:
                raise ValueError(
                    f"Outcome collision for normalized exact match {normalized_value!r}: {existing} vs {outcome}"
                )
            lookup[normalized_value] = outcome
    return lookup


_EXACT_OUTCOME_LOOKUP: dict[str, str] = _build_exact_outcome_lookup()

_ORDERED_SUBSTRING_PATTERNS: tuple[tuple[str, str], ...] = (
    ("NEGADO PROVIMENTO", "unfavorable"),
    ("NEGOU PROVIMENTO", "unfavorable"),
    ("NEGADO SEGUIMENTO", "unfavorable"),
    ("NAO PROVIDO", "unfavorable"),
    ("NAO CONHECIDO", "unfavorable"),
    ("NAO REFERENDADA", "unfavorable"),
    ("LIMINAR INDEFERIDA AD REFERENDUM", "unfavorable"),
    ("LIMINAR INDEFERIDA", "unfavorable"),
    ("INDEFERIDA A ORDEM", "unfavorable"),
    ("INDEFERIDO", "unfavorable"),
    ("IMPROCEDENTE", "unfavorable"),
    ("DENEGADA", "unfavorable"),
    ("DENEGADO", "unfavorable"),
    ("REJEITADOS", "unfavorable"),
    ("REJEITADO", "unfavorable"),
    ("PREJUDICADO", "neutral"),
    ("EXTINTO O PROCESSO", "neutral"),
    ("BAIXA SEM RESOLUCAO", "neutral"),
    ("SEM RESOLUCAO", "neutral"),
    ("LIMINAR PARCIALMENTE DEFERIDA", "favorable"),
    ("LIMINAR DEFERIDA AD REFERENDUM", "favorable"),
    ("CONCEDIDA EM PARTE A ORDEM", "favorable"),
    ("CONCEDIDA DE OFICIO", "favorable"),
    ("CONHECER DO AGRAVO E DAR PARCIAL PROVIMENTO AO RE", "favorable"),
    ("CONHECER DO AGRAVO E DAR PROVIMENTO AO RE", "favorable"),
    ("CONHECIDO EM PARTE E NESSA PARTE DA PROVIMENTO", "favorable"),
    ("CONHECIDO EM PARTE E NESSA PARTE PROVIDO", "favorable"),
    ("DAR PARCIAL PROVIMENTO", "favorable"),
    ("DAR PROVIMENTO", "favorable"),
    ("PROVIDO EM PARTE", "favorable"),
    ("DEFERIDA", "favorable"),
    ("DEFERIDO", "favorable"),
    ("PROVIDO", "favorable"),
    ("CONCEDIDA", "favorable"),
    ("CONCEDIDO", "favorable"),
    ("PROCEDENTE", "favorable"),
    ("HOMOLOGADA", "favorable"),
    ("HOMOLOGADO", "favorable"),
    ("HOMOL A DESISTENCIA", "favorable"),
    ("REFERENDADA", "favorable"),
    ("RATIFICADA", "favorable"),
)


def classify_outcome_raw(decision_progress: str) -> str | None:
    """Classify a decision_progress string as canonical outcome or None.

    Handles both simple values ("Provido") and composite STF formats
    ("JULGAMENTO DA PRIMEIRA TURMA - NEGADO PROVIMENTO") by checking
    exact normalized matches first and ordered substring patterns afterward.
    """
    normalized = _normalize_outcome_text(decision_progress)
    if not normalized:
        return None

    exact = _EXACT_OUTCOME_LOOKUP.get(normalized)
    if exact is not None:
        return exact

    for pattern, outcome in _ORDERED_SUBSTRING_PATTERNS:
        if pattern in normalized:
            return outcome

    return None


def classify_outcome_for_party(
    decision_progress: str,
    role_in_case: str | None,
) -> str | None:
    """Classify a decision outcome relative to a specific party's role.

    When the party is passive (respondent), the meaning is inverted:
    "Agravo regimental não provido" is unfavorable in abstract, but when the
    party is the respondent it means the opponent's appeal was denied, which is
    favorable for the party.

    Returns "favorable", "unfavorable", "neutral", or None.
    """
    raw = classify_outcome_raw(decision_progress)
    if raw is None:
        return None

    if not role_in_case:
        return raw

    normalized_role = _normalize_role_label(role_in_case)
    if normalized_role in PASSIVE_PARTY_ROLES and raw in _BINARY_OUTCOME_LABELS:
        return "favorable" if raw == "unfavorable" else "unfavorable"

    return raw


def classify_judging_body_category(
    judging_body: str | None,
    is_collegiate: bool | None,
) -> str:
    """Classify judging body into granular category for group stratification."""
    if judging_body:
        jb_norm = judging_body.upper()
        if "PLENÁRIO VIRTUAL" in jb_norm or "PLENARIO VIRTUAL" in jb_norm:
            return "plenario_virtual"
        if "PLENÁRIO" in jb_norm or "PLENARIO" in jb_norm or "TRIBUNAL PLENO" in jb_norm:
            return "plenario"
        if "TURMA" in jb_norm:
            return "turma"
    if is_collegiate is False:
        return "monocratico"
    if is_collegiate is True:
        return "colegiado_outro"
    return "incerto"


@dataclass(frozen=True)
class GroupKey:
    process_class: str
    thematic_key: str
    decision_type: str
    judging_body_category: str
    decision_year: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "process_class": self.process_class,
            "thematic_key": self.thematic_key,
            "decision_type": self.decision_type,
            "judging_body_category": self.judging_body_category,
            "decision_year": self.decision_year,
        }


def classify_group_size(case_count: int) -> tuple[str, str | None]:
    if case_count < MIN_CASE_COUNT:
        return "insufficient_cases", "below_min_case_count"
    if case_count > MAX_CASE_COUNT:
        return "too_broad", "above_max_case_count"
    return "valid", None


_PROVISIONAL_KEYWORDS: tuple[str, ...] = ("LIMINAR", "AD REFERENDUM")

_PROCEDURAL_PATTERNS: tuple[str, ...] = (
    "NEGADO SEGUIMENTO",
    "NAO CONHECIDO",
    "HOMOLOGADA A DESISTENCIA",
    "HOMOLOGADO O ACORDO",
    "PREJUDICADO",
    "EXTINTO O PROCESSO",
    "BAIXA SEM RESOLUCAO",
    "HOMOL A DESISTENCIA",
)

_SUBSTANTIVE_PATTERNS: tuple[str, ...] = (
    "CONHECIDO E PROVIDO",
    "CONHECIDO E NEGADO PROVIMENTO",
    "CONHECIDO EM PARTE E NESSA PARTE PROVIDO",
    "CONHECIDO EM PARTE E NESSA PARTE NEGADO PROVIMENTO",
    "CONHECIDO EM PARTE E NESSA PARTE DA PROVIMENTO",
    "CONCEDIDA A ORDEM",
    "CONCEDIDA EM PARTE A ORDEM",
    "CONCEDIDA A ORDEM DE OFICIO",
    "DENEGADA A ORDEM",
    "CONCEDIDA A SEGURANCA",
    "CONCEDIDA EM PARTE A SEGURANCA",
    "DENEGADA A SEGURANCA",
    "CONCEDIDA A SUSPENSAO",
    "CONCEDIDA EM PARTE A SUSPENSAO",
    "DENEGADA A SUSPENSAO",
    "PROVIDO EM PARTE",
    "PARCIALMENTE PROVIDO",
    "PROVIDO",
    "NAO PROVIDO",
    "DESPROVIDO",
    "NEGADO PROVIMENTO",
    "NEGOU PROVIMENTO",
    "DAR PROVIMENTO",
    "DAR PARCIAL PROVIMENTO",
    "PROCEDENTE",
    "IMPROCEDENTE",
)


def classify_outcome_materiality(decision_progress: str) -> str:
    """Classify a decision_progress into materiality subcategory.

    Evaluation order: provisional → procedural → substantive → unknown.
    Returns one of: "provisional", "procedural", "substantive", "unknown".
    """
    normalized = _normalize_outcome_text(decision_progress)
    if not normalized:
        return "unknown"

    # 1. Provisional — tutela de urgência / cautelar
    for kw in _PROVISIONAL_KEYWORDS:
        if kw in normalized:
            return "provisional"

    # 2. Procedural — admissibilidade, desistência, extinção sem mérito
    for pat in _PROCEDURAL_PATTERNS:
        if pat in normalized:
            return "procedural"

    # 3. Substantive — mérito efetivo
    for pat in _SUBSTANTIVE_PATTERNS:
        if pat in normalized:
            return "substantive"

    return "unknown"


def derive_thematic_key(
    subjects_normalized: list[str] | None,
    branch_of_law: str | None,
    fallback: str = "INCERTO",
) -> str:
    """Derive thematic key from subjects list, falling back to branch_of_law."""
    if subjects_normalized:
        for item in subjects_normalized:
            candidate = str(item).strip() if item else ""
            if candidate:
                return candidate
    if branch_of_law:
        candidate = str(branch_of_law).strip()
        if candidate:
            return candidate
    return fallback
