"""Constants and stratum definitions for the gold set pipeline."""

from __future__ import annotations

from dataclasses import dataclass

MINIMUM_GOLD_SET_SIZE = 100
SEED = 20260330
REQUIRED_STRATA = frozenset({"counsel_match", "levenshtein_dist1", "scl_degree2"})

VALID_LABELS = frozenset({"correct", "incorrect", "ambiguous", "insufficient"})
ADJUDICATION_TYPES = frozenset(
    {
        "evidence_deterministic",
        "heuristic_provisional",
        "human_review",
        "operator_delegated_curatorial",
    }
)

PJ_INDICATORS = frozenset(
    {
        "S/A",
        "S.A.",
        "S.A",
        "SA",
        "LTDA",
        "ME",
        "EPP",
        "EIRELI",
        "ASSOCIACAO",
        "FUNDACAO",
        "INSTITUTO",
        "SINDICATO",
        "COOPERATIVA",
        "PARTIDO",
        "IGREJA",
        "BANCO",
        "CAIXA",
        "COMPANHIA",
        "CIA",
        "FEDERAL",
        "MUNICIPAL",
        "ESTADUAL",
        "PREFEITURA",
        "CAMARA",
        "MINISTERIO",
        "CONSELHO",
        "COMITE",
    }
)


@dataclass(frozen=True)
class StratumDef:
    name: str
    target: int
    source_type: str  # donation, donation_ambiguous, sanction, scl, counsel
    adjudication_default: str  # evidence_deterministic or heuristic_provisional


ALL_STRATA: list[StratumDef] = [
    # --- Donation (party) ---
    StratumDef("exact_with_cpf", 15, "donation_party", "evidence_deterministic"),
    StratumDef("exact_name_fallback", 15, "donation_party", "evidence_deterministic"),
    StratumDef("canonical_with_cpf", 10, "donation_party", "evidence_deterministic"),
    StratumDef("canonical_no_cpf", 5, "donation_party", "evidence_deterministic"),
    StratumDef("jaccard_high", 10, "donation_party", "heuristic_provisional"),
    StratumDef("jaccard_borderline", 20, "donation_party", "heuristic_provisional"),
    StratumDef("levenshtein_dist1", 20, "donation_party", "heuristic_provisional"),
    StratumDef("levenshtein_dist2", 20, "donation_party", "heuristic_provisional"),
    StratumDef("ambiguous_multi", 15, "donation_ambiguous", "evidence_deterministic"),
    # --- Counsel ---
    StratumDef("counsel_match", 15, "counsel", "heuristic_provisional"),
    # --- Sanction ---
    StratumDef("sanction_match", 15, "sanction", "heuristic_provisional"),
    # --- SCL ---
    StratumDef("scl_degree2", 15, "scl", "heuristic_provisional"),
]
