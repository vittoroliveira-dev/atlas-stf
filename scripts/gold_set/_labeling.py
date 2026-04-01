"""Heuristic labeling functions for each stratum.

Each function returns (label, justification, decision_basis).
These are SUGGESTIONS — the final label comes from adjudication.
"""

from __future__ import annotations

from ._analysis import first_token, is_pj, levenshtein, name_containment, tokens_are_reorder

type LabelResult = tuple[str, str, str]


def _entity_name(rec: dict) -> str:
    return rec.get("entity_name_normalized") or rec.get("party_name_normalized") or rec.get("stf_entity_name") or ""


# ---------------------------------------------------------------------------
# Donation (party) strata
# ---------------------------------------------------------------------------


def label_exact_with_cpf(rec: dict) -> LabelResult:
    return ("correct", "Exact name match with government-issued CPF present.", "name_identity_tax_id_present")


def label_exact_name_fallback(rec: dict) -> LabelResult:
    name = rec.get("donor_name_normalized", "")
    if is_pj(name):
        return (
            "correct",
            f"Exact match for corporate entity '{name}'. Legally unique name.",
            "distinctive_corporate_name",
        )
    return ("ambiguous", f"Exact match for '{name}' without CPF. Homonym risk.", "common_name_no_document")


def label_canonical_with_cpf(rec: dict) -> LabelResult:
    donor = rec.get("donor_name_normalized", "")
    entity = _entity_name(rec)
    return ("correct", f"Accent-normalized match: '{donor}' ~ '{entity}'. CPF confirms.", "accent_normalized_tax_id")


def label_canonical_no_cpf(rec: dict) -> LabelResult:
    return (
        "ambiguous",
        "Accent-normalized match without CPF. Cannot confirm identity.",
        "accent_normalized_no_document",
    )


def label_jaccard_high(rec: dict) -> LabelResult:
    donor = rec.get("donor_name_normalized", "")
    entity = _entity_name(rec)
    score = rec.get("match_score", 0)
    has_cpf = bool(rec.get("donor_cpf_cnpj"))
    detail = ""
    if name_containment(donor, entity):
        detail = "Name containment pattern. "
    elif tokens_are_reorder(donor, entity):
        detail = "Token reorder. "
    cpf_note = "CPF present. " if has_cpf else "No CPF. "
    return (
        "correct" if has_cpf else "ambiguous",
        f"Jaccard={score:.2f}: '{donor}' ~ '{entity}'. {detail}{cpf_note}",
        "high_jaccard_similarity",
    )


def label_jaccard_borderline(rec: dict) -> LabelResult:
    donor = rec.get("donor_name_normalized", "")
    entity = _entity_name(rec)
    has_cpf = bool(rec.get("donor_cpf_cnpj"))
    if tokens_are_reorder(donor, entity) and has_cpf:
        return ("correct", f"Token reorder with CPF: '{donor}' ~ '{entity}'.", "token_reorder_cpf")
    if name_containment(donor, entity) and has_cpf:
        return ("correct", f"Name containment with CPF: '{donor}' ~ '{entity}'.", "name_containment_cpf")
    return (
        "ambiguous",
        f"Borderline Jaccard: '{donor}' ~ '{entity}'. "
        f"{'CPF present but ' if has_cpf else ''}cannot determine algorithmically.",
        "borderline_similarity",
    )


def label_levenshtein_dist1(rec: dict) -> LabelResult:
    donor = rec.get("donor_name_normalized", "")
    entity = _entity_name(rec)
    has_cpf = bool(rec.get("donor_cpf_cnpj"))
    return (
        "correct" if has_cpf else "ambiguous",
        f"Levenshtein dist=1: '{donor}' ~ '{entity}'. {'CPF present.' if has_cpf else 'No CPF.'}",
        "single_edit_distance",
    )


def label_levenshtein_dist2(rec: dict) -> LabelResult:
    donor = rec.get("donor_name_normalized", "")
    entity = _entity_name(rec)
    ft_donor = first_token(donor)
    ft_entity = first_token(entity)
    ft_dist = levenshtein(ft_donor, ft_entity)

    if ft_dist == 0:
        return ("ambiguous", f"Lev dist=2: '{donor}' ~ '{entity}'. Same first name, surname edit.", "surname_edit")
    if ft_dist >= 3:
        return (
            "incorrect",
            f"Lev dist=2: '{donor}' ~ '{entity}'. First names '{ft_donor}'/'{ft_entity}' (dist={ft_dist}).",
            "different_first_names",
        )
    return (
        "ambiguous",
        f"Lev dist=2: '{donor}' ~ '{entity}'. First names '{ft_donor}'/'{ft_entity}' (dist={ft_dist}).",
        "borderline_first_name",
    )


def label_ambiguous_multi(rec: dict) -> LabelResult:
    count = rec.get("candidate_count", 0)
    sample = rec.get("sample_candidate_name", "?")
    return ("insufficient", f"{count} candidates. Sample: '{sample}'. Unresolvable.", "multiple_candidates")


# ---------------------------------------------------------------------------
# Counsel
# ---------------------------------------------------------------------------


def label_counsel_match(rec: dict) -> LabelResult:
    strategy = rec.get("match_strategy", "")
    donor = rec.get("donor_name_normalized", "")
    entity = _entity_name(rec)
    has_cpf = bool(rec.get("donor_cpf_cnpj"))
    return (
        "correct" if strategy == "exact" and has_cpf else "ambiguous",
        f"Counsel {strategy}: '{donor}' ~ '{entity}'. {'CPF present.' if has_cpf else 'No CPF.'}",
        f"counsel_{strategy}",
    )


# ---------------------------------------------------------------------------
# Sanction
# ---------------------------------------------------------------------------


def label_sanction_match(rec: dict) -> LabelResult:
    strategy = rec.get("match_strategy", "")
    entity = _entity_name(rec)
    has_tax = bool(rec.get("matched_tax_id"))
    source = rec.get("sanction_source", "")
    if has_tax:
        return ("correct", f"Sanction {strategy} with tax ID for '{entity}' ({source}).", "sanction_tax_id")
    if is_pj(entity):
        return ("correct", f"Sanction {strategy} for corporate '{entity}' ({source}).", "sanction_corporate")
    return ("ambiguous", f"Sanction {strategy} for '{entity}' ({source}) without tax ID.", "sanction_no_document")


# ---------------------------------------------------------------------------
# SCL degree-2
# ---------------------------------------------------------------------------


def label_scl_degree2(rec: dict) -> LabelResult:
    sanction_name = rec.get("sanction_entity_name", "")
    bridge = rec.get("bridge_company_name", "")
    stf_name = rec.get("stf_entity_name", "")
    strategy = rec.get("stf_match_strategy", "")
    return (
        "ambiguous",
        f"SCL degree-2: sanction '{sanction_name}' via '{bridge}' to '{stf_name}' ({strategy}).",
        "scl_indirect_link",
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

LABEL_FNS: dict[str, object] = {
    "exact_with_cpf": label_exact_with_cpf,
    "exact_name_fallback": label_exact_name_fallback,
    "canonical_with_cpf": label_canonical_with_cpf,
    "canonical_no_cpf": label_canonical_no_cpf,
    "jaccard_high": label_jaccard_high,
    "jaccard_borderline": label_jaccard_borderline,
    "levenshtein_dist1": label_levenshtein_dist1,
    "levenshtein_dist2": label_levenshtein_dist2,
    "ambiguous_multi": label_ambiguous_multi,
    "counsel_match": label_counsel_match,
    "sanction_match": label_sanction_match,
    "scl_degree2": label_scl_degree2,
}
