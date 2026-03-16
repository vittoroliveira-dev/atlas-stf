"""Calibration harness for fuzzy matching thresholds."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import (
    canonicalize_entity_name,
    jaccard_similarity,
    levenshtein_distance,
    normalize_entity_name,
    normalize_tax_id,
    strip_accents,
)
from ._atomic_io import AtomicJsonlWriter
from ._match_helpers import (
    DEFAULT_MATCH_THRESHOLDS,
    EntityMatchIndex,
    MatchThresholds,
    _collect_fuzzy_candidates,
    build_entity_match_index,
    read_jsonl,
)

logger = logging.getLogger(__name__)

CALIBRATION_CONFIGS: tuple[tuple[str, MatchThresholds], ...] = (
    ("default", MatchThresholds()),
    ("jaccard_0.75", MatchThresholds(jaccard_min=0.75)),
    ("jaccard_0.85", MatchThresholds(jaccard_min=0.85)),
    ("jaccard_0.90", MatchThresholds(jaccard_min=0.90)),
    ("levenshtein_1", MatchThresholds(levenshtein_max=1)),
    ("levenshtein_3", MatchThresholds(levenshtein_max=3)),
)

_MAX_REVIEW_PER_REASON = 500


@dataclass(frozen=True)
class MatchDiagnostic:
    """Full diagnostic for a single query against a single index."""

    query_name: str
    query_tax_id: str | None
    # Deterministic stages
    tax_id_hit: bool
    alias_hit: bool
    exact_hit: bool
    canonical_hit: bool
    # Fuzzy scores (always computed)
    best_jaccard_score: float | None
    best_jaccard_candidate: str | None
    jaccard_candidate_count: int
    best_levenshtein_distance: int | None
    best_levenshtein_candidate: str | None
    levenshtein_candidate_count: int
    # Cascade result at default thresholds
    winning_strategy: str | None
    winning_score: float | None
    is_ambiguous: bool
    ambiguity_count: int


def _diag_outcome_kwargs(diag: MatchDiagnostic) -> dict[str, Any]:
    """Extract the fields needed by _derive_outcome from a diagnostic."""
    return {
        "tax_id_hit": diag.tax_id_hit, "alias_hit": diag.alias_hit,
        "exact_hit": diag.exact_hit, "canonical_hit": diag.canonical_hit,
        "best_jaccard_score": diag.best_jaccard_score,
        "jaccard_candidate_count": diag.jaccard_candidate_count,
        "best_levenshtein_distance": diag.best_levenshtein_distance,
        "levenshtein_candidate_count": diag.levenshtein_candidate_count,
    }


def _derive_outcome(
    *,
    tax_id_hit: bool,
    alias_hit: bool,
    exact_hit: bool,
    canonical_hit: bool,
    best_jaccard_score: float | None,
    jaccard_candidate_count: int,
    best_levenshtein_distance: int | None,
    levenshtein_candidate_count: int,
    thresholds: MatchThresholds,
) -> tuple[str | None, float | None, bool, int]:
    """Derive (winning_strategy, winning_score, is_ambiguous, ambiguity_count)."""
    if tax_id_hit:
        return "tax_id", 1.0, False, 0
    if alias_hit:
        return "alias", 1.0, False, 0
    if exact_hit:
        return "exact", 1.0, False, 0
    if canonical_hit:
        return "canonical_name", 1.0, False, 0
    if best_jaccard_score is not None and best_jaccard_score >= thresholds.jaccard_min:
        if jaccard_candidate_count == 1:
            return "jaccard", best_jaccard_score, False, 0
        return "ambiguous", best_jaccard_score, True, jaccard_candidate_count
    if best_levenshtein_distance is not None and best_levenshtein_distance <= thresholds.levenshtein_max:
        if levenshtein_candidate_count == 1:
            return "levenshtein", float(best_levenshtein_distance), False, 0
        return "ambiguous", float(best_levenshtein_distance), True, levenshtein_candidate_count
    return None, None, False, 0


def match_entity_record_diagnostic(
    *,
    query_name: Any,
    query_tax_id: Any = None,
    index: EntityMatchIndex,
    name_field: str,
) -> MatchDiagnostic:
    """Diagnostic variant of match_entity_record — computes ALL stages, no short-circuit."""
    normalized_name = normalize_entity_name(query_name)
    canonical_name = canonicalize_entity_name(query_name)
    tax_id = normalize_tax_id(query_tax_id)
    t = DEFAULT_MATCH_THRESHOLDS

    # Stage 1: tax_id
    tax_id_hit = bool(tax_id and len(index.by_tax_id.get(tax_id, [])) == 1)

    # Stage 2: alias
    alias_hit = False
    if normalized_name:
        alias_matches = index.aliases.get(normalized_name, [])
        if len(alias_matches) == 1:
            alias = alias_matches[0]
            a_tax = normalize_tax_id(alias.get("entity_tax_id"))
            if a_tax and len(index.by_tax_id.get(a_tax, [])) == 1:
                alias_hit = True
            else:
                a_canonical = canonicalize_entity_name(alias.get("canonical_name_normalized"))
                if a_canonical and len(index.by_canonical_name.get(a_canonical, [])) == 1:
                    alias_hit = True

    # Stage 3: exact name
    exact_hit = bool(normalized_name and len(index.by_name.get(normalized_name, [])) == 1)

    # Stage 4: canonical name (accent-sensitive by design — preserves identity key semantics)
    canonical_hit = bool(canonical_name and len(index.by_canonical_name.get(canonical_name, [])) == 1)

    # Stage 5: fuzzy (always computed regardless of deterministic hits)
    best_jaccard_score: float | None = None
    best_jaccard_candidate: str | None = None
    jaccard_above_threshold = 0
    best_lev_distance: int | None = None
    best_lev_candidate: str | None = None
    lev_within_threshold = 0

    if canonical_name:
        candidate_indices = _collect_fuzzy_candidates(canonical_name, index, t.max_fuzzy_candidates)

        # Jaccard (only meaningful for multi-word names)
        if " " in canonical_name:
            scored: list[tuple[float, str]] = []
            for idx in candidate_indices:
                cand = index.canonical_names[idx]
                if not cand:
                    continue
                sc = jaccard_similarity(canonical_name, cand)
                scored.append((sc, cand))
            if scored:
                scored.sort(key=lambda x: x[0], reverse=True)
                best_jaccard_score = scored[0][0]
                best_jaccard_candidate = scored[0][1]
                best_sc = scored[0][0]
                jaccard_above_threshold = sum(1 for sc, _ in scored if sc == best_sc and sc >= t.jaccard_min)

        # Levenshtein
        canonical_len = len(canonical_name)
        lev_scored: list[tuple[int, str]] = []
        for idx in candidate_indices:
            cand = index.canonical_names[idx]
            if not cand:
                continue
            if abs(len(cand) - canonical_len) > t.length_prefilter_max:
                continue
            dist = levenshtein_distance(canonical_name, cand)
            lev_scored.append((dist, cand))
        if lev_scored:
            lev_scored.sort(key=lambda x: x[0])
            best_lev_distance = lev_scored[0][0]
            best_lev_candidate = lev_scored[0][1]
            best_d = lev_scored[0][0]
            lev_within_threshold = sum(1 for d, _ in lev_scored if d == best_d and d <= t.levenshtein_max)

    outcome_kwargs = {
        "tax_id_hit": tax_id_hit, "alias_hit": alias_hit,
        "exact_hit": exact_hit, "canonical_hit": canonical_hit,
        "best_jaccard_score": best_jaccard_score,
        "jaccard_candidate_count": jaccard_above_threshold,
        "best_levenshtein_distance": best_lev_distance,
        "levenshtein_candidate_count": lev_within_threshold,
    }
    winning_strategy, winning_score, is_ambiguous, ambiguity_count = _derive_outcome(
        **outcome_kwargs, thresholds=t,
    )

    return MatchDiagnostic(
        query_name=str(canonical_name or normalized_name or ""),
        query_tax_id=tax_id,
        tax_id_hit=tax_id_hit,
        alias_hit=alias_hit,
        exact_hit=exact_hit,
        canonical_hit=canonical_hit,
        best_jaccard_score=best_jaccard_score,
        best_jaccard_candidate=best_jaccard_candidate,
        jaccard_candidate_count=jaccard_above_threshold,
        best_levenshtein_distance=best_lev_distance,
        best_levenshtein_candidate=best_lev_candidate,
        levenshtein_candidate_count=lev_within_threshold,
        winning_strategy=winning_strategy,
        winning_score=winning_score,
        is_ambiguous=is_ambiguous,
        ambiguity_count=ambiguity_count,
    )



def _raw_levenshtein(left: str, right: str) -> int:
    """Levenshtein distance WITHOUT accent normalization."""
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    prev = list(range(len(right) + 1))
    for i, lc in enumerate(left, start=1):
        curr = [i]
        for j, rc in enumerate(right, start=1):
            cost = 0 if lc == rc else 1
            curr.append(min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]


def _raw_jaccard(left: str, right: str) -> float:
    """Jaccard similarity WITHOUT accent normalization (simple token overlap)."""
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    return intersection / union if union > 0 else 0.0


def _has_accent(text: str | None) -> bool:
    return bool(text and strip_accents(text) != text)


def _compute_accent_impact(
    diagnostics: list[MatchDiagnostic],
    thresholds: MatchThresholds,
) -> dict[str, int]:
    """Compute accent impact counters via contrafactual comparison."""
    accent_affected = 0
    match_gain = 0
    ambiguous_gain = 0
    strategy_shift = 0

    for diag in diagnostics:
        query_canonical = canonicalize_entity_name(diag.query_name)
        if not query_canonical:
            continue
        if not (_has_accent(query_canonical) or _has_accent(diag.best_jaccard_candidate)
                or _has_accent(diag.best_levenshtein_candidate)):
            continue
        accent_affected += 1

        # Contrafactual: compute fuzzy WITHOUT normalization
        raw_jaccard: float | None = None
        raw_lev: int | None = None
        if diag.best_jaccard_candidate:
            raw_jaccard = _raw_jaccard(query_canonical, diag.best_jaccard_candidate)
        if diag.best_levenshtein_candidate:
            raw_lev = _raw_levenshtein(query_canonical, diag.best_levenshtein_candidate)

        # Derive contrafactual winning strategy
        cf_strategy: str | None = None
        if diag.tax_id_hit:
            cf_strategy = "tax_id"
        elif diag.alias_hit:
            cf_strategy = "alias"
        elif diag.exact_hit:
            cf_strategy = "exact"
        elif diag.canonical_hit:
            cf_strategy = "canonical_name"
        elif raw_jaccard is not None and raw_jaccard >= thresholds.jaccard_min:
            cf_strategy = "jaccard"
        elif raw_lev is not None and raw_lev <= thresholds.levenshtein_max:
            cf_strategy = "levenshtein"

        if diag.winning_strategy in {"jaccard", "levenshtein"} and cf_strategy is None:
            match_gain += 1
        if diag.is_ambiguous and cf_strategy != "ambiguous":
            ambiguous_gain += 1
        if diag.winning_strategy and cf_strategy and diag.winning_strategy != cf_strategy:
            if diag.winning_strategy != "ambiguous" and cf_strategy != "ambiguous":
                strategy_shift += 1

    return {
        "accent_affected_count": accent_affected,
        "accent_only_match_gain_count": match_gain,
        "accent_only_ambiguous_gain_count": ambiguous_gain,
        "accent_strategy_shift_count": strategy_shift,
    }


def _build_review_records(
    diagnostics: list[MatchDiagnostic], entity_type: str,
) -> tuple[list[dict[str, Any]], int]:
    """Build review records for borderline/ambiguous/accent cases. Returns (capped, omitted)."""
    raw: list[dict[str, Any]] = []
    for diag in diagnostics:
        configs_match: list[str] = []
        configs_reject: list[str] = []
        kw = _diag_outcome_kwargs(diag)
        for config_name, config_thresholds in CALIBRATION_CONFIGS:
            strategy, _, _, _ = _derive_outcome(**kw, thresholds=config_thresholds)
            if strategy is not None and strategy != "ambiguous":
                configs_match.append(config_name)
            else:
                configs_reject.append(config_name)

        review_reason: str | None = None
        if configs_match and configs_reject:
            review_reason = "borderline_disagreement"
        elif diag.is_ambiguous:
            review_reason = "ambiguous"
        elif _has_accent(canonicalize_entity_name(diag.query_name)) or _has_accent(diag.best_jaccard_candidate):
            review_reason = "accent_affected"

        if review_reason:
            raw.append({
                "entity_type": entity_type,
                "query_name": diag.query_name,
                "query_tax_id": diag.query_tax_id,
                "best_jaccard_score": diag.best_jaccard_score,
                "best_jaccard_candidate": diag.best_jaccard_candidate,
                "best_levenshtein_distance": diag.best_levenshtein_distance,
                "best_levenshtein_candidate": diag.best_levenshtein_candidate,
                "accent_affected": review_reason == "accent_affected",
                "current_winning_strategy": diag.winning_strategy,
                "configs_that_match": configs_match,
                "configs_that_reject": configs_reject,
                "review_reason": review_reason,
            })

    # Cap per review_reason
    by_reason: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in raw:
        by_reason[str(r["review_reason"])].append(r)

    capped: list[dict[str, Any]] = []
    omitted = 0
    for reason, records in by_reason.items():
        if len(records) > _MAX_REVIEW_PER_REASON:
            if reason == "borderline_disagreement":
                records.sort(key=lambda r: r.get("best_jaccard_score") or 0.0, reverse=True)
            else:
                records.sort(key=lambda r: r.get("best_levenshtein_distance") or 999)
            omitted += len(records) - _MAX_REVIEW_PER_REASON
            records = records[:_MAX_REVIEW_PER_REASON]
        capped.extend(records)

    return capped, omitted


def _score_histograms(
    diagnostics: list[MatchDiagnostic],
) -> tuple[dict[str, int], dict[str, int]]:
    """Build jaccard (0.05-width buckets) and levenshtein histograms."""
    jac: dict[str, int] = defaultdict(int)
    lev: dict[str, int] = defaultdict(int)
    for diag in diagnostics:
        if diag.best_jaccard_score is not None:
            lo = int(diag.best_jaccard_score * 20) * 5
            jac[f"{lo / 100:.2f}-{(lo + 5) / 100:.2f}"] += 1
        if diag.best_levenshtein_distance is not None:
            lev[str(diag.best_levenshtein_distance)] += 1
    return dict(sorted(jac.items())), dict(sorted(lev.items()))


def run_match_calibration(
    *,
    tse_dir: Path = Path("data/raw/tse"),
    party_path: Path = Path("data/curated/party.jsonl"),
    counsel_path: Path = Path("data/curated/counsel.jsonl"),
    output_dir: Path = Path("data/analytics"),
    alias_path: Path = Path("data/curated/entity_alias.jsonl"),
) -> Path:
    """Run calibration harness over TSE donations × party + counsel indices."""
    output_dir.mkdir(parents=True, exist_ok=True)

    from .donation_match import _stream_aggregate_donations

    donations_path = tse_dir / "donations_raw.jsonl"
    if not donations_path.exists():
        logger.warning("No donations_raw.jsonl found in %s — cannot calibrate", tse_dir)
        return output_dir

    donor_agg, raw_count, _ = _stream_aggregate_donations(donations_path)
    logger.info("Loaded %d unique donors from %d raw records", len(donor_agg), raw_count)

    party_records = read_jsonl(party_path) if party_path.exists() else []
    party_index = build_entity_match_index(
        party_records, name_field="party_name_normalized",
        alias_path=alias_path, entity_kind="party",
    )
    counsel_records = read_jsonl(counsel_path) if counsel_path.exists() else []
    counsel_index = build_entity_match_index(
        counsel_records, name_field="counsel_name_normalized",
        alias_path=alias_path, entity_kind="counsel",
    )

    indices: dict[str, tuple[EntityMatchIndex, str]] = {
        "party": (party_index, "party_name_normalized"),
        "counsel": (counsel_index, "counsel_name_normalized"),
    }
    entity_results: dict[str, Any] = {}
    all_review: list[dict[str, Any]] = []

    for entity_type, (index, name_field) in indices.items():
        diagnostics = [
            match_entity_record_diagnostic(
                query_name=d["donor_name_normalized"], query_tax_id=d.get("donor_cpf_cnpj"),
                index=index, name_field=name_field,
            )
            for d in donor_agg.values()
        ]
        config_results: dict[str, dict[str, Any]] = {}
        for config_name, config_thresholds in CALIBRATION_CONFIGS:
            matched, ambiguous, fuzzy_accepted = 0, 0, 0
            by_strategy: dict[str, int] = defaultdict(int)
            for diag in diagnostics:
                strategy, _, amb, _ = _derive_outcome(
                    **_diag_outcome_kwargs(diag), thresholds=config_thresholds,
                )
                if strategy is not None:
                    matched += 1
                    by_strategy[strategy] += 1
                    if amb:
                        ambiguous += 1
                    if strategy in {"jaccard", "levenshtein"}:
                        fuzzy_accepted += 1

            config_results[config_name] = {
                "matched_count": matched,
                "by_strategy": dict(by_strategy),
                "ambiguous_count": ambiguous,
                "fuzzy_accepted_count": fuzzy_accepted,
            }

        review, omitted = _build_review_records(diagnostics, entity_type)
        all_review.extend(review)
        jac_hist, lev_hist = _score_histograms(diagnostics)

        entity_results[entity_type] = {
            "index_size": len(index.records),
            "configs": config_results,
            "jaccard_score_histogram": jac_hist,
            "levenshtein_distance_histogram": lev_hist,
            "accent_impact": _compute_accent_impact(diagnostics, DEFAULT_MATCH_THRESHOLDS),
            "review_total_written": len(review),
            "review_omitted_by_cap": omitted,
        }

        logger.info(
            "Calibration [%s]: %d diagnostics, %d review cases",
            entity_type, len(diagnostics), len(review),
        )

    summary = {
        "total_donors_evaluated": len(donor_agg),
        "entity_types": entity_results,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    summary_path = output_dir / "match_calibration_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    review_path = output_dir / "match_calibration_review.jsonl"
    with AtomicJsonlWriter(review_path) as fh:
        for rec in all_review:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    logger.info(
        "Calibration complete: summary → %s, review → %s (%d records)",
        summary_path, review_path, len(all_review),
    )
    return summary_path
