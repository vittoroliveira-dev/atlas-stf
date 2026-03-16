"""Build empirical metrics report from TSE donation matching artifacts.

Reads existing JSONL artifacts produced by donation_match and produces a
structured JSON report with raw data quality, match quality and ambiguity
analysis.  Does NOT re-execute matching — pure post-hoc analysis.
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ._donor_identity import donor_identity_key
from ._match_helpers import iter_jsonl

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reservoir sampling for approximate percentiles
# ---------------------------------------------------------------------------

class _ReservoirSampler:
    """Reservoir sampling with fixed capacity for approximate percentiles."""

    __slots__ = ("_capacity", "_reservoir", "_count", "_rng")

    def __init__(self, capacity: int = 10_000, *, seed: int = 42) -> None:
        self._capacity = capacity
        self._reservoir: list[float] = []
        self._count = 0
        self._rng = random.Random(seed)

    def add(self, value: float) -> None:
        self._count += 1
        if len(self._reservoir) < self._capacity:
            self._reservoir.append(value)
        else:
            j = self._rng.randint(0, self._count - 1)
            if j < self._capacity:
                self._reservoir[j] = value

    def percentiles(self, ps: list[int]) -> dict[str, float | None]:
        if not self._reservoir:
            return {f"p{p}": None for p in ps}
        self._reservoir.sort()
        n = len(self._reservoir)
        result: dict[str, float | None] = {}
        for p in ps:
            idx = int(p / 100 * (n - 1))
            idx = max(0, min(idx, n - 1))
            result[f"p{p}"] = round(self._reservoir[idx], 2)
        return result


# ---------------------------------------------------------------------------
# Section builders (pure functions over file streams)
# ---------------------------------------------------------------------------

def _compute_raw_data_metrics(donations_path: Path) -> dict[str, Any]:
    """Streaming pass over donations_raw.jsonl for data quality metrics."""
    total = 0
    empty_donor_name = 0
    cpf_cnpj_empty = 0
    cpf_cnpj_masked = 0
    cpf_cnpj_valid_cpf = 0
    cpf_cnpj_valid_cnpj = 0
    identity_key_cpf = 0
    identity_key_name = 0
    election_year_dist: dict[str, int] = {}
    state_dist: dict[str, int] = {}
    sampler = _ReservoirSampler()

    # For homonymy proxy: name-only keys → set of distinct CPF/CNPJ seen
    name_key_cpfs: dict[str, set[str]] = {}

    if not donations_path.exists():
        return _empty_raw_metrics()

    for d in iter_jsonl(donations_path):
        total += 1
        name = d.get("donor_name_normalized", "")
        if not name:
            empty_donor_name += 1
            continue

        cpf_cnpj = d.get("donor_cpf_cnpj", "")

        # CPF/CNPJ classification
        if not cpf_cnpj:
            cpf_cnpj_empty += 1
        elif "*" in cpf_cnpj:
            cpf_cnpj_masked += 1
        else:
            digits = cpf_cnpj.replace(".", "").replace("-", "").replace("/", "")
            if digits.isdigit():
                if len(digits) == 11:
                    cpf_cnpj_valid_cpf += 1
                elif len(digits) == 14:
                    cpf_cnpj_valid_cnpj += 1

        # Identity key classification
        key = donor_identity_key(name, cpf_cnpj)
        if key.startswith("cpf:"):
            identity_key_cpf += 1
        else:
            identity_key_name += 1
            # Track distinct CPF/CNPJs for name-only keys (homonymy proxy)
            if cpf_cnpj and "*" not in cpf_cnpj:
                if key not in name_key_cpfs:
                    name_key_cpfs[key] = set()
                name_key_cpfs[key].add(cpf_cnpj)

        # Distributions
        year = d.get("election_year")
        if year is not None:
            yk = str(year)
            election_year_dist[yk] = election_year_dist.get(yk, 0) + 1
        state = d.get("state", "")
        if state:
            state_dist[state] = state_dist.get(state, 0) + 1

        # Amount sampling
        amount = d.get("donation_amount")
        if isinstance(amount, int | float) and amount > 0:
            sampler.add(float(amount))

    unique_keys = identity_key_cpf + identity_key_name
    homonymy_count = sum(1 for cpfs in name_key_cpfs.values() if len(cpfs) >= 2)

    return {
        "total_raw_records": total,
        "empty_donor_name_count": empty_donor_name,
        "cpf_cnpj_empty_count": cpf_cnpj_empty,
        "cpf_cnpj_empty_rate": round(cpf_cnpj_empty / total, 4) if total > 0 else None,
        "cpf_cnpj_masked_count": cpf_cnpj_masked,
        "cpf_cnpj_masked_rate": round(cpf_cnpj_masked / total, 4) if total > 0 else None,
        "cpf_cnpj_valid_cpf_count": cpf_cnpj_valid_cpf,
        "cpf_cnpj_valid_cnpj_count": cpf_cnpj_valid_cnpj,
        "identity_key_cpf_count": identity_key_cpf,
        "identity_key_name_count": identity_key_name,
        "identity_key_cpf_rate": (
            round(identity_key_cpf / unique_keys, 4) if unique_keys > 0 else None
        ),
        "unique_identity_keys_count": unique_keys,
        "homonymy_proxy_count": homonymy_count,
        "homonymy_proxy_rate": (
            round(homonymy_count / identity_key_name, 4) if identity_key_name > 0 else None
        ),
        "election_year_distribution": dict(sorted(election_year_dist.items())),
        "state_distribution": dict(sorted(state_dist.items(), key=lambda x: -x[1])),
        "amount_percentiles": sampler.percentiles([25, 50, 75, 90, 99]),
    }


def _empty_raw_metrics() -> dict[str, Any]:
    return {
        "total_raw_records": 0,
        "empty_donor_name_count": 0,
        "cpf_cnpj_empty_count": 0,
        "cpf_cnpj_empty_rate": None,
        "cpf_cnpj_masked_count": 0,
        "cpf_cnpj_masked_rate": None,
        "cpf_cnpj_valid_cpf_count": 0,
        "cpf_cnpj_valid_cnpj_count": 0,
        "identity_key_cpf_count": 0,
        "identity_key_name_count": 0,
        "identity_key_cpf_rate": None,
        "unique_identity_keys_count": 0,
        "homonymy_proxy_count": 0,
        "homonymy_proxy_rate": None,
        "election_year_distribution": {},
        "state_distribution": {},
        "amount_percentiles": {"p25": None, "p50": None, "p75": None, "p90": None, "p99": None},
    }


def _compute_match_metrics(match_path: Path) -> dict[str, Any]:
    """Streaming pass over donation_match.jsonl for match quality metrics."""
    total = 0
    by_entity_type: dict[str, int] = {}
    strategy_dist: dict[str, int] = {}
    strategy_by_entity: dict[str, dict[str, int]] = {}
    jaccard_histogram: dict[str, int] = {
        "[0.80, 0.85)": 0,
        "[0.85, 0.90)": 0,
        "[0.90, 0.95)": 0,
        "[0.95, 1.00]": 0,
    }
    levenshtein_histogram: dict[str, int] = {"0": 0, "1": 0, "2": 0}
    red_flag_count = 0
    red_flag_by_strategy: dict[str, int] = {}
    corporate_enriched = 0

    if not match_path.exists():
        return _empty_match_metrics()

    bucket_labels = ["[0.80, 0.85)", "[0.85, 0.90)", "[0.90, 0.95)", "[0.95, 1.00]"]

    for m in iter_jsonl(match_path):
        total += 1
        entity_type = m.get("entity_type", "")
        by_entity_type[entity_type] = by_entity_type.get(entity_type, 0) + 1

        strategy = m.get("match_strategy", "")
        strategy_dist[strategy] = strategy_dist.get(strategy, 0) + 1
        if entity_type not in strategy_by_entity:
            strategy_by_entity[entity_type] = {}
        strategy_by_entity[entity_type][strategy] = strategy_by_entity[entity_type].get(strategy, 0) + 1

        score = m.get("match_score")
        if strategy == "jaccard" and isinstance(score, int | float):
            bucket_idx = min(int((score - 0.80) / 0.05), 3)
            bucket_idx = max(0, bucket_idx)
            jaccard_histogram[bucket_labels[bucket_idx]] += 1
        elif strategy == "levenshtein" and isinstance(score, int | float):
            key = str(int(score))
            if key in levenshtein_histogram:
                levenshtein_histogram[key] += 1

        if m.get("red_flag"):
            red_flag_count += 1
            red_flag_by_strategy[strategy] = red_flag_by_strategy.get(strategy, 0) + 1

        if any(
            m.get(f) is not None
            for f in ("donor_document_type", "donor_tax_id_normalized", "economic_group_id")
        ):
            corporate_enriched += 1

    return {
        "total_matches": total,
        "match_by_entity_type": dict(sorted(by_entity_type.items())),
        "match_strategy_distribution": dict(sorted(strategy_dist.items())),
        "match_strategy_by_entity_type": {
            k: dict(sorted(v.items())) for k, v in sorted(strategy_by_entity.items())
        },
        "jaccard_score_histogram": jaccard_histogram,
        "levenshtein_score_histogram": levenshtein_histogram,
        "red_flag_count": red_flag_count,
        "red_flag_by_strategy": dict(sorted(red_flag_by_strategy.items())),
        "corporate_enriched_count": corporate_enriched,
    }


def _empty_match_metrics() -> dict[str, Any]:
    return {
        "total_matches": 0,
        "match_by_entity_type": {},
        "match_strategy_distribution": {},
        "match_strategy_by_entity_type": {},
        "jaccard_score_histogram": {
            "[0.80, 0.85)": 0,
            "[0.85, 0.90)": 0,
            "[0.90, 0.95)": 0,
            "[0.95, 1.00]": 0,
        },
        "levenshtein_score_histogram": {"0": 0, "1": 0, "2": 0},
        "red_flag_count": 0,
        "red_flag_by_strategy": {},
        "corporate_enriched_count": 0,
    }


def _compute_ambiguous_metrics(
    ambiguous_path: Path, total_matches: int
) -> dict[str, Any]:
    """Streaming pass over donation_match_ambiguous.jsonl."""
    total_ambiguous = 0
    by_entity_type: dict[str, int] = {}
    by_uncertainty_note: dict[str, int] = {}
    candidate_count_dist: dict[str, int] = {"2": 0, "3": 0, "4+": 0}
    total_donated_brl = 0.0

    if not ambiguous_path.exists():
        return _empty_ambiguous_metrics(total_matches)

    for rec in iter_jsonl(ambiguous_path):
        total_ambiguous += 1
        et = rec.get("entity_type", "")
        by_entity_type[et] = by_entity_type.get(et, 0) + 1

        note = rec.get("uncertainty_note", "")
        if note:
            by_uncertainty_note[note] = by_uncertainty_note.get(note, 0) + 1

        cc = rec.get("candidate_count")
        if isinstance(cc, int):
            if cc >= 4:
                candidate_count_dist["4+"] += 1
            elif cc == 3:
                candidate_count_dist["3"] += 1
            else:
                candidate_count_dist["2"] += 1

        amt = rec.get("total_donated_brl", 0.0)
        if isinstance(amt, int | float):
            total_donated_brl += amt

    denominator = total_matches + total_ambiguous
    ambiguous_rate: float | None = (
        round(total_ambiguous / denominator, 4) if denominator > 0 else None
    )

    return {
        "total_ambiguous": total_ambiguous,
        "ambiguous_by_entity_type": dict(sorted(by_entity_type.items())),
        "ambiguous_by_uncertainty_note": dict(sorted(by_uncertainty_note.items())),
        "candidate_count_distribution": candidate_count_dist,
        "total_donated_brl_ambiguous": round(total_donated_brl, 2),
        "ambiguous_rate": ambiguous_rate,
        "ambiguous_rate_formula": "total_ambiguous / (total_matches + total_ambiguous)",
    }


def _empty_ambiguous_metrics(total_matches: int) -> dict[str, Any]:
    return {
        "total_ambiguous": 0,
        "ambiguous_by_entity_type": {},
        "ambiguous_by_uncertainty_note": {},
        "candidate_count_distribution": {"2": 0, "3": 0, "4+": 0},
        "total_donated_brl_ambiguous": 0.0,
        "ambiguous_rate": 0.0 if total_matches > 0 else None,
        "ambiguous_rate_formula": "total_ambiguous / (total_matches + total_ambiguous)",
    }


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

_METHODOLOGY_NOTES: dict[str, str] = {
    "homonymy_proxy": (
        "Contagem de chaves name: associadas a 2+ CPF/CNPJ distintos no raw "
        "— indica homonimia potencial."
    ),
    "masked_cpf_definition": "Documentos contendo '*' (ex: ***.***.***-**).",
    "ambiguous_definition": (
        "Doadores com 2+ candidatos a match com score fuzzy identico."
    ),
    "percentile_method": (
        "Percentis aproximados via reservoir sampling de 10.000 amostras."
    ),
    "jaccard_histogram_buckets": (
        "Intervalos semiabertos [low, high) exceto o ultimo que e fechado [0.95, 1.00]."
    ),
    "ambiguous_rate_formula": (
        "total_ambiguous / (total_matches + total_ambiguous); None se denominador = 0."
    ),
    "identity_key_computation": (
        "Usa o mesmo helper donor_identity_key() do pipeline de matching "
        "(analytics._donor_identity)."
    ),
    "no_precision_recall": (
        "Metricas observaveis/proxy. Precisao/recall real requer ground truth rotulado."
    ),
}


def build_empirical_report(
    *,
    tse_dir: Path = Path("data/raw/tse"),
    analytics_dir: Path = Path("data/analytics"),
    output_dir: Path = Path("data/analytics"),
) -> Path:
    """Build empirical metrics report from existing donation matching artifacts."""
    output_dir.mkdir(parents=True, exist_ok=True)

    donations_path = tse_dir / "donations_raw.jsonl"
    match_path = analytics_dir / "donation_match.jsonl"
    ambiguous_path = analytics_dir / "donation_match_ambiguous.jsonl"

    logger.info("Building empirical report from %s, %s, %s", donations_path, match_path, ambiguous_path)

    raw_metrics = _compute_raw_data_metrics(donations_path)
    match_metrics = _compute_match_metrics(match_path)
    ambiguous_metrics = _compute_ambiguous_metrics(
        ambiguous_path, match_metrics["total_matches"]
    )

    report = {
        "raw_data_quality": raw_metrics,
        "match_quality": match_metrics,
        "ambiguous_analysis": ambiguous_metrics,
        "methodology_notes": _METHODOLOGY_NOTES,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    output_path = output_dir / "donation_empirical_metrics.json"
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("Wrote empirical metrics report to %s", output_path)
    return output_path
