"""Empirical evaluation of scoring methods for outlier alerts.

Read-only script: loads analytics data, computes 13 alternative scoring
methods, evaluates with 3 proxy ground-truth labels and 5 metrics, and
prints a comparative table.

Usage:
    uv run python scripts/score_evaluation.py
"""

from __future__ import annotations

import json
import math
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_ANALYTICS = Path("data/analytics")
DATA_CURATED = Path("data/curated")

PROXY_COMPOUND_MIN_SIGNALS = 2
ML_RARITY_THRESHOLD = 0.9
BOOTSTRAP_N = 100
BONFERRONI_ALPHA = 0.05 / 36  # 12 methods × 3 proxies
SPLIT_SEED = 42
TOP_K_VALUES = (100, 500, 1000, 5000)

# Bonus magnitudes (tuned on dev split, reported on holdout)
BONUS_DEVIATION = 0.05
BONUS_SEQUENTIAL = 0.05
BONUS_COUNSEL = 0.10
BONUS_SANCTION_DONATION = 0.10
ML_WEIGHT = 0.30


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def _load_jsonl(path: Path, fields: set[str] | None = None) -> list[dict[str, Any]]:
    """Load a JSONL file into a list of dicts.

    If *fields* is given, only keep those keys (saves memory on large files).
    """
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rec = json.loads(line)
                if fields is not None:
                    rec = {k: v for k, v in rec.items() if k in fields}
                records.append(rec)
    return records


def _index_by(records: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    """Index records by a single key (first match wins)."""
    idx: dict[str, dict[str, Any]] = {}
    for rec in records:
        k = rec.get(key)
        if k is not None and k not in idx:
            idx[k] = rec
    return idx


def _index_by_composite(
    records: list[dict[str, Any]], keys: tuple[str, ...]
) -> dict[tuple[str, ...], dict[str, Any]]:
    """Index by composite key (first match wins)."""
    idx: dict[tuple[str, ...], dict[str, Any]] = {}
    for rec in records:
        vals = tuple(rec.get(k) for k in keys)
        if None not in vals and vals not in idx:
            idx[vals] = rec
    return idx


def _group_by(
    records: list[dict[str, Any]], key: str
) -> dict[str, list[dict[str, Any]]]:
    """Group records by a key."""
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        k = rec.get(key)
        if k is not None:
            groups[k].append(rec)
    return groups


# ---------------------------------------------------------------------------
# Score component recomputation
# ---------------------------------------------------------------------------

@dataclass
class Dimension:
    name: str
    observed_value: str
    probability: float  # P(observed | baseline)
    rarity: float  # 1 - probability


def _normalize_dist(dist: dict[str, int], total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {str(k): float(v) / float(total) for k, v in dist.items() if k and int(v) > 0}


def _extract_dimensions(
    event: dict[str, Any], baseline: dict[str, Any], process_class: str | None
) -> list[Dimension]:
    """Recompute the score dimensions from event + baseline (mirrors score.py)."""
    total = int(baseline.get("event_count") or 0)
    dims: list[Dimension] = []

    # 1. decision_progress
    dp = event.get("decision_progress")
    dp_dist = baseline.get("expected_decision_progress_distribution") or {}
    if dp and dp_dist and total > 0:
        norm = _normalize_dist(dp_dist, total)
        prob = norm.get(str(dp).strip(), 0.0)
        dims.append(Dimension("decision_progress", str(dp).strip(), prob, 1.0 - prob))

    # 2. current_rapporteur
    rapp = event.get("current_rapporteur")
    rapp_dist = baseline.get("expected_rapporteur_distribution") or {}
    if rapp and rapp_dist and total > 0:
        norm = _normalize_dist(rapp_dist, total)
        prob = norm.get(str(rapp).strip(), 0.0)
        dims.append(Dimension("current_rapporteur", str(rapp).strip(), prob, 1.0 - prob))

    # 3. judging_body
    jb = event.get("judging_body")
    jb_dist = baseline.get("expected_judging_body_distribution") or {}
    if jb and jb_dist and total > 0:
        norm = _normalize_dist(jb_dist, total)
        prob = norm.get(str(jb).strip(), 0.0)
        dims.append(Dimension("judging_body", str(jb).strip(), prob, 1.0 - prob))

    # 4. process_class_outcome (class-stratified)
    if process_class and dp:
        by_class = baseline.get("expected_progress_by_class") or {}
        class_dist = by_class.get(process_class)
        if class_dist:
            class_total = sum(int(v) for v in class_dist.values())
            if class_total >= 5:
                freq = int(class_dist.get(str(dp).strip(), 0)) / class_total
                dims.append(Dimension("process_class_outcome", f"{process_class}/{str(dp).strip()}", freq, 1.0 - freq))
            else:
                # fallback to global
                norm = _normalize_dist(dp_dist, total)
                freq = norm.get(str(dp).strip(), 0.0)
                dims.append(Dimension("process_class_outcome", f"{process_class}/{str(dp).strip()}", freq, 1.0 - freq))
        else:
            # no class-specific data, use global
            norm = _normalize_dist(dp_dist, total)
            freq = norm.get(str(dp).strip(), 0.0)
            dims.append(Dimension("process_class_outcome", f"{process_class}/{str(dp).strip()}", freq, 1.0 - freq))

    return dims


# ---------------------------------------------------------------------------
# Enriched alert record
# ---------------------------------------------------------------------------

@dataclass
class EnrichedAlert:
    alert_id: str
    process_id: str
    decision_event_id: str
    comparison_group_id: str
    original_score: float
    dimensions: list[Dimension] = field(default_factory=list)
    rapporteur: str | None = None
    decision_year: int | None = None
    process_class: str | None = None

    # External signals
    deviation_flag: bool = False
    sequential_bias_flag: bool = False
    counsel_affinity_flag: bool = False
    sanction_flag: bool = False
    donation_flag: bool = False
    ml_rarity_score: float | None = None

    # Proxy labels
    proxy_compound: bool = False
    proxy_deviation: bool = False
    proxy_ml: bool = False

    # Computed scores (M0..M12)
    scores: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Scoring methods
# ---------------------------------------------------------------------------

def _m0_arithmetic(dims: list[Dimension]) -> float:
    """M0: Current method — arithmetic mean of rarities."""
    if not dims:
        return 0.0
    return sum(d.rarity for d in dims) / len(dims)


def _entropy_weights(dims: list[Dimension]) -> list[float]:
    """Compute surprisal-based weights: w_i = -log2(prob_i) / max_surprisal.

    For prob=0, use max surprisal (log2 of baseline size capped at 20 bits).
    """
    max_surprisal = 20.0  # cap
    weights: list[float] = []
    for d in dims:
        if d.probability <= 0:
            weights.append(1.0)
        else:
            surprisal = min(-math.log2(d.probability), max_surprisal)
            weights.append(surprisal / max_surprisal)
    return weights


def _m1_entropy_weighted(dims: list[Dimension]) -> float:
    """M1: Entropy-weighted mean."""
    if not dims:
        return 0.0
    weights = _entropy_weights(dims)
    total_w = sum(weights)
    if total_w <= 0:
        return 0.0
    return sum(w * d.rarity for w, d in zip(weights, dims)) / total_w


def _m2_filtered(dims: list[Dimension]) -> float:
    """M2: Filter out dimensions with prob > 0.4, then arithmetic mean."""
    filtered = [d for d in dims if d.probability < 0.4]
    if not filtered:
        return _m0_arithmetic(dims)  # fallback
    return sum(d.rarity for d in filtered) / len(filtered)


def _m3_geometric(dims: list[Dimension]) -> float:
    """M3: Geometric mean of rarities."""
    if not dims:
        return 0.0
    product = 1.0
    for d in dims:
        product *= max(d.rarity, 1e-10)  # avoid zero
    return product ** (1.0 / len(dims))


def _m4_entropy_filtered(dims: list[Dimension]) -> float:
    """M4: Filter prob > 0.4, then entropy-weight."""
    filtered = [d for d in dims if d.probability < 0.4]
    if not filtered:
        return _m1_entropy_weighted(dims)  # fallback
    return _m1_entropy_weighted(filtered)


def _m5_max(dims: list[Dimension]) -> float:
    """M5: Max rarity."""
    if not dims:
        return 0.0
    return max(d.rarity for d in dims)


def _m6_harmonic(dims: list[Dimension]) -> float:
    """M6: Harmonic mean of rarities."""
    if not dims:
        return 0.0
    pos = [d.rarity for d in dims if d.rarity > 0]
    if not pos:
        return 0.0
    return len(pos) / sum(1.0 / r for r in pos)


def _m7_trimmed(dims: list[Dimension]) -> float:
    """M7: Remove lowest rarity dim, arithmetic mean of rest."""
    if len(dims) <= 1:
        return _m0_arithmetic(dims)
    sorted_dims = sorted(dims, key=lambda d: d.rarity)
    trimmed = sorted_dims[1:]
    return sum(d.rarity for d in trimmed) / len(trimmed)


def _cap(score: float) -> float:
    return min(score, 1.0)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def _precision_at_k(scores: list[float], labels: list[bool], k: int) -> float:
    """Precision@K: fraction of top-K that are label-positive."""
    if k <= 0 or not scores:
        return 0.0
    paired = sorted(zip(scores, labels), key=lambda x: -x[0])
    top_k = paired[:k]
    positives = sum(1 for _, label in top_k if label)
    return positives / len(top_k)


def _auroc(scores: list[float], labels: list[bool]) -> float:
    """AUROC via sort-based Mann-Whitney U statistic — O(n log n)."""
    n_pos = sum(1 for l in labels if l)
    n_neg = len(labels) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5
    # Sort by score descending, break ties by label=False first
    # so that tied scores contribute 0.5 correctly via rank averaging
    paired = sorted(zip(scores, labels), key=lambda x: x[0])
    # Assign ranks (1-based), averaging ties
    ranks = [0.0] * len(paired)
    i = 0
    while i < len(paired):
        j = i
        while j < len(paired) and paired[j][0] == paired[i][0]:
            j += 1
        avg_rank = (i + 1 + j) / 2.0  # average of ranks i+1..j
        for k in range(i, j):
            ranks[k] = avg_rank
        i = j
    # Sum of ranks for positives
    rank_sum_pos = sum(r for r, (_, l) in zip(ranks, paired) if l)
    u = rank_sum_pos - n_pos * (n_pos + 1) / 2
    return u / (n_pos * n_neg)


def _separation(scores: list[float], labels: list[bool]) -> float:
    """Mean(score | label=True) - Mean(score | label=False)."""
    pos = [s for s, l in zip(scores, labels) if l]
    neg = [s for s, l in zip(scores, labels) if not l]
    if not pos or not neg:
        return 0.0
    return sum(pos) / len(pos) - sum(neg) / len(neg)


def _gini_concentration(scores: list[float], labels: list[bool]) -> float:
    """Gini coefficient of proxy-positive concentration in score ranking.

    Perfect concentration (all positives at top) = 1.0.
    Random = 0.0.
    """
    n = len(scores)
    n_pos = sum(1 for l in labels if l)
    if n_pos == 0 or n_pos == n:
        return 0.0
    paired = sorted(zip(scores, labels), key=lambda x: -x[0])
    cum_pos = 0.0
    area = 0.0
    for i, (_, label) in enumerate(paired):
        if label:
            cum_pos += 1
        area += cum_pos
    # Normalize: max area vs random area
    max_area = n_pos * (n_pos + 1) / 2 + n_pos * (n - n_pos)
    random_area = n_pos * (n + 1) / 2
    if max_area == random_area:
        return 0.0
    return (area - random_area) / (max_area - random_area)


def _volume_above_threshold(scores: list[float], threshold: float = 0.75) -> int:
    return sum(1 for s in scores if s >= threshold)


# ---------------------------------------------------------------------------
# Statistical tests
# ---------------------------------------------------------------------------

def _wilcoxon_signed_rank(diffs: list[float]) -> tuple[float, float]:
    """Wilcoxon signed-rank test (two-sided). Returns (W+, approx p-value).

    Uses normal approximation for n > 20.
    """
    nonzero = [(abs(d), d) for d in diffs if d != 0.0]
    if len(nonzero) < 10:
        return 0.0, 1.0
    nonzero.sort(key=lambda x: x[0])
    # Assign ranks
    n = len(nonzero)
    ranks = list(range(1, n + 1))
    # Handle ties (average rank)
    i = 0
    while i < n:
        j = i
        while j < n and nonzero[j][0] == nonzero[i][0]:
            j += 1
        avg_rank = sum(ranks[i:j]) / (j - i)
        for k in range(i, j):
            ranks[k] = avg_rank
        i = j

    w_plus = sum(r for r, (_, d) in zip(ranks, nonzero) if d > 0)
    # Normal approximation
    mean_w = n * (n + 1) / 4
    std_w = math.sqrt(n * (n + 1) * (2 * n + 1) / 24)
    if std_w == 0:
        return w_plus, 1.0
    z = (w_plus - mean_w) / std_w
    # Two-sided p-value via standard normal CDF approximation
    p = 2.0 * _normal_cdf(-abs(z))
    return w_plus, p


def _normal_cdf(z: float) -> float:
    """Approximate standard normal CDF."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _bootstrap_auroc_ci(
    scores: list[float], labels: list[bool], n_boot: int = BOOTSTRAP_N, seed: int = 99
) -> tuple[float, float, float]:
    """Bootstrap 95% CI for AUROC. Returns (auroc, lower, upper).

    For large datasets (>5000), subsample to 5000 for each bootstrap
    iteration to keep runtime manageable.
    """
    rng = random.Random(seed)
    n = len(scores)
    subsample_size = min(n, 5000)
    aurocs: list[float] = []
    for _ in range(n_boot):
        indices = [rng.randint(0, n - 1) for _ in range(subsample_size)]
        boot_scores = [scores[i] for i in indices]
        boot_labels = [labels[i] for i in indices]
        aurocs.append(_auroc(boot_scores, boot_labels))
    aurocs.sort()
    lo_idx = max(0, int(0.025 * n_boot))
    hi_idx = min(n_boot - 1, int(0.975 * n_boot))
    point = _auroc(scores, labels)
    return point, aurocs[lo_idx], aurocs[hi_idx]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    t0 = time.time()

    # ===== PHASE 1: Load data =====
    print("=" * 70)
    print("FASE 1: Carregando dados...")
    print("=" * 70)

    t1 = time.time()

    alerts_raw = _load_jsonl(DATA_ANALYTICS / "outlier_alert.jsonl")
    print(f"  outlier_alert:       {len(alerts_raw):>8} registros")

    baselines_raw = _load_jsonl(DATA_ANALYTICS / "baseline.jsonl")
    baselines = _index_by(baselines_raw, "comparison_group_id")
    print(f"  baseline:            {len(baselines):>8} registros")

    _de_fields = {"decision_event_id", "current_rapporteur", "decision_year",
                   "decision_progress", "judging_body", "process_id"}
    events_raw = _load_jsonl(DATA_CURATED / "decision_event.jsonl", fields=_de_fields)
    events = _index_by(events_raw, "decision_event_id")
    print(f"  decision_event:      {len(events):>8} registros")
    del events_raw  # free memory

    _proc_fields = {"process_id", "process_class"}
    processes_raw = _load_jsonl(DATA_CURATED / "process.jsonl", fields=_proc_fields)
    process_class_map: dict[str, str] = {}
    for rec in processes_raw:
        pid = rec.get("process_id")
        pc = rec.get("process_class")
        if pid and pc:
            process_class_map[pid] = pc
    print(f"  process:             {len(process_class_map):>8} registros")
    del processes_raw

    rapp_profile_raw = _load_jsonl(DATA_ANALYTICS / "rapporteur_profile.jsonl")
    rapp_profiles = _index_by_composite(
        rapp_profile_raw, ("rapporteur", "process_class", "decision_year")
    )
    print(f"  rapporteur_profile:  {len(rapp_profiles):>8} registros")

    seq_raw = _load_jsonl(DATA_ANALYTICS / "sequential_analysis.jsonl")
    seq_index = _index_by_composite(seq_raw, ("rapporteur", "decision_year"))
    print(f"  sequential_analysis: {len(seq_index):>8} registros")

    # counsel_affinity: index red_flag=True by (rapporteur, counsel_id)
    aff_raw = _load_jsonl(DATA_ANALYTICS / "counsel_affinity.jsonl")
    aff_red: dict[tuple[str, str], bool] = {}
    for rec in aff_raw:
        if rec.get("red_flag"):
            key = (rec.get("rapporteur", ""), rec.get("counsel_id", ""))
            aff_red[key] = True
    print(f"  counsel_affinity:    {len(aff_raw):>8} registros ({len(aff_red)} red flags)")
    del aff_raw

    # sanction_match: index red_flag=True by party_id
    sanc_raw = _load_jsonl(DATA_ANALYTICS / "sanction_match.jsonl")
    sanc_red: set[str] = set()
    for rec in sanc_raw:
        if rec.get("red_flag"):
            pid = rec.get("party_id")
            if pid:
                sanc_red.add(pid)
    print(f"  sanction_match:      {len(sanc_raw):>8} registros ({len(sanc_red)} red flags)")
    del sanc_raw

    # donation_match: index red_flag=True by party_id
    don_raw = _load_jsonl(DATA_ANALYTICS / "donation_match.jsonl")
    don_red: set[str] = set()
    for rec in don_raw:
        if rec.get("red_flag"):
            pid = rec.get("party_id")
            if pid:
                don_red.add(pid)
    print(f"  donation_match:      {len(don_raw):>8} registros ({len(don_red)} red flags)")
    del don_raw

    # ml_outlier_score: index by decision_event_id
    _ml_fields = {"decision_event_id", "ml_rarity_score"}
    ml_raw = _load_jsonl(DATA_ANALYTICS / "ml_outlier_score.jsonl", fields=_ml_fields)
    ml_index = _index_by(ml_raw, "decision_event_id")
    print(f"  ml_outlier_score:    {len(ml_index):>8} registros")
    del ml_raw

    # compound_risk: process_id → max signal_count
    _cr_fields = {"signal_count", "shared_process_ids"}
    cr_raw = _load_jsonl(DATA_ANALYTICS / "compound_risk.jsonl", fields=_cr_fields)
    compound_process_signals: dict[str, int] = defaultdict(int)
    for rec in cr_raw:
        sig_count = int(rec.get("signal_count") or 0)
        for pid in rec.get("shared_process_ids") or []:
            compound_process_signals[pid] = max(compound_process_signals[pid], sig_count)
    print(f"  compound_risk:       {len(cr_raw):>8} registros")
    del cr_raw

    # process_party_link: process_id → set(party_id)
    _ppl_fields = {"process_id", "party_id"}
    ppl_raw = _load_jsonl(DATA_CURATED / "process_party_link.jsonl", fields=_ppl_fields)
    process_parties: dict[str, set[str]] = defaultdict(set)
    for rec in ppl_raw:
        proc = rec.get("process_id")
        party = rec.get("party_id")
        if proc and party:
            process_parties[proc].add(party)
    print(f"  process_party_link:  {len(ppl_raw):>8} registros")
    del ppl_raw

    # process_counsel_link: process_id → set(counsel_id)
    _pcl_fields = {"process_id", "counsel_id"}
    pcl_raw = _load_jsonl(DATA_CURATED / "process_counsel_link.jsonl", fields=_pcl_fields)
    process_counsels: dict[str, set[str]] = defaultdict(set)
    for rec in pcl_raw:
        proc = rec.get("process_id")
        csl = rec.get("counsel_id")
        if proc and csl:
            process_counsels[proc].add(csl)
    print(f"  process_counsel_link:{len(pcl_raw):>8} registros")
    del pcl_raw

    print(f"\n  Carga concluída em {time.time() - t1:.1f}s")

    # ===== PHASE 2: Enrich alerts =====
    print("\n" + "=" * 70)
    print("FASE 2: Enriquecendo alertas...")
    print("=" * 70)
    t2 = time.time()

    enriched: list[EnrichedAlert] = []
    skipped = 0

    for rec in alerts_raw:
        alert_id = rec.get("alert_id", "")
        process_id = rec.get("process_id", "")
        de_id = rec.get("decision_event_id", "")
        cg_id = rec.get("comparison_group_id", "")
        original_score = float(rec.get("alert_score") or 0.0)

        # Lookup event and baseline
        event = events.get(de_id)
        baseline = baselines.get(cg_id)
        if not event or not baseline:
            skipped += 1
            continue

        rapporteur = event.get("current_rapporteur")
        decision_year = event.get("decision_year")
        if isinstance(decision_year, str):
            try:
                decision_year = int(decision_year)
            except (ValueError, TypeError):
                decision_year = None
        process_class = process_class_map.get(process_id)

        # Extract dimensions
        dims = _extract_dimensions(event, baseline, process_class)
        if not dims:
            skipped += 1
            continue

        ea = EnrichedAlert(
            alert_id=alert_id,
            process_id=process_id,
            decision_event_id=de_id,
            comparison_group_id=cg_id,
            original_score=original_score,
            dimensions=dims,
            rapporteur=rapporteur,
            decision_year=decision_year,
            process_class=process_class,
        )

        # Attach rapporteur_profile.deviation_flag
        if rapporteur and process_class and decision_year is not None:
            rp = rapp_profiles.get((rapporteur, process_class, decision_year))
            if rp:
                ea.deviation_flag = bool(rp.get("deviation_flag"))

        # Attach sequential_analysis.sequential_bias_flag
        if rapporteur and decision_year is not None:
            sq = seq_index.get((rapporteur, decision_year))
            if sq:
                ea.sequential_bias_flag = bool(sq.get("sequential_bias_flag"))

        # Attach counsel_affinity red_flag
        if rapporteur:
            counsels = process_counsels.get(process_id, set())
            for csl_id in counsels:
                if aff_red.get((rapporteur, csl_id)):
                    ea.counsel_affinity_flag = True
                    break

        # Attach sanction/donation red_flag
        parties = process_parties.get(process_id, set())
        for party_id in parties:
            if party_id in sanc_red:
                ea.sanction_flag = True
            if party_id in don_red:
                ea.donation_flag = True
            if ea.sanction_flag and ea.donation_flag:
                break

        # Attach ML rarity score
        ml_rec = ml_index.get(de_id)
        if ml_rec:
            ea.ml_rarity_score = ml_rec.get("ml_rarity_score")

        # Proxy labels
        ea.proxy_compound = compound_process_signals.get(process_id, 0) >= PROXY_COMPOUND_MIN_SIGNALS
        ea.proxy_deviation = ea.deviation_flag
        ea.proxy_ml = ea.ml_rarity_score is not None and ea.ml_rarity_score > ML_RARITY_THRESHOLD

        enriched.append(ea)

    print(f"  Alertas enriquecidos: {len(enriched)} (skipped: {skipped})")
    proxy_a_count = sum(1 for e in enriched if e.proxy_compound)
    proxy_b_count = sum(1 for e in enriched if e.proxy_deviation)
    proxy_c_count = sum(1 for e in enriched if e.proxy_ml)
    print(f"  Proxy A (compound risk ≥{PROXY_COMPOUND_MIN_SIGNALS} signals): {proxy_a_count}")
    print(f"  Proxy B (rapporteur deviation): {proxy_b_count}")
    print(f"  Proxy C (ML rarity > {ML_RARITY_THRESHOLD}): {proxy_c_count}")

    sig_counsel = sum(1 for e in enriched if e.counsel_affinity_flag)
    sig_sanction = sum(1 for e in enriched if e.sanction_flag)
    sig_donation = sum(1 for e in enriched if e.donation_flag)
    sig_sequential = sum(1 for e in enriched if e.sequential_bias_flag)
    sig_deviation = sum(1 for e in enriched if e.deviation_flag)
    sig_ml = sum(1 for e in enriched if e.ml_rarity_score is not None)
    print("\n  Sinais disponíveis:")
    print(f"    counsel_affinity red_flag: {sig_counsel}")
    print(f"    sanction red_flag:         {sig_sanction}")
    print(f"    donation red_flag:         {sig_donation}")
    print(f"    sequential_bias_flag:      {sig_sequential}")
    print(f"    deviation_flag:            {sig_deviation}")
    print(f"    ml_rarity_score:           {sig_ml}")

    print(f"\n  Enriquecimento concluído em {time.time() - t2:.1f}s")

    # ===== PHASE 3: Compute alternative scores =====
    print("\n" + "=" * 70)
    print("FASE 3: Computando 13 métodos de scoring...")
    print("=" * 70)
    t3 = time.time()

    for ea in enriched:
        dims = ea.dimensions
        m1 = _m1_entropy_weighted(dims)

        ea.scores = {
            "M0": _m0_arithmetic(dims),
            "M1": m1,
            "M2": _m2_filtered(dims),
            "M3": _m3_geometric(dims),
            "M4": _m4_entropy_filtered(dims),
            "M5": _m5_max(dims),
            "M6": _m6_harmonic(dims),
            "M7": _m7_trimmed(dims),
            "M8": _cap(m1 + (BONUS_DEVIATION if ea.deviation_flag else 0.0)),
            "M9": _cap(m1 + (BONUS_SEQUENTIAL if ea.sequential_bias_flag else 0.0)),
            "M10": _cap(m1 + (BONUS_COUNSEL if ea.counsel_affinity_flag else 0.0)),
            "M11": _cap(m1 + (BONUS_SANCTION_DONATION if (ea.sanction_flag or ea.donation_flag) else 0.0)),
            "M12": _cap(
                (1.0 - ML_WEIGHT) * m1 + ML_WEIGHT * (ea.ml_rarity_score or m1)
            ),
        }

    print(f"  Scores computados em {time.time() - t3:.1f}s")

    # ===== PHASE 4: Evaluation =====
    print("\n" + "=" * 70)
    print("FASE 4: Avaliação...")
    print("=" * 70)
    t4 = time.time()

    method_names = ["M0", "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9", "M10", "M11", "M12"]
    method_labels = {
        "M0": "Atual (média)",
        "M1": "Entropy-weighted",
        "M2": "Filtered(p<0.4)",
        "M3": "Geometric mean",
        "M4": "Entropy+filtered",
        "M5": "Max-of-N",
        "M6": "Harmonic mean",
        "M7": "Trimmed mean",
        "M8": "M1+deviation",
        "M9": "M1+sequential",
        "M10": "M1+counsel aff",
        "M11": "M1+sanç/doação",
        "M12": "M1+ML ensemble",
    }
    proxy_names = [
        ("A", "compound", lambda e: e.proxy_compound),
        ("B", "deviation", lambda e: e.proxy_deviation),
        ("C", "ML agree", lambda e: e.proxy_ml),
    ]

    # 4.1 Split dev/holdout 50/50
    rng = random.Random(SPLIT_SEED)
    indices = list(range(len(enriched)))
    rng.shuffle(indices)
    mid = len(indices) // 2
    dev_indices = set(indices[:mid])
    holdout_indices = set(indices[mid:])

    dev_alerts = [enriched[i] for i in sorted(dev_indices)]
    holdout_alerts = [enriched[i] for i in sorted(holdout_indices)]
    print(f"  Split: dev={len(dev_alerts)}, holdout={len(holdout_alerts)}")

    # 4.2 Temporal split
    pre2020 = [e for e in enriched if e.decision_year is not None and e.decision_year < 2020]
    post2020 = [e for e in enriched if e.decision_year is not None and e.decision_year >= 2020]
    print(f"  Temporal: pre-2020={len(pre2020)}, post-2020={len(post2020)}")

    # ---- Results storage ----
    @dataclass
    class MethodResult:
        method: str
        auroc: dict[str, float] = field(default_factory=dict)
        auroc_ci_lo: dict[str, float] = field(default_factory=dict)
        auroc_ci_hi: dict[str, float] = field(default_factory=dict)
        separation: dict[str, float] = field(default_factory=dict)
        precision_at_k: dict[str, dict[int, float]] = field(default_factory=dict)
        gini: dict[str, float] = field(default_factory=dict)
        volume_075: int = 0
        wilcoxon_p: dict[str, float] = field(default_factory=dict)
        # Holdout
        auroc_holdout: dict[str, float] = field(default_factory=dict)
        # Temporal stability
        auroc_pre: dict[str, float] = field(default_factory=dict)
        auroc_post: dict[str, float] = field(default_factory=dict)

    results: dict[str, MethodResult] = {}

    def _eval_method_on_set(
        method: str, alerts_set: list[EnrichedAlert], proxy_name: str, proxy_fn: Any
    ) -> tuple[float, float, float, float, dict[int, float], float]:
        scores = [a.scores[method] for a in alerts_set]
        labels = [proxy_fn(a) for a in alerts_set]
        auroc_val, auroc_lo, auroc_hi = _bootstrap_auroc_ci(scores, labels)
        sep = _separation(scores, labels)
        gini_val = _gini_concentration(scores, labels)
        prec_k = {k: _precision_at_k(scores, labels, k) for k in TOP_K_VALUES}
        return auroc_val, auroc_lo, auroc_hi, sep, prec_k, gini_val

    # Pre-extract M0 scores for Wilcoxon tests
    m0_scores_all = [a.scores["M0"] for a in enriched]

    for mi, method in enumerate(method_names):
        print(f"  [{mi+1}/{len(method_names)}] {method}...", end="", flush=True)
        mr = MethodResult(method=method)
        method_scores_all = [a.scores[method] for a in enriched]

        # Volume
        mr.volume_075 = _volume_above_threshold(method_scores_all)

        for proxy_label, proxy_short, proxy_fn in proxy_names:
            # Full set metrics
            scores_full = method_scores_all
            labels_full = [proxy_fn(a) for a in enriched]
            auroc_val, auroc_lo, auroc_hi = _bootstrap_auroc_ci(scores_full, labels_full)
            mr.auroc[proxy_label] = auroc_val
            mr.auroc_ci_lo[proxy_label] = auroc_lo
            mr.auroc_ci_hi[proxy_label] = auroc_hi
            mr.separation[proxy_label] = _separation(scores_full, labels_full)
            mr.gini[proxy_label] = _gini_concentration(scores_full, labels_full)
            mr.precision_at_k[proxy_label] = {
                k: _precision_at_k(scores_full, labels_full, k) for k in TOP_K_VALUES
            }

            # Holdout
            scores_ho = [a.scores[method] for a in holdout_alerts]
            labels_ho = [proxy_fn(a) for a in holdout_alerts]
            mr.auroc_holdout[proxy_label] = _auroc(scores_ho, labels_ho)

            # Temporal
            if pre2020:
                scores_pre = [a.scores[method] for a in pre2020]
                labels_pre = [proxy_fn(a) for a in pre2020]
                mr.auroc_pre[proxy_label] = _auroc(scores_pre, labels_pre)
            if post2020:
                scores_post = [a.scores[method] for a in post2020]
                labels_post = [proxy_fn(a) for a in post2020]
                mr.auroc_post[proxy_label] = _auroc(scores_post, labels_post)

        # Wilcoxon vs M0 (proxy-independent)
        if method != "M0":
            diffs = [a.scores[method] - a.scores["M0"] for a in enriched]
            _, p_val = _wilcoxon_signed_rank(diffs)
            mr.wilcoxon_p["all"] = p_val

        results[method] = mr
        print(f" ok ({time.time() - t4:.0f}s)", flush=True)

    print(f"  Avaliação concluída em {time.time() - t4:.1f}s")

    # ===== PHASE 5: Output =====
    print("\n" + "=" * 70)
    print("FASE 5: Resultados")
    print("=" * 70)

    # --- Main comparison table (proxy A) ---
    print("\n╔══════════════════════════════════════════════════════════════════════════════════╗")
    print(f"║{'COMPARAÇÃO DE MÉTODOS DE SCORING':^82}║")
    print(f"║{f'{len(enriched)} alertas, Proxy A (compound risk ≥{PROXY_COMPOUND_MIN_SIGNALS} signals)':^82}║")
    print("╠═══════════════════╦════════╦════════╦═════════╦═════════╦═════════╦════════════╣")
    print("║ Método            ║ AUROC  ║ Sep.   ║ P@100   ║ P@1000  ║ Gini    ║ Vol≥0.75   ║")
    print("╠═══════════════════╬════════╬════════╬═════════╬═════════╬═════════╬════════════╣")

    for method in method_names:
        mr = results[method]
        label = f"{method} {method_labels[method]}"[:17]
        auroc = mr.auroc.get("A", 0.5)
        sep = mr.separation.get("A", 0.0)
        p100 = mr.precision_at_k.get("A", {}).get(100, 0.0)
        p1000 = mr.precision_at_k.get("A", {}).get(1000, 0.0)
        gini = mr.gini.get("A", 0.0)
        vol = mr.volume_075
        print(f"║ {label:<17} ║ {auroc:.4f} ║ {sep:+.4f} ║ {p100:.5f} ║ {p1000:.5f} ║ {gini:.5f} ║ {vol:>10} ║")

    print("╚═══════════════════╩════════╩════════╩═════════╩═════════╩═════════╩════════════╝")

    # --- Proxy B table ---
    print(f"\n{'─' * 82}")
    print(f"  Proxy B: Rapporteur Deviation ({proxy_b_count} positivos)")
    print(f"{'─' * 82}")
    print(f"  {'Método':<20} {'AUROC':>7} {'Sep.':>8} {'P@100':>8} {'P@1000':>8} {'Gini':>8}")
    for method in method_names:
        mr = results[method]
        label = f"{method} {method_labels[method]}"[:20]
        auroc = mr.auroc.get("B", 0.5)
        sep = mr.separation.get("B", 0.0)
        p100 = mr.precision_at_k.get("B", {}).get(100, 0.0)
        p1000 = mr.precision_at_k.get("B", {}).get(1000, 0.0)
        gini = mr.gini.get("B", 0.0)
        print(f"  {label:<20} {auroc:>7.4f} {sep:>+8.4f} {p100:>8.5f} {p1000:>8.5f} {gini:>8.5f}")

    # --- Proxy C table ---
    print(f"\n{'─' * 82}")
    print(f"  Proxy C: ML Agreement (ml_rarity > {ML_RARITY_THRESHOLD}, {proxy_c_count} positivos)")
    print(f"{'─' * 82}")
    print(f"  {'Método':<20} {'AUROC':>7} {'Sep.':>8} {'P@100':>8} {'P@1000':>8} {'Gini':>8}")
    for method in method_names:
        mr = results[method]
        label = f"{method} {method_labels[method]}"[:20]
        auroc = mr.auroc.get("C", 0.5)
        sep = mr.separation.get("C", 0.0)
        p100 = mr.precision_at_k.get("C", {}).get(100, 0.0)
        p1000 = mr.precision_at_k.get("C", {}).get(1000, 0.0)
        gini = mr.gini.get("C", 0.0)
        print(f"  {label:<20} {auroc:>7.4f} {sep:>+8.4f} {p100:>8.5f} {p1000:>8.5f} {gini:>8.5f}")

    # --- Wilcoxon signed-rank tests (M0 vs each) ---
    print(f"\n{'─' * 82}")
    print("  Wilcoxon signed-rank (M0 vs método, Bonferroni α = {:.4f})".format(BONFERRONI_ALPHA))
    print(f"{'─' * 82}")
    print(f"  {'Método':<20} {'p-value':>12} {'Significante?':>15}")
    for method in method_names:
        if method == "M0":
            continue
        mr = results[method]
        p = mr.wilcoxon_p.get("all", 1.0)
        sig = "SIM***" if p < BONFERRONI_ALPHA else "não"
        print(f"  {method:<20} {p:>12.2e} {sig:>15}")

    # --- Bootstrap CI for AUROC (Proxy A) ---
    print(f"\n{'─' * 82}")
    print("  Bootstrap 95% CI — AUROC Proxy A")
    print(f"{'─' * 82}")
    print(f"  {'Método':<20} {'AUROC':>7} {'[lo':>7} {'hi]':>7}")
    for method in method_names:
        mr = results[method]
        auroc = mr.auroc.get("A", 0.5)
        lo = mr.auroc_ci_lo.get("A", 0.5)
        hi = mr.auroc_ci_hi.get("A", 0.5)
        label = f"{method} {method_labels[method]}"[:20]
        print(f"  {label:<20} {auroc:>7.4f} [{lo:>6.4f}  {hi:>6.4f}]")

    # --- Anti-overfitting: holdout AUROC ---
    print(f"\n{'─' * 82}")
    print("  Anti-overfitting: AUROC no holdout (50% dados)")
    print(f"{'─' * 82}")
    print(f"  {'Método':<20} {'Proxy A':>8} {'Proxy B':>8} {'Proxy C':>8}")
    for method in method_names:
        mr = results[method]
        a = mr.auroc_holdout.get("A", 0.5)
        b = mr.auroc_holdout.get("B", 0.5)
        c = mr.auroc_holdout.get("C", 0.5)
        label = f"{method} {method_labels[method]}"[:20]
        print(f"  {label:<20} {a:>8.4f} {b:>8.4f} {c:>8.4f}")

    # --- Temporal stability ---
    print(f"\n{'─' * 82}")
    print(f"  Estabilidade temporal: AUROC Proxy A (pre-2020 n={len(pre2020)} vs post-2020 n={len(post2020)})")
    print(f"{'─' * 82}")
    print(f"  {'Método':<20} {'Pre-2020':>9} {'Post-2020':>10} {'Delta':>8}")
    for method in method_names:
        mr = results[method]
        pre = mr.auroc_pre.get("A", 0.5)
        post = mr.auroc_post.get("A", 0.5)
        delta = post - pre
        label = f"{method} {method_labels[method]}"[:20]
        print(f"  {label:<20} {pre:>9.4f} {post:>10.4f} {delta:>+8.4f}")

    # --- Marginal value of additional signals ---
    print(f"\n{'─' * 82}")
    print("  Valor marginal dos sinais adicionais (M8–M12 vs M1)")
    print(f"{'─' * 82}")
    m1_auroc_a = results["M1"].auroc.get("A", 0.5)
    for method in ["M8", "M9", "M10", "M11", "M12"]:
        mr = results[method]
        auroc_a = mr.auroc.get("A", 0.5)
        delta_auroc = auroc_a - m1_auroc_a
        sep_a = mr.separation.get("A", 0.0)
        m1_sep_a = results["M1"].separation.get("A", 0.0)
        delta_sep = sep_a - m1_sep_a
        label = f"{method} {method_labels[method]}"
        print(f"  {label:<25} ΔAUROC={delta_auroc:+.4f}  ΔSep={delta_sep:+.4f}")

    # --- Recommendation ---
    print(f"\n{'=' * 82}")
    print("  RECOMENDAÇÃO")
    print(f"{'=' * 82}")

    # Find best method by AUROC Proxy A (holdout)
    best_method = max(method_names, key=lambda m: results[m].auroc_holdout.get("A", 0.0))
    best_mr = results[best_method]
    best_auroc = best_mr.auroc_holdout.get("A", 0.5)
    best_ci_lo = best_mr.auroc_ci_lo.get("A", 0.5)
    best_ci_hi = best_mr.auroc_ci_hi.get("A", 0.5)
    m0_auroc = results["M0"].auroc_holdout.get("A", 0.5)

    print(f"\n  Melhor método (AUROC holdout, Proxy A): {best_method} ({method_labels[best_method]})")
    print(f"  AUROC holdout: {best_auroc:.4f} (IC 95%: [{best_ci_lo:.4f}, {best_ci_hi:.4f}])")
    print(f"  Melhoria vs M0 atual: {best_auroc - m0_auroc:+.4f}")
    print(f"  Volume ≥0.75:  M0={results['M0'].volume_075}  →  {best_method}={best_mr.volume_075}")

    # Threshold calibration for best method
    best_scores = sorted([a.scores[best_method] for a in enriched], reverse=True)
    best_labels = [a.proxy_compound for a in enriched]
    # Find threshold that maximizes F1-like: precision * recall balance
    print(f"\n  Calibração de threshold para {best_method}:")
    for threshold in [0.90, 0.85, 0.80, 0.75, 0.70, 0.65, 0.60]:
        above = [(a.scores[best_method], a.proxy_compound) for a in enriched if a.scores[best_method] >= threshold]
        if not above:
            continue
        n_above = len(above)
        n_pos_above = sum(1 for _, l in above if l)
        total_pos = sum(1 for a in enriched if a.proxy_compound)
        precision = n_pos_above / n_above if n_above > 0 else 0
        recall = n_pos_above / total_pos if total_pos > 0 else 0
        print(f"    threshold={threshold:.2f}: n={n_above:>5}, precision={precision:.4f}, recall={recall:.4f}")

    # --- Dimension analysis ---
    print(f"\n{'─' * 82}")
    print("  Análise das dimensões (distribuição de probabilidades)")
    print(f"{'─' * 82}")
    dim_probs: dict[str, list[float]] = defaultdict(list)
    for ea in enriched:
        for d in ea.dimensions:
            dim_probs[d.name].append(d.probability)

    for name, probs in sorted(dim_probs.items()):
        n = len(probs)
        mean_p = sum(probs) / n
        gt04 = sum(1 for p in probs if p > 0.4)
        gt035 = sum(1 for p in probs if p > 0.35)
        median_p = sorted(probs)[n // 2]
        print(f"  {name:<25} n={n:>6}  mean_prob={mean_p:.4f}  median={median_p:.4f}  "
              f"prob>0.35={gt035/n*100:.1f}%  prob>0.40={gt04/n*100:.1f}%")

    total_time = time.time() - t0
    print(f"\n{'=' * 82}")
    print(f"  Tempo total: {total_time:.1f}s")
    print(f"{'=' * 82}")


if __name__ == "__main__":
    main()
