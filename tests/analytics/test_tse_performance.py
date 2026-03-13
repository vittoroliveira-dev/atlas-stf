"""Benchmark tests for TSE donation matching performance.

These tests measure the impact of 3 candidate optimizations:
1. Canonical dedup — group donors by canonical name, match once per group
2. Token frequency cap — exclude high-frequency tokens from by_token index
3. Fast-path hit rate — measure how many donors resolve without fuzzy matching

Run with: uv run pytest tests/analytics/test_tse_performance.py -m slow -v -s --no-header
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from atlas_stf.analytics._match_helpers import (
    EntityMatchIndex,
    build_entity_match_index,
    iter_jsonl,
    match_entity_record,
    read_jsonl,
)
from atlas_stf.core.identity import (
    canonicalize_entity_name,
    normalize_tax_id,
)

logger = logging.getLogger(__name__)

DONATIONS_PATH = Path("data/raw/tse/donations_raw.jsonl")
PARTY_PATH = Path("data/curated/party.jsonl")
COUNSEL_PATH = Path("data/curated/counsel.jsonl")
DATA_EXISTS = DONATIONS_PATH.exists() and PARTY_PATH.exists() and COUNSEL_PATH.exists()

STREAM_LIMIT = 500_000
DEDUP_SAMPLE = 10_000  # test 1: canonical grouping (no matching, fast)
MATCH_SAMPLE = 500  # tests 3-6: actual match_entity_record calls (slow due to fuzzy)
FUZZY_TIMING_SAMPLE = 100  # test 6: timing of fuzzy-path donors


def _sample_donors() -> dict[str, str | None]:
    """Stream STREAM_LIMIT lines from donations_raw.jsonl, aggregate by donor_name_normalized.

    Returns dict[donor_name_normalized, cpf_cnpj] (first seen tax_id per name).
    """
    donors: dict[str, str | None] = {}
    for i, record in enumerate(iter_jsonl(DONATIONS_PATH)):
        if i >= STREAM_LIMIT:
            break
        name = record.get("donor_name_normalized")
        if not name:
            continue
        if name not in donors:
            donors[name] = record.get("cpf_cnpj")
    return donors


def _build_party_index() -> EntityMatchIndex:
    """Build EntityMatchIndex from party.jsonl."""
    records = read_jsonl(PARTY_PATH)
    return build_entity_match_index(records, name_field="party_name_normalized")


@pytest.mark.slow
@pytest.mark.skipif(not DATA_EXISTS, reason="Real data files not available")
class TestCanonicalDedupRatio:
    """Test 1: Measure dedup ratio when donors are grouped by canonical name."""

    def test_canonical_dedup_ratio(self) -> None:
        donors = _sample_donors()
        unique_donors = len(donors)

        # Group by canonical name
        canonical_groups: dict[str, list[str]] = {}
        for name in donors:
            canonical = canonicalize_entity_name(name)
            key = canonical or name
            canonical_groups.setdefault(key, []).append(name)

        unique_canonical = len(canonical_groups)
        dedup_ratio = unique_donors / unique_canonical if unique_canonical > 0 else 1.0

        # Separate by tax_id availability
        with_tax_id = sum(1 for name, tid in donors.items() if normalize_tax_id(tid))
        without_tax_id = unique_donors - with_tax_id

        # Group size distribution
        sizes = [len(group) for group in canonical_groups.values()]
        size_dist = Counter(sizes)

        # Top 10 largest groups
        top_groups = sorted(canonical_groups.items(), key=lambda x: len(x[1]), reverse=True)[:10]

        logger.info("=" * 70)
        logger.info("BENCHMARK: Canonical Dedup Ratio")
        logger.info("=" * 70)
        logger.info("unique_donors:    %d", unique_donors)
        logger.info("unique_canonical:  %d", unique_canonical)
        logger.info("dedup_ratio:       %.2fx", dedup_ratio)
        logger.info("with_tax_id:       %d (%.1f%%)", with_tax_id, 100 * with_tax_id / unique_donors)
        logger.info("without_tax_id:    %d (%.1f%%)", without_tax_id, 100 * without_tax_id / unique_donors)
        logger.info("--- Group size distribution ---")
        for size in sorted(size_dist.keys()):
            logger.info("  size=%d: %d groups", size, size_dist[size])
        logger.info("--- Top 10 canonical groups ---")
        for canonical, members in top_groups:
            logger.info("  [%d members] %s", len(members), canonical)
        logger.info("=" * 70)

        assert unique_canonical <= unique_donors
        assert dedup_ratio >= 1.0


@pytest.mark.slow
@pytest.mark.skipif(not DATA_EXISTS, reason="Real data files not available")
class TestTokenFrequencyDistribution:
    """Test 2: Measure token frequency distribution in the party index."""

    def test_token_frequency_distribution(self) -> None:
        index = _build_party_index()

        # Token frequency: token -> number of records sharing that token
        token_freqs: list[tuple[str, int]] = [(token, len(indices)) for token, indices in index.by_token.items()]
        token_freqs.sort(key=lambda x: x[1], reverse=True)

        freqs_only = [f for _, f in token_freqs]
        freqs_only.sort()

        n = len(freqs_only)
        p50 = freqs_only[n // 2] if n else 0
        p90 = freqs_only[int(n * 0.9)] if n else 0
        p95 = freqs_only[int(n * 0.95)] if n else 0
        p99 = freqs_only[int(n * 0.99)] if n else 0

        above_100 = sum(1 for f in freqs_only if f > 100)
        above_500 = sum(1 for f in freqs_only if f > 500)
        above_1000 = sum(1 for f in freqs_only if f > 1000)
        above_5000 = sum(1 for f in freqs_only if f > 5000)

        logger.info("=" * 70)
        logger.info("BENCHMARK: Token Frequency Distribution (Party Index)")
        logger.info("=" * 70)
        logger.info("total_tokens:      %d", n)
        logger.info("total_records:     %d", len(index.records))
        logger.info("--- Percentiles ---")
        logger.info("  p50: %d", p50)
        logger.info("  p90: %d", p90)
        logger.info("  p95: %d", p95)
        logger.info("  p99: %d", p99)
        logger.info("--- Tokens above thresholds ---")
        logger.info("  >100:  %d", above_100)
        logger.info("  >500:  %d", above_500)
        logger.info("  >1000: %d", above_1000)
        logger.info("  >5000: %d", above_5000)
        logger.info("--- Top 30 tokens ---")
        for token, freq in token_freqs[:30]:
            logger.info("  %-20s %d", token, freq)
        logger.info("=" * 70)

        assert len(index.by_token) > 0


@pytest.mark.slow
@pytest.mark.skipif(not DATA_EXISTS, reason="Real data files not available")
class TestExactMatchHitRate:
    """Test 3: Measure % of donors that resolve via fast-path (no fuzzy)."""

    def test_exact_match_hit_rate(self) -> None:
        donors = _sample_donors()
        index = _build_party_index()

        sample = list(donors.items())[:MATCH_SAMPLE]
        sample_size = len(sample)

        strategy_counts: Counter[str] = Counter()
        total_time = 0.0

        for name, tax_id in sample:
            t0 = time.perf_counter()
            result = match_entity_record(
                query_name=name,
                query_tax_id=tax_id,
                index=index,
                name_field="party_name_normalized",
            )
            total_time += time.perf_counter() - t0

            if result is None:
                strategy_counts["no_match"] += 1
            else:
                strategy_counts[result.strategy] += 1

        fast_path_strategies = {"tax_id", "alias", "exact", "canonical_name"}
        fuzzy_strategies = {"jaccard", "levenshtein", "ambiguous"}

        fast_path_count = sum(strategy_counts[s] for s in fast_path_strategies)
        fuzzy_count = sum(strategy_counts[s] for s in fuzzy_strategies)
        no_match_count = strategy_counts["no_match"]

        fast_path_rate = fast_path_count / sample_size if sample_size else 0
        fuzzy_rate = fuzzy_count / sample_size if sample_size else 0
        no_match_rate = no_match_count / sample_size if sample_size else 0
        avg_time_us = (total_time / sample_size) * 1_000_000 if sample_size else 0

        logger.info("=" * 70)
        logger.info("BENCHMARK: Exact Match Hit Rate")
        logger.info("=" * 70)
        logger.info("sample_size:       %d", sample_size)
        logger.info("--- Strategy distribution ---")
        for strategy, count in strategy_counts.most_common():
            logger.info("  %-20s %d (%.1f%%)", strategy, count, 100 * count / sample_size)
        logger.info("--- Rates ---")
        logger.info("  fast_path_rate:  %.1f%%", 100 * fast_path_rate)
        logger.info("  fuzzy_rate:      %.1f%%", 100 * fuzzy_rate)
        logger.info("  no_match_rate:   %.1f%%", 100 * no_match_rate)
        logger.info("--- Timing ---")
        logger.info("  avg_time_per_query: %.1f us", avg_time_us)
        logger.info("  total_time:         %.2f s", total_time)
        logger.info("=" * 70)

        assert sum(strategy_counts.values()) == sample_size


@pytest.mark.slow
@pytest.mark.skipif(not DATA_EXISTS, reason="Real data files not available")
class TestCanonicalDedupQuality:
    """Test 4: Check if canonical dedup is lossless — same canonical -> same match?"""

    def test_canonical_dedup_quality(self) -> None:
        donors = _sample_donors()
        index = _build_party_index()

        # Group by canonical name
        canonical_groups: dict[str, list[tuple[str, str | None]]] = {}
        for name, tax_id in donors.items():
            canonical = canonicalize_entity_name(name)
            key = canonical or name
            canonical_groups.setdefault(key, []).append((name, tax_id))

        # Select groups with size >= 2, limit to 1000
        multi_groups = [(k, v) for k, v in canonical_groups.items() if len(v) >= 2][:1000]

        fully_consistent = 0
        inconsistent = 0
        mismatch_causes: Counter[str] = Counter()

        for _canonical, members in multi_groups:
            results: list[tuple[str | None, str | None]] = []
            for name, tax_id in members:
                result = match_entity_record(
                    query_name=name,
                    query_tax_id=tax_id,
                    index=index,
                    name_field="party_name_normalized",
                )
                if result is None:
                    results.append((None, None))
                else:
                    entity_id = result.record.get("party_id") or result.record.get("entity_id")
                    results.append((result.strategy, entity_id))

            # Check consistency: all members should produce the same (strategy, entity_id)
            unique_results = set(results)
            if len(unique_results) == 1:
                fully_consistent += 1
            else:
                inconsistent += 1
                strategies = {r[0] for r in results}
                entity_ids = {r[1] for r in results}
                if None in entity_ids and len(entity_ids) > 1:
                    mismatch_causes["match_vs_none"] += 1
                elif len(strategies) > 1:
                    mismatch_causes["different_strategy"] += 1
                elif len(entity_ids) > 1:
                    mismatch_causes["different_entity"] += 1
                else:
                    mismatch_causes["other"] += 1

        groups_tested = len(multi_groups)
        consistency_rate = fully_consistent / groups_tested if groups_tested else 0

        logger.info("=" * 70)
        logger.info("BENCHMARK: Canonical Dedup Quality")
        logger.info("=" * 70)
        logger.info("groups_tested:     %d", groups_tested)
        logger.info("fully_consistent:  %d (%.1f%%)", fully_consistent, 100 * consistency_rate)
        logger.info("inconsistent:      %d (%.1f%%)", inconsistent, 100 * (1 - consistency_rate))
        logger.info("--- Mismatch causes ---")
        for cause, count in mismatch_causes.most_common():
            logger.info("  %-25s %d", cause, count)
        logger.info("=" * 70)

        assert groups_tested > 0


@pytest.mark.slow
@pytest.mark.skipif(not DATA_EXISTS, reason="Real data files not available")
class TestTokenCapQuality:
    """Test 5: Measure quality loss when high-frequency tokens are excluded."""

    def test_token_cap_quality(self) -> None:
        donors = _sample_donors()
        index = _build_party_index()

        sample = list(donors.items())[:MATCH_SAMPLE]
        sample_size = len(sample)

        # Baseline: match all donors against original index
        baseline_results: list[tuple[str | None, str | None]] = []
        for name, tax_id in sample:
            result = match_entity_record(
                query_name=name,
                query_tax_id=tax_id,
                index=index,
                name_field="party_name_normalized",
            )
            if result is None:
                baseline_results.append((None, None))
            else:
                entity_id = result.record.get("party_id") or result.record.get("entity_id")
                baseline_results.append((result.strategy, entity_id))

        caps = [100, 500, 1000, 5000]

        logger.info("=" * 70)
        logger.info("BENCHMARK: Token Cap Quality")
        logger.info("=" * 70)
        logger.info("sample_size:       %d", sample_size)
        logger.info("baseline_tokens:   %d", len(index.by_token))

        for cap in caps:
            # Filter by_token: exclude tokens with too many records
            by_token_capped = {token: indices for token, indices in index.by_token.items() if len(indices) <= cap}
            tokens_excluded = len(index.by_token) - len(by_token_capped)

            # Build capped index (frozen dataclass → use replace)
            capped_index = replace(index, by_token=by_token_capped)

            preserved = 0
            lost = 0
            degraded = 0
            no_match_both = 0

            for i, (name, tax_id) in enumerate(sample):
                result = match_entity_record(
                    query_name=name,
                    query_tax_id=tax_id,
                    index=capped_index,
                    name_field="party_name_normalized",
                )

                baseline_strategy, baseline_entity = baseline_results[i]

                if result is None:
                    capped_pair = (None, None)
                else:
                    entity_id = result.record.get("party_id") or result.record.get("entity_id")
                    capped_pair = (result.strategy, entity_id)

                if baseline_strategy is None and capped_pair[0] is None:
                    no_match_both += 1
                elif baseline_entity == capped_pair[1] and baseline_entity is not None:
                    preserved += 1
                elif baseline_entity is not None and capped_pair[1] is None:
                    lost += 1
                else:
                    degraded += 1

            quality_retention = preserved / (preserved + lost + degraded) if (preserved + lost + degraded) > 0 else 1.0

            logger.info("--- cap=%d ---", cap)
            logger.info("  tokens_excluded: %d", tokens_excluded)
            logger.info("  preserved:       %d", preserved)
            logger.info("  lost:            %d", lost)
            logger.info("  degraded:        %d", degraded)
            logger.info("  no_match_both:   %d", no_match_both)
            logger.info("  quality_retention: %.1f%%", 100 * quality_retention)

            assert preserved + lost + degraded + no_match_both == sample_size

        logger.info("=" * 70)


@pytest.mark.slow
@pytest.mark.skipif(not DATA_EXISTS, reason="Real data files not available")
class TestCombinedSpeedupEstimate:
    """Test 6: Estimate total time with combined optimizations."""

    def test_combined_speedup_estimate(self) -> None:
        donors = _sample_donors()
        index = _build_party_index()
        unique_donors = len(donors)

        # Group by canonical name for dedup stats
        canonical_groups: dict[str, list[tuple[str, str | None]]] = {}
        for name, tax_id in donors.items():
            canonical = canonicalize_entity_name(name)
            key = canonical or name
            canonical_groups.setdefault(key, []).append((name, tax_id))
        unique_canonical = len(canonical_groups)
        dedup_factor = unique_donors / unique_canonical if unique_canonical > 0 else 1.0

        # Classify donors into fast-path vs fuzzy
        fast_donors: list[tuple[str, str | None]] = []
        fuzzy_donors: list[tuple[str, str | None]] = []

        fast_path_strategies = {"tax_id", "alias", "exact", "canonical_name"}

        for name, tax_id in list(donors.items())[:MATCH_SAMPLE]:
            result = match_entity_record(
                query_name=name,
                query_tax_id=tax_id,
                index=index,
                name_field="party_name_normalized",
            )
            if result is not None and result.strategy in fast_path_strategies:
                fast_donors.append((name, tax_id))
            else:
                fuzzy_donors.append((name, tax_id))

        total_classified = len(fast_donors) + len(fuzzy_donors)
        fast_rate = len(fast_donors) / total_classified if total_classified else 0
        fuzzy_rate = 1.0 - fast_rate

        # Time fast-path donors
        fast_sample = fast_donors[:FUZZY_TIMING_SAMPLE]
        t0 = time.perf_counter()
        for name, tax_id in fast_sample:
            match_entity_record(query_name=name, query_tax_id=tax_id, index=index, name_field="party_name_normalized")
        fast_total = time.perf_counter() - t0
        avg_fast_us = (fast_total / len(fast_sample)) * 1_000_000 if fast_sample else 0

        # Time fuzzy donors
        fuzzy_sample = fuzzy_donors[:FUZZY_TIMING_SAMPLE]
        t0 = time.perf_counter()
        for name, tax_id in fuzzy_sample:
            match_entity_record(query_name=name, query_tax_id=tax_id, index=index, name_field="party_name_normalized")
        fuzzy_total = time.perf_counter() - t0
        avg_fuzzy_us = (fuzzy_total / len(fuzzy_sample)) * 1_000_000 if fuzzy_sample else 0

        # Average over all
        avg_all_us = (fast_rate * avg_fast_us + fuzzy_rate * avg_fuzzy_us) if total_classified else 0

        # Estimates for 4.28M donors
        total_donors = 4_280_000

        est_current_s = total_donors * avg_all_us / 1_000_000
        est_current_h = est_current_s / 3600

        deduped_donors = total_donors / dedup_factor
        est_dedup_s = deduped_donors * avg_all_us / 1_000_000
        est_dedup_h = est_dedup_s / 3600

        est_combined_s = deduped_donors * (fast_rate * avg_fast_us + fuzzy_rate * avg_fuzzy_us) / 1_000_000
        est_combined_h = est_combined_s / 3600

        speedup_dedup = est_current_h / est_dedup_h if est_dedup_h > 0 else 0
        speedup_combined = est_current_h / est_combined_h if est_combined_h > 0 else 0

        logger.info("=" * 70)
        logger.info("BENCHMARK: Combined Speedup Estimate")
        logger.info("=" * 70)
        logger.info("--- Sample stats ---")
        logger.info("  unique_donors (sample):    %d", unique_donors)
        logger.info("  unique_canonical (sample):  %d", unique_canonical)
        logger.info("  dedup_factor:               %.2fx", dedup_factor)
        logger.info("  fast_rate:                  %.1f%%", 100 * fast_rate)
        logger.info("  fuzzy_rate:                 %.1f%%", 100 * fuzzy_rate)
        logger.info("--- Timing (us/query) ---")
        logger.info("  avg_fast:  %.1f us", avg_fast_us)
        logger.info("  avg_fuzzy: %.1f us", avg_fuzzy_us)
        logger.info("  avg_all:   %.1f us", avg_all_us)
        logger.info("--- Estimates for %dM donors ---", total_donors // 1_000_000)
        logger.info("  CURRENT:       %.1f h", est_current_h)
        logger.info("  DEDUP ONLY:    %.1f h  (%.1fx speedup)", est_dedup_h, speedup_dedup)
        logger.info("  DEDUP+FAST:    %.1f h  (%.1fx speedup)", est_combined_h, speedup_combined)
        logger.info("=" * 70)

        if fast_sample and fuzzy_sample:
            assert avg_fuzzy_us > avg_fast_us, f"Expected fuzzy ({avg_fuzzy_us:.1f}us) > fast ({avg_fast_us:.1f}us)"
        assert est_combined_s <= est_current_s or est_current_s == 0
