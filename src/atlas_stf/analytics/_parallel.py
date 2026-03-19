"""Parallel matching helpers — auto-detects CPU cores and distributes work.

Uses fork-based multiprocessing to avoid serializing the large EntityMatchIndex.
Workers inherit the parent's memory via copy-on-write (Linux), so the 7GB index
is shared without copying.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import threading
from typing import Any

from ._match_helpers import (
    EntityMatchIndex,
    EntityMatchResult,
    MatchThresholds,
    compute_favorable_rate_role_aware,
    match_entity_record,
)

logger = logging.getLogger(__name__)

# Module-level state shared with forked workers (copy-on-write).
_worker_index: EntityMatchIndex | None = None
_worker_name_field: str = ""
_worker_thresholds: MatchThresholds | None = None
_worker_state_lock = threading.Lock()

# Module-level state for counsel profile workers.
_cp_process_party_map: dict[str, list[tuple[str, str | None]]] = {}
_cp_process_outcomes: dict[str, list[str]] = {}
_cp_matched_names: set[str] = set()
_cp_counsel_index: dict[str, dict[str, Any]] = {}
_cp_red_flag_delta: float = 0.15
_cp_min_cases: int = 3
_cp_state_lock = threading.Lock()


def _match_one(item: tuple[str, str | None]) -> tuple[str, dict[str, Any] | None]:
    """Match a single (normalized_name, tax_id) against the shared index.

    Returns a plain dict instead of EntityMatchResult to avoid pickle issues
    when sending results back from workers.
    """
    norm_name, tax_id = item
    assert _worker_index is not None  # noqa: S101
    result = match_entity_record(
        query_name=norm_name,
        query_tax_id=tax_id,
        index=_worker_index,
        name_field=_worker_name_field,
        thresholds=_worker_thresholds,
    )
    if result is None:
        return norm_name, None
    return norm_name, {
        "record": result.record,
        "strategy": result.strategy,
        "score": result.score,
        "matched_alias": result.matched_alias,
        "matched_tax_id": result.matched_tax_id,
        "uncertainty_note": result.uncertainty_note,
        "candidate_count": result.candidate_count,
    }


def _result_from_dict(data: dict[str, Any]) -> EntityMatchResult:
    """Reconstruct EntityMatchResult from a plain dict."""
    return EntityMatchResult(
        record=data["record"],
        strategy=data["strategy"],
        score=data["score"],
        matched_alias=data["matched_alias"],
        matched_tax_id=data["matched_tax_id"],
        uncertainty_note=data["uncertainty_note"],
        candidate_count=data.get("candidate_count"),
    )


_MAX_WORKERS_CAP = 2
_MIN_RAM_FOR_FORK_GB = 8.0


def optimal_workers(ram_per_worker_gb: float = 1.5) -> int:
    """Return optimal worker count based on CPU cores and available RAM.

    Capped at ``_MAX_WORKERS_CAP`` and requires at least
    ``_MIN_RAM_FOR_FORK_GB`` free before spawning any workers, to avoid
    OOM on shared machines where fork-based workers inherit the parent's
    RSS via copy-on-write.
    """
    cores = os.cpu_count() or 1
    cpu_limit = max(1, cores - 2)

    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    available_gb = int(line.split()[1]) / (1024 * 1024)
                    if available_gb < _MIN_RAM_FOR_FORK_GB:
                        logger.info(
                            "Auto-detected: %.1f GB available RAM < %.1f GB minimum → 1 worker (sequential)",
                            available_gb,
                            _MIN_RAM_FOR_FORK_GB,
                        )
                        return 1
                    ram_limit = max(1, int(available_gb / ram_per_worker_gb))
                    result = min(cpu_limit, ram_limit, _MAX_WORKERS_CAP)
                    logger.info(
                        "Auto-detected: %d cores, %.1f GB available RAM → %d workers (cap=%d)",
                        cores,
                        available_gb,
                        result,
                        _MAX_WORKERS_CAP,
                    )
                    return result
    except OSError:
        pass

    return min(cpu_limit, _MAX_WORKERS_CAP)


def _clear_match_state() -> None:
    global _worker_index, _worker_name_field, _worker_thresholds  # noqa: PLW0603
    _worker_index = None
    _worker_name_field = ""
    _worker_thresholds = None


def _clear_counsel_profile_state() -> None:
    global _cp_process_party_map, _cp_process_outcomes  # noqa: PLW0603
    global _cp_matched_names, _cp_counsel_index  # noqa: PLW0603
    global _cp_red_flag_delta, _cp_min_cases  # noqa: PLW0603
    _cp_process_party_map = {}
    _cp_process_outcomes = {}
    _cp_matched_names = set()
    _cp_counsel_index = {}
    _cp_red_flag_delta = 0.15
    _cp_min_cases = 3


def match_entities_parallel(
    items: list[tuple[str, str | None]],
    *,
    index: EntityMatchIndex,
    name_field: str,
    max_workers: int | None = None,
    thresholds: MatchThresholds | None = None,
) -> dict[str, EntityMatchResult | None]:
    """Match a list of (name, tax_id) pairs against an index using multiple cores.

    Uses fork-based multiprocessing so the index is shared via copy-on-write
    instead of being serialized (which would OOM for large indices).

    Falls back to sequential processing if there are fewer than 100 items
    or only 1 CPU core is available.
    """
    global _worker_index, _worker_name_field, _worker_thresholds  # noqa: PLW0603

    workers = max_workers or optimal_workers()

    if workers <= 1 or len(items) < 100:
        logger.info("Matching %d entities sequentially (workers=%d)", len(items), workers)
        results: dict[str, EntityMatchResult | None] = {}
        for norm_name, tax_id in items:
            result = match_entity_record(
                query_name=norm_name,
                query_tax_id=tax_id,
                index=index,
                name_field=name_field,
                thresholds=thresholds,
            )
            results[norm_name] = result
        return results

    logger.info("Matching %d entities in parallel with %d workers", len(items), workers)

    chunksize = max(1, len(items) // (workers * 4))
    ctx = multiprocessing.get_context("fork")

    with _worker_state_lock:
        # Set module-level state BEFORE forking so workers inherit it.
        # The "fork" start method copies the parent's memory to children (COW on Linux),
        # so the index is shared without serialization. Thresholds are set here for
        # the same reason — workers inherit the module-level state after fork.
        _worker_index = index
        _worker_name_field = name_field
        _worker_thresholds = thresholds

        results: dict[str, EntityMatchResult | None] = {}
        try:
            with ctx.Pool(processes=workers) as pool:
                for norm_name, match_dict in pool.imap_unordered(_match_one, items, chunksize=chunksize):
                    if match_dict is None:
                        results[norm_name] = None
                    else:
                        results[norm_name] = _result_from_dict(match_dict)
        finally:
            _clear_match_state()

    return results


def _counsel_profile_one(
    item: tuple[str, set[str]],
) -> dict[str, Any] | None:
    """Compute a single counsel profile against shared data."""
    return _compute_counsel_profile(
        item,
        process_party_map=_cp_process_party_map,
        process_outcomes=_cp_process_outcomes,
        matched_names=_cp_matched_names,
        counsel_index=_cp_counsel_index,
        red_flag_delta=_cp_red_flag_delta,
        min_cases=_cp_min_cases,
    )


def _compute_counsel_profile(
    item: tuple[str, set[str]],
    *,
    process_party_map: dict[str, list[tuple[str, str | None]]],
    process_outcomes: dict[str, list[str]],
    matched_names: set[str],
    counsel_index: dict[str, dict[str, Any]],
    red_flag_delta: float,
    min_cases: int,
) -> dict[str, Any] | None:
    """Compute a single counsel profile against explicit inputs."""
    counsel_name, clients = item
    counsel = counsel_index.get(counsel_name)
    if not counsel:
        return None

    flagged_clients = clients & matched_names
    if not flagged_clients:
        return None

    counsel_id = counsel.get("counsel_id", "")

    all_or: list[tuple[str, str | None]] = []
    flagged_or: list[tuple[str, str | None]] = []
    flagged_pids: set[str] = set()
    for client_name in clients:
        client_entries = process_party_map.get(client_name, [])
        for pid, role in client_entries:
            for progress in process_outcomes.get(pid, []):
                all_or.append((progress, role))
                if client_name in flagged_clients:
                    flagged_or.append((progress, role))
                    flagged_pids.add(pid)

    overall_rate = compute_favorable_rate_role_aware(all_or)
    flagged_rate = compute_favorable_rate_role_aware(flagged_or)

    red_flag = False
    if flagged_rate is not None and overall_rate is not None:
        red_flag = flagged_rate > overall_rate + red_flag_delta and len(flagged_pids) >= min_cases

    return {
        "counsel_id": counsel_id,
        "counsel_name_normalized": counsel_name,
        "flagged_client_count": len(flagged_clients),
        "total_client_count": len(clients),
        "flagged_client_rate": len(flagged_clients) / len(clients) if clients else 0.0,
        "flagged_favorable_rate": flagged_rate,
        "overall_favorable_rate": overall_rate,
        "red_flag": red_flag,
    }


def build_counsel_profiles_parallel(
    counsel_client_map: dict[str, set[str]],
    *,
    counsel_index: dict[str, dict[str, Any]],
    process_party_map: dict[str, list[tuple[str, str | None]]],
    process_outcomes: dict[str, list[str]],
    matched_names: set[str],
    red_flag_delta: float = 0.15,
    min_cases: int = 3,
    max_workers: int | None = None,
) -> list[dict[str, Any]]:
    """Build counsel profiles in parallel using fork-based multiprocessing."""
    global _cp_process_party_map, _cp_process_outcomes  # noqa: PLW0603
    global _cp_matched_names, _cp_counsel_index  # noqa: PLW0603
    global _cp_red_flag_delta, _cp_min_cases  # noqa: PLW0603

    workers = max_workers or optimal_workers()
    items = list(counsel_client_map.items())

    if workers <= 1 or len(items) < 50:
        logger.info("Building %d counsel profiles sequentially", len(items))
        profiles = []
        for item in items:
            result = _compute_counsel_profile(
                item,
                process_party_map=process_party_map,
                process_outcomes=process_outcomes,
                matched_names=matched_names,
                counsel_index=counsel_index,
                red_flag_delta=red_flag_delta,
                min_cases=min_cases,
            )
            if result is not None:
                profiles.append(result)
        return profiles

    logger.info("Building %d counsel profiles in parallel with %d workers", len(items), workers)

    chunksize = max(1, len(items) // (workers * 4))
    ctx = multiprocessing.get_context("fork")

    with _cp_state_lock:
        # Set module-level state BEFORE forking.
        _cp_process_party_map = process_party_map
        _cp_process_outcomes = process_outcomes
        _cp_matched_names = matched_names
        _cp_counsel_index = counsel_index
        _cp_red_flag_delta = red_flag_delta
        _cp_min_cases = min_cases

        profiles: list[dict[str, Any]] = []
        try:
            with ctx.Pool(processes=workers) as pool:
                for result in pool.imap_unordered(_counsel_profile_one, items, chunksize=chunksize):
                    if result is not None:
                        profiles.append(result)
        finally:
            _clear_counsel_profile_state()

    return profiles
