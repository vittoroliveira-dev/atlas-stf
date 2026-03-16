from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from atlas_stf.analytics import _parallel
from atlas_stf.analytics._match_helpers import EntityMatchIndex, EntityMatchResult


def _build_index() -> EntityMatchIndex:
    return EntityMatchIndex(
        records=[],
        by_tax_id={},
        by_name={},
        by_canonical_name={},
        by_token={},
        canonical_names=[],
        aliases={},
    )


def test_match_entities_parallel_sequential_path_uses_direct_calls(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_match_entity_record(
        *,
        query_name: Any,
        query_tax_id: Any,
        index: EntityMatchIndex,
        name_field: str,
        thresholds: Any = None,
    ):
        calls.append((str(query_name), name_field))
        return EntityMatchResult(record={"entity": query_name}, strategy="exact", score=1.0)

    monkeypatch.setattr(_parallel, "match_entity_record", fake_match_entity_record)

    results = _parallel.match_entities_parallel(
        [("ALFA", None), ("BETA", "123")],
        index=_build_index(),
        name_field="party_name_normalized",
        max_workers=1,
    )

    assert calls == [("ALFA", "party_name_normalized"), ("BETA", "party_name_normalized")]
    assert results["ALFA"] is not None
    assert _parallel._worker_index is None
    assert _parallel._worker_name_field == ""


def test_build_counsel_profiles_parallel_sequential_path_does_not_leak_globals(monkeypatch) -> None:
    def fake_rate(values: list[tuple[str, str | None]]) -> float | None:
        if not values:
            return None
        favorable = sum(1 for progress, _role in values if progress == "FAVORAVEL")
        return favorable / len(values)

    monkeypatch.setattr(_parallel, "compute_favorable_rate_role_aware", fake_rate)

    profiles = _parallel.build_counsel_profiles_parallel(
        {"ADV A": {"CLIENTE 1", "CLIENTE 2"}},
        counsel_index={"ADV A": {"counsel_id": "c1"}},
        process_party_map={
            "CLIENTE 1": [("p1", "REQTE")],
            "CLIENTE 2": [("p2", "REQDO")],
        },
        process_outcomes={
            "p1": ["FAVORAVEL", "FAVORAVEL"],
            "p2": ["DESFAVORAVEL"],
        },
        matched_names={"CLIENTE 1"},
        red_flag_delta=0.2,
        min_cases=1,
        max_workers=1,
    )

    assert len(profiles) == 1
    assert profiles[0]["counsel_id"] == "c1"
    assert _parallel._cp_process_party_map == {}
    assert _parallel._cp_process_outcomes == {}
    assert _parallel._cp_matched_names == set()
    assert _parallel._cp_counsel_index == {}


def test_match_entities_parallel_serializes_parallel_worker_state(monkeypatch) -> None:
    first_pool_entered = threading.Event()
    release_first_pool = threading.Event()
    second_pool_entered = threading.Event()
    pool_counter = {"value": 0}

    @dataclass
    class FakePool:
        pool_id: int

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def imap_unordered(self, func, items, chunksize=1):
            if self.pool_id == 1:
                first_pool_entered.set()
                release_first_pool.wait(timeout=2)
            else:
                second_pool_entered.set()
            for item in items:
                yield func(item)

    class FakeContext:
        def Pool(self, processes: int):
            pool_counter["value"] += 1
            return FakePool(pool_counter["value"])

    def fake_match_entity_record(
        *,
        query_name: Any,
        query_tax_id: Any,
        index: EntityMatchIndex,
        name_field: str,
        thresholds: Any = None,
    ):
        return EntityMatchResult(record={"query_name": query_name, "name_field": name_field}, strategy="exact")

    monkeypatch.setattr(_parallel, "match_entity_record", fake_match_entity_record)
    monkeypatch.setattr(_parallel.multiprocessing, "get_context", lambda method: FakeContext())

    first_results: dict[str, EntityMatchResult | None] = {}
    second_results: dict[str, EntityMatchResult | None] = {}

    def run_first() -> None:
        first_results.update(
            _parallel.match_entities_parallel(
                [(f"FIRST-{i}", None) for i in range(100)],
                index=_build_index(),
                name_field="field_a",
                max_workers=2,
            )
        )

    def run_second() -> None:
        second_results.update(
            _parallel.match_entities_parallel(
                [(f"SECOND-{i}", None) for i in range(100)],
                index=_build_index(),
                name_field="field_b",
                max_workers=2,
            )
        )

    first_thread = threading.Thread(target=run_first)
    second_thread = threading.Thread(target=run_second)

    first_thread.start()
    assert first_pool_entered.wait(timeout=2)

    second_thread.start()
    assert not second_pool_entered.wait(timeout=0.2)

    release_first_pool.set()
    first_thread.join(timeout=2)
    second_thread.join(timeout=2)

    assert second_pool_entered.is_set()
    assert {result.record["name_field"] for result in first_results.values() if result is not None} == {"field_a"}
    assert {result.record["name_field"] for result in second_results.values() if result is not None} == {"field_b"}
    assert _parallel._worker_index is None
    assert _parallel._worker_name_field == ""
