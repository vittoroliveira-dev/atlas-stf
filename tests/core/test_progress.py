"""Tests for core.progress — ProgressTracker with real ETA."""

from __future__ import annotations

from unittest.mock import MagicMock

from atlas_stf.core.progress import PhaseSpec, ProgressTracker, _format_eta


class TestFormatEta:
    def test_none(self):
        assert _format_eta(None) == ""

    def test_negative(self):
        assert _format_eta(-5.0) == ""

    def test_seconds(self):
        assert _format_eta(45.0) == "~45s"

    def test_minutes(self):
        assert _format_eta(125.0) == "~2m05s"

    def test_hours(self):
        assert _format_eta(3725.0) == "~1h02m"

    def test_zero(self):
        assert _format_eta(0.0) == "~0s"


class TestProgressTrackerPhases:
    def test_unknown_phase_raises(self):
        tracker = ProgressTracker(phases=[PhaseSpec("a")])
        import pytest

        with pytest.raises(ValueError, match="Unknown phase"):
            tracker.begin_phase("nonexistent")

    def test_single_phase_no_callback(self):
        tracker = ProgressTracker(phases=[PhaseSpec("load")])
        tracker.begin_phase("load")
        tracker.advance(10)
        tracker.complete_phase()

    def test_callback_receives_updates(self):
        cb = MagicMock()
        tracker = ProgressTracker(
            phases=[PhaseSpec("work", weight=1.0)],
            callback=cb,
            min_interval_ms=0,
            min_advance_pct=0,
        )
        tracker.begin_phase("work", total=100, unit="items")
        tracker.advance(50)
        tracker.complete_phase()

        assert cb.call_count >= 2
        # Final call should be at 100%
        last_call = cb.call_args_list[-1]
        completed, total, desc = last_call[0]
        assert completed == total  # 100%

    def test_overall_progress_weighted(self):
        cb = MagicMock()
        tracker = ProgressTracker(
            phases=[
                PhaseSpec("load", weight=0.1),
                PhaseSpec("analyze", weight=0.8),
                PhaseSpec("write", weight=0.1),
            ],
            callback=cb,
            min_interval_ms=0,
            min_advance_pct=0,
        )

        tracker.begin_phase("load")
        tracker.complete_phase()

        # After completing load (10% weight), overall should be ~10%
        last_call = cb.call_args_list[-1]
        completed, total, _ = last_call[0]
        pct = completed / total * 100
        assert 9.0 <= pct <= 11.0

        tracker.begin_phase("analyze", total=200, unit="pairs")
        tracker.advance(100)  # 50% of analyze phase

        # After 50% of analyze (80% weight), overall = 10% + 40% = ~50%
        last_call = cb.call_args_list[-1]
        completed, total, _ = last_call[0]
        pct = completed / total * 100
        assert 45.0 <= pct <= 55.0

    def test_description_includes_counts_and_unit(self):
        cb = MagicMock()
        tracker = ProgressTracker(
            phases=[PhaseSpec("Analyzing", weight=1.0)],
            callback=cb,
            min_interval_ms=0,
            min_advance_pct=0,
        )
        tracker.begin_phase("Analyzing", total=1000, unit="pairs")
        tracker.advance(500)

        calls_with_counts = [
            c for c in cb.call_args_list if "500" in str(c[0][2]) and "pairs" in str(c[0][2])
        ]
        assert len(calls_with_counts) >= 1

    def test_throttling_by_time(self):
        cb = MagicMock()
        tracker = ProgressTracker(
            phases=[PhaseSpec("work", weight=1.0)],
            callback=cb,
            min_interval_ms=10000,  # 10 seconds — no time-based updates
            min_advance_pct=50.0,  # only on 50% jumps
        )
        tracker.begin_phase("work", total=1000, unit="items")
        # begin_phase emits 1 forced update
        initial_count = cb.call_count

        for _ in range(100):
            tracker.advance(1)

        # At 10% advance with 50% threshold + 10s time gate, no new updates
        assert cb.call_count == initial_count

    def test_indeterminate_phase(self):
        cb = MagicMock()
        tracker = ProgressTracker(
            phases=[PhaseSpec("load", weight=1.0)],
            callback=cb,
            min_interval_ms=0,
            min_advance_pct=0,
        )
        tracker.begin_phase("load")  # no total
        tracker.advance(5)
        tracker.complete_phase()
        assert cb.call_count >= 1


class TestConfidence:
    def test_zero_at_start(self):
        tracker = ProgressTracker(phases=[PhaseSpec("w")])
        tracker.begin_phase("w", total=100)
        assert tracker._compute_confidence() == 0.0

    def test_ramps_up(self):
        tracker = ProgressTracker(phases=[PhaseSpec("w")])
        tracker.begin_phase("w", total=100)
        tracker._phase_completed = 10  # 10%
        assert 0.4 <= tracker._compute_confidence() <= 0.6

    def test_full_at_20_percent(self):
        tracker = ProgressTracker(phases=[PhaseSpec("w")])
        tracker.begin_phase("w", total=100)
        tracker._phase_completed = 20
        assert tracker._compute_confidence() == 1.0

    def test_indeterminate_zero(self):
        tracker = ProgressTracker(phases=[PhaseSpec("w")])
        tracker.begin_phase("w")  # no total
        assert tracker._compute_confidence() == 0.0
