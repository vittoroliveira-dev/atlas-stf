"""Unified progress tracking with throughput-based ETA.

Provides a two-level progress model:
- **Phases**: coarse steps with configurable weights (e.g. load=5%, analyze=90%, write=5%)
- **Work units**: fine-grained progress within the heavy phase, driving ETA calculation

ETA is computed from observed throughput (completed / elapsed).  It is only
displayed when confidence >= 0.3, which requires ~6% of work to be completed.
Confidence reaches 1.0 at 20% completion — a linear ramp, not a threshold.

Throttling: updates are emitted at most every ``min_interval_ms`` milliseconds
OR when progress advances by at least ``min_advance_pct`` percent — whichever
comes first.  This avoids both UI stutter and callback overhead.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ProgressEvent:
    """Snapshot emitted on each throttled update."""

    phase_name: str
    completed: int
    total: int
    unit: str
    elapsed_seconds: float
    throughput: float
    eta_seconds: float | None
    confidence: float  # 0.0–1.0


# Type alias for the callback consumers (cli_progress, RunContext, etc.)
ProgressCallback = Callable[[int, int, str], None]


@dataclass
class PhaseSpec:
    """Declares a phase with a relative weight for overall progress."""

    name: str
    weight: float = 1.0


@dataclass
class ProgressTracker:
    """Two-level progress tracker with real ETA.

    Usage::

        tracker = ProgressTracker(
            phases=[PhaseSpec("load", 0.05), PhaseSpec("analyze", 0.90), PhaseSpec("write", 0.05)],
            callback=on_progress,
        )
        tracker.begin_phase("load")
        tracker.complete_phase()

        tracker.begin_phase("analyze", total=200_000, unit="pairs")
        for i, batch in enumerate(batches):
            process(batch)
            tracker.advance(len(batch))
        tracker.complete_phase()

        tracker.begin_phase("write")
        tracker.complete_phase()
    """

    phases: list[PhaseSpec]
    callback: ProgressCallback | None = None
    min_interval_ms: float = 300.0
    min_advance_pct: float = 0.5

    # --- internal state ---
    _phase_index: int = field(default=-1, init=False, repr=False)
    _phase_total: int = field(default=0, init=False, repr=False)
    _phase_completed: int = field(default=0, init=False, repr=False)
    _phase_unit: str = field(default="items", init=False, repr=False)
    _phase_start: float = field(default=0.0, init=False, repr=False)
    _last_emit_time: float = field(default=0.0, init=False, repr=False)
    _last_emit_pct: float = field(default=-1.0, init=False, repr=False)
    _total_weight: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        self._total_weight = sum(p.weight for p in self.phases) or 1.0

    def begin_phase(self, name: str, *, total: int = 0, unit: str = "items") -> None:
        """Start a named phase.  ``total=0`` means indeterminate."""
        idx = next((i for i, p in enumerate(self.phases) if p.name == name), None)
        if idx is None:
            msg = f"Unknown phase: {name!r}"
            raise ValueError(msg)
        self._phase_index = idx
        self._phase_total = total
        self._phase_completed = 0
        self._phase_unit = unit
        self._phase_start = time.monotonic()
        self._last_emit_time = 0.0
        self._last_emit_pct = -1.0
        self._emit(force=True)

    def advance(self, delta: int = 1) -> None:
        """Report ``delta`` work units completed in the current phase."""
        if self._phase_total:
            self._phase_completed = min(self._phase_completed + delta, self._phase_total)
        else:
            self._phase_completed += delta
        self._emit()

    _completed_phases: set[int] = field(default_factory=set, init=False, repr=False)

    def complete_phase(self) -> None:
        """Mark the current phase as 100% done."""
        if self._phase_total > 0:
            self._phase_completed = self._phase_total
        self._completed_phases.add(self._phase_index)
        self._emit(force=True)

    def _emit(self, *, force: bool = False) -> None:
        if self.callback is None or self._phase_index < 0:
            return

        now = time.monotonic()
        elapsed = now - self._phase_start
        pct = (self._phase_completed / self._phase_total * 100.0) if self._phase_total > 0 else 0.0

        if not force:
            time_ok = (now - self._last_emit_time) * 1000 >= self.min_interval_ms
            advance_ok = (pct - self._last_emit_pct) >= self.min_advance_pct
            if not (time_ok or advance_ok):
                return

        self._last_emit_time = now
        self._last_emit_pct = pct

        throughput = self._phase_completed / elapsed if elapsed > 0 else 0.0
        eta = self._compute_eta(throughput)
        confidence = self._compute_confidence()

        # Compute overall progress across weighted phases
        overall_completed, overall_total = self._overall_progress()

        phase = self.phases[self._phase_index]
        desc = phase.name
        if self._phase_total > 0:
            eta_str = _format_eta(eta) if eta is not None and confidence >= 0.3 else ""
            desc = f"{phase.name}: {self._phase_completed:,}/{self._phase_total:,} {self._phase_unit}"
            if eta_str:
                desc += f" {eta_str}"

        self.callback(overall_completed, overall_total, desc)

    def _compute_eta(self, throughput: float) -> float | None:
        if self._phase_total <= 0 or throughput <= 0:
            return None
        remaining = self._phase_total - self._phase_completed
        return remaining / throughput

    def _compute_confidence(self) -> float:
        """Linear ramp from 0.0 to 1.0 over the first 20% of work.

        - 0% completed → 0.0 (no data, ETA suppressed)
        - 6% completed → 0.3 (minimum for ETA display)
        - 10% completed → 0.5
        - 20% completed → 1.0 (fully confident)

        Indeterminate phases (total=0) always return 0.0.
        """
        if self._phase_total <= 0:
            return 0.0
        fraction = self._phase_completed / self._phase_total
        if fraction <= 0:
            return 0.0
        if fraction >= 0.2:
            return 1.0
        return fraction / 0.2

    def _overall_progress(self) -> tuple[int, int]:
        """Map phase-level progress to a weighted 0–10000 scale."""
        scale = 10000
        completed = 0.0
        for i, phase in enumerate(self.phases):
            w = phase.weight / self._total_weight
            if i in self._completed_phases:
                completed += w
            elif i == self._phase_index:
                frac = (self._phase_completed / self._phase_total) if self._phase_total > 0 else 0.0
                completed += w * frac
        return int(completed * scale), scale


def _format_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return ""
    total = int(seconds)
    if total < 60:
        return f"~{total}s"
    minutes, secs = divmod(total, 60)
    if minutes < 60:
        return f"~{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"~{hours}h{minutes:02d}m"
