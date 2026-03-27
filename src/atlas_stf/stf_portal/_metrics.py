"""Thread-safe extraction metrics for STF portal."""

from __future__ import annotations

import json
import os
import threading
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_TIMING_BUFFER_SIZE = 1000


@dataclass
class ExtractionMetrics:
    """Accumulates extraction metrics across all worker threads.

    All counter increments and timing recordings are serialized via a single
    lock.  Since the hot path is I/O-bound (HTTP requests of 200 ms-5 s),
    lock contention of microseconds is negligible.

    Design note: ``to_dict()`` and ``summary_line()`` compute derived values
    inline while holding the lock, rather than calling ``@property`` methods
    that would re-acquire it.  This avoids reentrant locking and ensures a
    consistent snapshot across all fields.
    """

    # --- Counters ---
    requests_total: int = 0
    requests_resolve: int = 0
    requests_tabs: int = 0

    processes_completed: int = 0
    processes_failed: int = 0
    processes_resumed_from_partial: int = 0

    incidente_reused_from_cache: int = 0  # resolve skipped (partial or checkpoint)
    tabs_reused_from_partial: int = 0
    tabs_downloaded_fresh: int = 0

    tab_retries_total: int = 0
    retryable_errors_total: int = 0
    non_retryable_errors_total: int = 0

    http_403_total: int = 0
    http_429_total: int = 0

    # --- Timing (circular buffers) ---
    _resolve_ms: deque[float] = field(default_factory=lambda: deque(maxlen=_TIMING_BUFFER_SIZE))
    _tab_ms: deque[float] = field(default_factory=lambda: deque(maxlen=_TIMING_BUFFER_SIZE))

    # --- Distribution ---
    http_status_counts: dict[int, int] = field(default_factory=dict)

    # --- Elapsed ---
    elapsed_seconds: float = 0.0

    # --- Internal ---
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    # --- Thread-safe increment helpers ---

    def inc(self, name: str, delta: int = 1) -> None:
        """Increment a named counter by *delta*."""
        with self._lock:
            current = getattr(self, name)
            setattr(self, name, current + delta)

    def record_resolve_ms(self, ms: float) -> None:
        with self._lock:
            self._resolve_ms.append(ms)

    def record_tab_ms(self, ms: float) -> None:
        with self._lock:
            self._tab_ms.append(ms)

    def record_http_status(self, status: int) -> None:
        with self._lock:
            self.http_status_counts[status] = self.http_status_counts.get(status, 0) + 1

    # --- Computed properties (single lock acquisition, no nesting) ---

    @property
    def avg_resolve_ms(self) -> float:
        with self._lock:
            return self._avg_resolve_ms_unlocked()

    @property
    def avg_tab_ms(self) -> float:
        with self._lock:
            return self._avg_tab_ms_unlocked()

    @property
    def effective_requests_per_hour(self) -> float:
        with self._lock:
            return self._effective_requests_per_hour_unlocked()

    @property
    def effective_processes_per_hour(self) -> float:
        with self._lock:
            return self._effective_processes_per_hour_unlocked()

    @property
    def average_requests_per_completed_process(self) -> float:
        with self._lock:
            return self._avg_requests_per_completed_unlocked()

    # --- Unlocked helpers (must be called while holding _lock) ---

    def _avg_resolve_ms_unlocked(self) -> float:
        if not self._resolve_ms:
            return 0.0
        return sum(self._resolve_ms) / len(self._resolve_ms)

    def _avg_tab_ms_unlocked(self) -> float:
        if not self._tab_ms:
            return 0.0
        return sum(self._tab_ms) / len(self._tab_ms)

    def _effective_requests_per_hour_unlocked(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.requests_total / self.elapsed_seconds * 3600

    def _effective_processes_per_hour_unlocked(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.processes_completed / self.elapsed_seconds * 3600

    def _avg_requests_per_completed_unlocked(self) -> float:
        if self.processes_completed <= 0:
            return 0.0
        return self.requests_total / self.processes_completed

    # --- Export ---

    def to_dict(self) -> dict[str, Any]:
        """Serialize metrics to a JSON-friendly dict.

        Acquires the lock once and computes all derived values inline
        to avoid reentrant lock acquisition and ensure a consistent snapshot.
        """
        with self._lock:
            return {
                "requests_total": self.requests_total,
                "requests_resolve": self.requests_resolve,
                "requests_tabs": self.requests_tabs,
                "processes_completed": self.processes_completed,
                "processes_failed": self.processes_failed,
                "processes_resumed_from_partial": self.processes_resumed_from_partial,
                "incidente_reused_from_cache": self.incidente_reused_from_cache,
                "tabs_reused_from_partial": self.tabs_reused_from_partial,
                "tabs_downloaded_fresh": self.tabs_downloaded_fresh,
                "tab_retries_total": self.tab_retries_total,
                "retryable_errors_total": self.retryable_errors_total,
                "non_retryable_errors_total": self.non_retryable_errors_total,
                "http_403_total": self.http_403_total,
                "http_429_total": self.http_429_total,
                "http_status_counts": dict(self.http_status_counts),
                "elapsed_seconds": round(self.elapsed_seconds, 2),
                "effective_requests_per_hour": round(self._effective_requests_per_hour_unlocked(), 1),
                "effective_processes_per_hour": round(self._effective_processes_per_hour_unlocked(), 1),
                "average_requests_per_completed_process": round(
                    self._avg_requests_per_completed_unlocked(), 2
                ),
                "avg_resolve_ms": round(self._avg_resolve_ms_unlocked(), 1),
                "avg_tab_ms": round(self._avg_tab_ms_unlocked(), 1),
            }

    def summary_line(self) -> str:
        """One-liner for periodic log output.

        Acquires lock once for a consistent snapshot across all fields.
        """
        with self._lock:
            return (
                f"completed={self.processes_completed} "
                f"failed={self.processes_failed} "
                f"resumed={self.processes_resumed_from_partial} "
                f"reqs/h={self._effective_requests_per_hour_unlocked():.0f} "
                f"procs/h={self._effective_processes_per_hour_unlocked():.0f} "
                f"avg_tab={self._avg_tab_ms_unlocked():.0f}ms "
                f"403s={self.http_403_total} "
                f"partial_reused={self.tabs_reused_from_partial}"
            )

    def save(self, path: Path) -> None:
        """Atomically write metrics to JSON file."""
        tmp = path.with_suffix(".json.tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n"
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, path)
