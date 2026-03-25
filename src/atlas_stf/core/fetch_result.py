"""Structured result for fetch operations.

Each fetch runner emits a ``FetchResult`` as a JSON log line at the end of
execution.  This replaces ``tail -N`` audit with a machine-readable summary
that includes source, status, record count, duration, and exit code.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchResult:
    """Immutable summary of a fetch operation."""

    source: str
    status: Literal["success", "skipped", "partial", "failed"]
    records_written: int
    duration_seconds: float
    exit_code: int  # 0=ok, 1=partial, 2=failed
    detail: str = ""

    def log(self) -> None:
        """Emit a structured JSON log line for observability."""
        logger.info("FETCH_RESULT %s", json.dumps(asdict(self), ensure_ascii=False))


@dataclass
class FetchTimer:
    """Timer that tracks elapsed time and guarantees a FETCH_RESULT log.

    Public API: ``start()``, ``log_success()``, ``log_failure()``.
    Also usable as a context manager (``__exit__`` auto-logs failure on
    unhandled exceptions if neither ``log_success`` nor ``log_failure``
    was called).

    Recommended pattern for long runner bodies::

        timer = FetchTimer("tse")
        timer.start()
        try:
            ...  # existing body, no extra indentation needed
            timer.log_success(records_written=42)
            return result
        except Exception as exc:
            timer.log_failure(exc)
            raise
    """

    source: str
    _start: float = field(default=0.0, init=False, repr=False)
    _elapsed: float = field(default=0.0, init=False, repr=False)
    _result_logged: bool = field(default=False, init=False, repr=False)

    # --- Public API ---

    def start(self) -> None:
        """Begin timing. Call once before the fetch body."""
        self._start = time.monotonic()

    def log_success(self, *, records_written: int = 0, detail: str = "") -> None:
        """Capture elapsed time and emit a success FETCH_RESULT log."""
        self._snap_elapsed()
        self._result_logged = True
        self.result(status="success", records_written=records_written, detail=detail).log()

    def log_failure(self, exc: BaseException | None = None) -> None:
        """Capture elapsed time and emit a failure FETCH_RESULT log."""
        self._snap_elapsed()
        if not self._result_logged:
            detail = f"{type(exc).__name__}: {exc}" if exc else ""
            self._result_logged = True
            self.result(status="failed", detail=detail).log()

    # --- Context manager protocol (for short bodies or tests) ---

    def __enter__(self) -> FetchTimer:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, _exc_tb) -> None:
        self._snap_elapsed()
        # Only auto-log for Exception subclasses (fetch errors). BaseException
        # types like KeyboardInterrupt and SystemExit are cancellations, not
        # fetch failures — logging them as status="failed" is misleading.
        if (
            exc_type is not None
            and issubclass(exc_type, Exception)
            and not self._result_logged
        ):
            self.log_failure(exc_val)

    # --- Result builder ---

    @property
    def elapsed(self) -> float:
        return self._elapsed

    def result(
        self,
        *,
        status: Literal["success", "skipped", "partial", "failed"],
        records_written: int = 0,
        detail: str = "",
    ) -> FetchResult:
        exit_code = {"success": 0, "skipped": 0, "partial": 1, "failed": 2}[status]
        return FetchResult(
            source=self.source,
            status=status,
            records_written=records_written,
            duration_seconds=round(self._elapsed, 2),
            exit_code=exit_code,
            detail=detail,
        )

    # --- Internal ---

    def _snap_elapsed(self) -> None:
        if self._start > 0:
            self._elapsed = time.monotonic() - self._start
