"""Atomic JSONL writer — crash-safe file writes via rename."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import IO

logger = logging.getLogger("atlas_stf.analytics")


class AtomicJsonlWriter:
    """Context manager for atomic file writes.

    Writes to a temporary file and atomically renames on success.
    On failure, removes the temporary file and preserves any existing target.
    """

    def __init__(self, target: Path) -> None:
        self._target = target
        self._tmp = target.with_suffix(".jsonl.tmp")
        self._fh: IO[str] | None = None

    def __enter__(self) -> IO[str]:
        self._target.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._tmp.open("w", encoding="utf-8")
        return self._fh

    def __exit__(self, exc_type: type[BaseException] | None, _exc_val: BaseException | None, _exc_tb: object) -> None:
        assert self._fh is not None
        self._fh.close()
        if exc_type is None:
            self._tmp.replace(self._target)
        else:
            self._tmp.unlink(missing_ok=True)
            logger.warning("Atomic write aborted for %s", self._target)
