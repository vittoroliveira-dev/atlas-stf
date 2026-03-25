"""Advisory file-based lock for fetch operations.

Prevents concurrent runs of the same fetch source from corrupting shared
state (checkpoint files, output JSONL, temporary ZIPs).

Uses ``fcntl.flock`` (POSIX advisory lock) — non-blocking by default so
a second run fails fast with a clear message instead of silently waiting.

Stale-lock recovery: if the holder PID recorded in the lock file no longer
exists, the lock is considered stale and reclaimed automatically.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType

logger = logging.getLogger(__name__)


class FetchLockError(RuntimeError):
    """Raised when a fetch lock cannot be acquired (another run is active)."""


class FetchLock:
    """Context manager that acquires an advisory lock for a fetch source.

    Usage::

        with FetchLock(output_dir, "tse"):
            # only one TSE fetch can execute this block at a time
            ...

    The lock file is placed at ``{output_dir}/.fetch_{source}.lock``.

    On acquire, the current PID and timestamp are written to the lock file so
    that a subsequent attempt can detect and reclaim a stale lock (holder
    process dead).
    """

    def __init__(self, output_dir: Path, source: str) -> None:
        self._lock_path = output_dir / f".fetch_{source}.lock"
        self._fd: int | None = None

    def __enter__(self) -> FetchLock:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(str(self._lock_path), os.O_WRONLY | os.O_CREAT)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            # Lock held — check if the holder is still alive
            if self._is_holder_dead():
                logger.warning(
                    "Stale lock detected (holder PID dead) — reclaiming: %s",
                    self._lock_path,
                )
                # Close and reopen to retry
                os.close(self._fd)
                self._fd = os.open(str(self._lock_path), os.O_WRONLY | os.O_CREAT)
                try:
                    fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    os.close(self._fd)
                    self._fd = None
                    raise FetchLockError(
                        f"Lock reclaim failed for '{self._lock_path.stem}'. Lock file: {self._lock_path}"
                    )
            else:
                os.close(self._fd)
                self._fd = None
                raise FetchLockError(
                    f"Another fetch for source '{self._lock_path.stem}' is already running. "
                    f"Lock file: {self._lock_path}"
                )
        # Write PID + timestamp to lock file for diagnostics
        self._write_holder_info()
        logger.debug("Acquired fetch lock: %s (pid=%d)", self._lock_path, os.getpid())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None
            logger.debug("Released fetch lock: %s", self._lock_path)

    def _write_holder_info(self) -> None:
        """Write current PID and timestamp to the lock file."""
        if self._fd is not None:
            info = json.dumps({"pid": os.getpid(), "acquired_at": datetime.now(UTC).isoformat()})
            os.ftruncate(self._fd, 0)
            os.lseek(self._fd, 0, os.SEEK_SET)
            os.write(self._fd, info.encode())

    def _is_holder_dead(self) -> bool:
        """Check if the PID in the lock file refers to a dead process."""
        try:
            content = self._lock_path.read_text(encoding="utf-8").strip()
            if not content:
                return True  # Empty lock file = stale
            data = json.loads(content)
            pid = data.get("pid", 0)
            if pid <= 0:
                return True
            os.kill(pid, 0)  # Signal 0 = check existence only
            return False  # Process exists
        except ProcessLookupError:
            return True  # PID doesn't exist
        except json.JSONDecodeError, OSError, KeyError, ValueError:
            return True  # Unreadable = assume stale
