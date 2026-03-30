"""Safety helpers for streamed HTTP downloads."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Protocol

logger = logging.getLogger(__name__)


class _ByteStreamResponse(Protocol):
    def iter_bytes(self) -> Iterator[bytes]: ...


def write_limited_stream_to_file(
    response: _ByteStreamResponse,
    destination: Path,
    *,
    max_download_bytes: int,
) -> int:
    """Write a streamed response to disk, refusing payloads above a byte ceiling."""
    total = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    part_path = destination.with_suffix(destination.suffix + ".part")
    try:
        with part_path.open("wb") as fh:
            for chunk in response.iter_bytes():
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_download_bytes:
                    raise ValueError(f"download exceeded max bytes ({total} > {max_download_bytes})")
                fh.write(chunk)
        part_path.replace(destination)
    except BaseException:
        part_path.unlink(missing_ok=True)
        raise
    return total


def write_stream_resilient(
    response: _ByteStreamResponse,
    destination: Path,
    *,
    max_download_bytes: int,
    stall_timeout_seconds: float = 300.0,
    resume_offset: int = 0,
    remote_etag: str = "",
    remote_content_length: int = 0,
) -> int:
    """Write a streamed response to disk with .part safety and stall detection.

    - Writes to ``destination.part`` first, renames atomically on success.
    - If ``resume_offset > 0``, appends to existing ``.part`` file (HTTP Range resume).
    - Detects stalls: if no bytes arrive for ``stall_timeout_seconds``, raises
      ``TimeoutError``.
    - On retryable failure, the ``.part`` file is kept with a progress marker
      recording ``remote_etag`` and ``remote_content_length`` for safe resume
      validation.

    Returns the total number of bytes written (including resumed offset).
    """
    part_path = destination.with_suffix(destination.suffix + ".part")
    part_path.parent.mkdir(parents=True, exist_ok=True)
    total = resume_offset
    mode = "ab" if resume_offset > 0 else "wb"

    def _save_progress() -> None:
        _write_part_progress(part_path, total, etag=remote_etag, content_length=remote_content_length)

    try:
        with part_path.open(mode) as fh:
            last_progress = time.monotonic()
            for chunk in response.iter_bytes():
                if not chunk:
                    now = time.monotonic()
                    if now - last_progress > stall_timeout_seconds:
                        _save_progress()
                        raise TimeoutError(
                            f"Download stalled: no data for {stall_timeout_seconds}s after {total} bytes"
                        )
                    continue
                total += len(chunk)
                if total > max_download_bytes:
                    raise ValueError(f"Download exceeded max bytes ({total} > {max_download_bytes})")
                fh.write(chunk)
                last_progress = time.monotonic()
        # Atomic rename on success
        part_path.replace(destination)
        _cleanup_part_progress(part_path)
        return total
    except (TimeoutError, ValueError):
        _save_progress()
        raise
    except BaseException:
        # Unexpected errors — keep .part for potential manual recovery
        _save_progress()
        raise


def get_part_resume_offset(
    destination: Path,
    *,
    expected_etag: str = "",
    expected_content_length: int = 0,
) -> int:
    """Return the byte offset to resume from, or 0 for a fresh download.

    Checks for a ``.part`` file and its progress marker.  If
    ``expected_etag`` or ``expected_content_length`` are provided, the
    partial is discarded when the remote resource has changed.
    """
    part_path = destination.with_suffix(destination.suffix + ".part")
    progress_path = _part_progress_path(part_path)

    if not part_path.exists():
        return 0

    actual_size = part_path.stat().st_size
    if actual_size == 0:
        return 0

    # Cross-check with progress marker if available
    if progress_path.exists():
        try:
            data = json.loads(progress_path.read_text(encoding="utf-8"))
            recorded = data.get("bytes_written", 0)

            # Validate remote resource hasn't changed since partial was written
            saved_etag = data.get("etag", "")
            saved_cl = data.get("content_length", 0)
            if expected_etag and saved_etag and expected_etag != saved_etag:
                logger.warning(
                    "Remote resource changed (etag %s → %s) — discarding partial",
                    saved_etag,
                    expected_etag,
                )
                _discard_partial(part_path, progress_path)
                return 0
            if expected_content_length and saved_cl and expected_content_length != saved_cl:
                logger.warning(
                    "Remote size changed (%d → %d) — discarding partial",
                    saved_cl,
                    expected_content_length,
                )
                _discard_partial(part_path, progress_path)
                return 0

            if recorded > 0 and abs(recorded - actual_size) <= 8192:
                logger.info("Resumable .part found: %d bytes", actual_size)
                return actual_size
            logger.warning(
                "Part progress mismatch (recorded=%d, actual=%d) — restarting",
                recorded,
                actual_size,
            )
            _discard_partial(part_path, progress_path)
            return 0
        except (json.JSONDecodeError, OSError):
            pass

    # No progress marker but .part exists — trust file size only if no
    # remote metadata to validate against
    if expected_content_length and actual_size >= expected_content_length:
        logger.warning("Partial >= expected size — discarding stale .part")
        _discard_partial(part_path, progress_path)
        return 0

    logger.info("Resumable .part found (no progress marker): %d bytes", actual_size)
    return actual_size


def _discard_partial(part_path: Path, progress_path: Path) -> None:
    part_path.unlink(missing_ok=True)
    progress_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Progress marker helpers
# ---------------------------------------------------------------------------


def _part_progress_path(part_path: Path) -> Path:
    return part_path.with_suffix(".progress")


def _write_part_progress(
    part_path: Path,
    bytes_written: int,
    *,
    etag: str = "",
    content_length: int = 0,
) -> None:
    """Write a progress marker alongside the .part file."""
    progress_path = _part_progress_path(part_path)
    data: dict[str, object] = {"bytes_written": bytes_written, "updated_at": time.time()}
    if etag:
        data["etag"] = etag
    if content_length:
        data["content_length"] = content_length
    try:
        progress_path.write_text(json.dumps(data), encoding="utf-8")
    except OSError:
        pass


def _cleanup_part_progress(part_path: Path) -> None:
    """Remove progress marker after successful download."""
    _part_progress_path(part_path).unlink(missing_ok=True)
