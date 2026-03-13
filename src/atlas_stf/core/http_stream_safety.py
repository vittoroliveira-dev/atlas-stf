"""Safety helpers for streamed HTTP downloads."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Protocol


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
    with destination.open("wb") as fh:
        for chunk in response.iter_bytes():
            if not chunk:
                continue
            total += len(chunk)
            if total > max_download_bytes:
                raise ValueError(f"download exceeded max bytes ({total} > {max_download_bytes})")
            fh.write(chunk)
    return total
