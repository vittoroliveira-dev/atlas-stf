"""ZIP safety helpers for bounded extraction and reads."""

from __future__ import annotations

import zipfile
from collections.abc import Iterable

MAX_ZIP_UNCOMPRESSED_BYTES = 128 * 1024 * 1024


def enforce_max_uncompressed_size(
    members: Iterable[zipfile.ZipInfo],
    *,
    max_total_uncompressed_bytes: int | None = None,
) -> None:
    """Reject ZIP members whose combined uncompressed size exceeds the limit."""
    limit = MAX_ZIP_UNCOMPRESSED_BYTES if max_total_uncompressed_bytes is None else max_total_uncompressed_bytes
    total_size = 0
    for member in members:
        if member.is_dir():
            continue
        total_size += member.file_size
        if total_size > limit:
            raise ValueError("ZIP uncompressed size exceeds limit")
