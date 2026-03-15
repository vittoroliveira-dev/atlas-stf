"""ZIP safety helpers for bounded extraction and reads."""

from __future__ import annotations

import zipfile
from collections.abc import Iterable
from pathlib import Path

MAX_ZIP_UNCOMPRESSED_BYTES = 128 * 1024 * 1024


def is_safe_zip_member(filename: str, output_dir: Path) -> bool:
    """Return True if extracting *filename* stays within *output_dir*.

    Guards against path-traversal attacks (``../``, absolute paths, and
    paths that resolve outside the target directory after normalisation).
    """
    base = output_dir.resolve()
    target = (output_dir / filename).resolve()
    return target == base or target.is_relative_to(base)


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
