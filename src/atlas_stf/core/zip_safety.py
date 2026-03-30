"""ZIP safety helpers for bounded extraction and reads."""

from __future__ import annotations

import zipfile
from collections.abc import Iterable
from pathlib import Path

MAX_ZIP_UNCOMPRESSED_BYTES = 128 * 1024 * 1024


def is_safe_zip_member(
    filename: str,
    output_dir: Path,
    *,
    external_attr: int = 0,
) -> bool:
    """Return True if extracting *filename* stays within *output_dir*.

    Guards against path-traversal attacks (``../``, absolute paths,
    paths that resolve outside the target directory after normalisation)
    and Unix symlinks embedded in the ZIP.
    """
    # Reject Unix symlinks (mode 0o120000 in upper 16 bits)
    if (external_attr >> 16) & 0o170000 == 0o120000:
        return False
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
