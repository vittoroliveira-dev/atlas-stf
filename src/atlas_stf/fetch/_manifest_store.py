"""Manifest persistence — load/save with advisory locking.

All writes go through ``save_manifest_locked()`` which acquires the
source-level ``FetchLock`` internally.

For code paths that are already executing inside a ``FetchLock`` (e.g.
inner runner functions called from ``_fetch_*_locked()``), use
``write_manifest_unlocked()`` to perform the atomic file-replace without
trying to re-acquire the same lock.
"""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path

from ..core.fetch_lock import FetchLock
from ._manifest_model import SourceManifest, serialize_manifest

logger = logging.getLogger(__name__)

_MANIFEST_FILENAME_TEMPLATE = "_manifest_{source}.json"


def _manifest_path(source: str, output_dir: Path) -> Path:
    return output_dir / _MANIFEST_FILENAME_TEMPLATE.format(source=source)


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def load_manifest(source: str, output_dir: Path) -> SourceManifest | None:
    """Load a source manifest from disk, or *None* if absent/corrupt."""
    path = _manifest_path(source, output_dir)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        manifest = SourceManifest.from_dict(data)
        if manifest.source != source:
            logger.warning("Manifest source mismatch: expected %r, got %r", source, manifest.source)
            return None
        return manifest
    except json.JSONDecodeError, KeyError, TypeError, ValueError:
        logger.warning("Corrupt manifest for source %r at %s", source, path)
        return None


def load_all_manifests(base_dir: Path) -> dict[str, SourceManifest]:
    """Load every ``_manifest_*.json`` under *base_dir*."""
    result: dict[str, SourceManifest] = {}
    if not base_dir.is_dir():
        return result
    for path in sorted(base_dir.glob("_manifest_*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            m = SourceManifest.from_dict(data)
            result[m.source] = m
        except json.JSONDecodeError, KeyError, TypeError, ValueError:
            logger.warning("Skipping corrupt manifest: %s", path)
    return result


# ---------------------------------------------------------------------------
# Write (locked)
# ---------------------------------------------------------------------------


def save_manifest_locked(manifest: SourceManifest, output_dir: Path) -> Path:
    """Atomically write a manifest while holding the source lock.

    1. Acquire ``FetchLock``
    2. Write to a temp file in the same directory
    3. ``os.replace`` (atomic on POSIX) to final path
    4. Release lock
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    dest = _manifest_path(manifest.source, output_dir)

    with FetchLock(output_dir, manifest.source):
        content = serialize_manifest(manifest)
        fd, tmp_path_str = tempfile.mkstemp(
            dir=str(output_dir),
            prefix=f".manifest_{manifest.source}_",
            suffix=".tmp",
        )
        tmp_path = Path(tmp_path_str)
        try:
            with open(fd, "w", encoding="utf-8") as f:
                f.write(content)
            tmp_path.replace(dest)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

    logger.info("Saved manifest for %r → %s", manifest.source, dest)
    return dest


def write_manifest_unlocked(manifest: SourceManifest, output_dir: Path) -> Path:
    """Atomically write a manifest without acquiring ``FetchLock``.

    Use this when the caller already holds the source lock (i.e. inside a
    ``_fetch_*_locked()`` function).  Calling ``save_manifest_locked()``
    from inside an existing lock would deadlock because ``FetchLock`` is
    non-reentrant.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    dest = _manifest_path(manifest.source, output_dir)
    content = serialize_manifest(manifest)
    fd, tmp_path_str = tempfile.mkstemp(
        dir=str(output_dir),
        prefix=f".manifest_{manifest.source}_",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_path_str)
    try:
        with open(fd, "w", encoding="utf-8") as f:
            f.write(content)
        tmp_path.replace(dest)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    logger.info("Saved manifest (unlocked) for %r → %s", manifest.source, dest)
    return dest
