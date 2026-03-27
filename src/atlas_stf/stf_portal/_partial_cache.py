"""Filesystem-backed partial cache for incremental STF portal extraction.

Stores per-process intermediate state (incidente ID, individual tab HTML,
retry metadata) so that interrupted or partially-failed extractions can
resume without re-fetching already-obtained data.

Layout on disk::

    {partial_dir}/{sanitized_process_number}/
        _meta.json           # retry_count, last_error, last_attempt_at
        incidente.json       # {"incidente": "12345", "resolved_at": "..."}
        abaAndamentos.html   # raw tab HTML
        abaPartes.html
        ...

All writes are atomic (write to ``.tmp`` then ``os.replace``).

Thread-safety: each worker thread operates on a distinct ``process_number``,
so no two threads write to the same directory.  ``os.replace`` is atomic on
Linux/ext4/xfs, protecting against mid-write corruption on crash.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from ._http import TABS

logger = logging.getLogger(__name__)

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_ -]+$")


@dataclass
class PartialMeta:
    """Per-process retry metadata stored in ``_meta.json``."""

    retry_count: int = 0
    last_error: str = ""
    last_attempt_at: str = ""


def _sanitize_dir_name(process_number: str) -> str:
    """Convert process number to a safe directory name.

    Raises ``ValueError`` for path-traversal attempts or invalid characters.
    """
    if not process_number:
        msg = "process_number is empty"
        raise ValueError(msg)
    if ".." in process_number or process_number.startswith("/"):
        msg = f"invalid process_number (path traversal): {process_number!r}"
        raise ValueError(msg)
    name = process_number.replace(" ", "_").replace("/", "_")
    if not _SAFE_NAME_RE.match(name):
        msg = f"invalid process_number characters: {process_number!r}"
        raise ValueError(msg)
    return name


def _atomic_write_text(path: Path, content: str) -> None:
    """Write *content* atomically: write to ``.tmp`` then ``os.replace``."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


class PartialCache:
    """Filesystem-backed partial cache for per-process extraction state."""

    def __init__(self, partial_dir: Path) -> None:
        self._dir = partial_dir

    # --- Internals ---

    def _process_dir(self, process_number: str) -> Path:
        return self._dir / _sanitize_dir_name(process_number)

    def _ensure_dir(self, process_number: str) -> Path:
        d = self._process_dir(process_number)
        d.mkdir(parents=True, exist_ok=True)
        return d

    # --- Incidente ---

    def get_incidente(self, process_number: str) -> str | None:
        """Return cached incidente ID, or ``None`` if not yet resolved."""
        path = self._process_dir(process_number) / "incidente.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data.get("incidente")
        except (json.JSONDecodeError, OSError):
            return None

    def save_incidente(self, process_number: str, incidente: str) -> None:
        """Persist incidente ID atomically."""
        d = self._ensure_dir(process_number)
        payload = json.dumps(
            {
                "incidente": incidente,
                "resolved_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        )
        _atomic_write_text(d / "incidente.json", payload + "\n")

    # --- Tabs ---

    def get_tab(self, process_number: str, tab: str) -> str | None:
        """Return cached HTML for *tab*, or ``None``."""
        path = self._process_dir(process_number) / f"{tab}.html"
        if not path.exists():
            return None
        try:
            return path.read_text(encoding="utf-8")
        except OSError:
            return None

    def save_tab(self, process_number: str, tab: str, html: str) -> None:
        """Persist tab HTML atomically.

        Accepts empty/whitespace HTML as a valid state — the portal may
        return empty content for tabs like ``abaPeticoes`` on processes
        with no petitions.  The file is created with the content as-is;
        ``get_tab()`` returns it, and the parser handles empty input
        gracefully (returns ``[]``/``{}``).
        """
        d = self._ensure_dir(process_number)
        _atomic_write_text(d / f"{tab}.html", html)

    def get_cached_tabs(self, process_number: str) -> dict[str, str]:
        """Return ``{tab_name: html}`` for all cached tabs."""
        result: dict[str, str] = {}
        for tab in TABS:
            html = self.get_tab(process_number, tab)
            if html is not None:
                result[tab] = html
        return result

    def get_missing_tabs(self, process_number: str) -> list[str]:
        """Return sorted list of tab names not yet cached."""
        cached = set(self.get_cached_tabs(process_number))
        all_tabs: set[str] = set(TABS)
        return sorted(all_tabs - cached)

    def all_tabs_present(self, process_number: str) -> bool:
        """True when all 5 mandatory tabs are cached."""
        return len(self.get_cached_tabs(process_number)) == len(TABS)

    # --- Meta (retry tracking) ---

    def get_meta(self, process_number: str) -> PartialMeta | None:
        """Read ``_meta.json``, or ``None`` if absent."""
        path = self._process_dir(process_number) / "_meta.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return PartialMeta(
                retry_count=data.get("retry_count", 0),
                last_error=data.get("last_error", ""),
                last_attempt_at=data.get("last_attempt_at", ""),
            )
        except (json.JSONDecodeError, OSError):
            return None

    def save_meta(self, process_number: str, meta: PartialMeta) -> None:
        """Persist retry metadata atomically."""
        d = self._ensure_dir(process_number)
        payload = json.dumps(asdict(meta), ensure_ascii=False)
        _atomic_write_text(d / "_meta.json", payload + "\n")

    def increment_retry(
        self,
        process_number: str,
        error: str,
    ) -> PartialMeta:
        """Bump retry_count, update last_error/last_attempt_at, persist, return updated meta."""
        existing = self.get_meta(process_number) or PartialMeta()
        updated = PartialMeta(
            retry_count=existing.retry_count + 1,
            last_error=error,
            last_attempt_at=datetime.now(timezone.utc).isoformat(),
        )
        self.save_meta(process_number, updated)
        return updated

    # --- Lifecycle ---

    def cleanup(self, process_number: str) -> None:
        """Remove the entire partial directory for a completed process."""
        d = self._process_dir(process_number)
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)

    def list_partial_processes(self) -> list[str]:
        """Return names of processes with partial state on disk."""
        if not self._dir.exists():
            return []
        return sorted(
            entry.name
            for entry in self._dir.iterdir()
            if entry.is_dir() and not entry.name.startswith(".")
        )

    def partial_count(self) -> int:
        """Count of processes with partial state."""
        return len(self.list_partial_processes())

    def has_partial(self, process_number: str) -> bool:
        """True if a partial directory exists for this process."""
        return self._process_dir(process_number).is_dir()
