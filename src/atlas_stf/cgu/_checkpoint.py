"""Incremental download checkpoint for CGU CSV strategy."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class _DatasetMeta:
    """Metadata for a single downloaded CGU dataset."""

    content_length: int
    download_date: str

    def to_dict(self) -> dict[str, Any]:
        return {"content_length": self.content_length, "download_date": self.download_date}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> _DatasetMeta:
        return cls(content_length=int(data["content_length"]), download_date=str(data["download_date"]))

    def matches(self, content_length: int, download_date: str) -> bool:
        """Return True if remote content-length and date match this metadata."""
        return self.content_length == content_length and self.download_date == download_date


@dataclass
class _CguCheckpoint:
    """Checkpoint for incremental CGU CSV downloads.

    Stores per-dataset metadata (content_length, download_date) so that
    subsequent runs can skip unchanged datasets via a HEAD request.
    """

    completed_datasets: dict[str, _DatasetMeta] = field(default_factory=dict)
    download_date: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "csv_completed_datasets": {k: v.to_dict() for k, v in self.completed_datasets.items()},
            "csv_download_date": self.download_date,
        }

    @classmethod
    def load(cls, output_dir: Path) -> _CguCheckpoint:
        """Load checkpoint from disk. Returns empty checkpoint if file is missing or invalid."""
        checkpoint_path = output_dir / "_checkpoint.json"
        if not checkpoint_path.exists():
            return cls()
        try:
            data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            raw_datasets = data.get("csv_completed_datasets", {})
            completed: dict[str, _DatasetMeta] = {}
            for name, meta_dict in raw_datasets.items():
                completed[name] = _DatasetMeta.from_dict(meta_dict)
            return cls(
                completed_datasets=completed,
                download_date=str(data.get("csv_download_date", "")),
            )
        except json.JSONDecodeError, KeyError, TypeError, ValueError:
            return cls()

    def save(self, output_dir: Path) -> None:
        """Save checkpoint to disk, preserving any existing non-CSV keys."""
        checkpoint_path = output_dir / "_checkpoint.json"
        existing: dict[str, Any] = {}
        if checkpoint_path.exists():
            try:
                existing = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError, ValueError:
                pass
        existing.update(self.to_dict())
        checkpoint_path.write_text(
            json.dumps(existing, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
