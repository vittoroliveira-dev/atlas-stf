"""Fetch adapter: bridges the fetch executor with datajud runner."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any

from ..fetch._executor import FetchExecutionResult
from ..fetch._manifest_model import PlanItem
from ._client import DatajudClient
from ._config import DATAJUD_API_KEY_ENV
from ._runner import fetch_single_index

logger = logging.getLogger(__name__)


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def execute_datajud_item(
    item: PlanItem,
    output_dir: Path,
    *,
    api_key: str = "",
    process_path: Path | None = None,
) -> FetchExecutionResult:
    """Execute a single DataJud plan item."""
    key = api_key or os.getenv(DATAJUD_API_KEY_ENV, "")
    if not key:
        return FetchExecutionResult(
            unit_id=item.unit_id,
            success=False,
            records_written=0,
            remote_artifact_sha256="",
            published_artifact_sha256="",
            error="No API key — set DATAJUD_API_KEY or pass --api-key",
        )

    index = ""
    if item.unit_id.startswith("datajud:"):
        index = item.unit_id.split(":", 1)[1]

    if not index:
        return FetchExecutionResult(
            unit_id=item.unit_id,
            success=False,
            records_written=0,
            remote_artifact_sha256="",
            published_artifact_sha256="",
            error=f"Cannot extract index from unit_id: {item.unit_id}",
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        with DatajudClient(key) as client:
            result: dict[str, Any] = fetch_single_index(client, index, output_dir)
    except Exception as exc:
        return FetchExecutionResult(
            unit_id=item.unit_id,
            success=False,
            records_written=0,
            remote_artifact_sha256="",
            published_artifact_sha256="",
            error=str(exc),
        )

    out_path = output_dir / f"{index}.json"
    pub_sha = _file_sha256(out_path) if out_path.exists() else ""
    remote_sha = hashlib.sha256(
        json.dumps(result, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()

    return FetchExecutionResult(
        unit_id=item.unit_id,
        success=True,
        records_written=result.get("total_processes", 0),
        remote_artifact_sha256=remote_sha,
        published_artifact_sha256=pub_sha,
        error="",
    )
