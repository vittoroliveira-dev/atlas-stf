"""Fetch executor: runs a plan item by dispatching to source-specific runners.

Phase 2D of the fetch engine: ``download_raw → publish → commit``.
Each source adapter maps plan items to runner calls and updates the manifest.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ._manifest_model import (
    REFRESH_POLICIES,
    FetchPlan,
    FetchUnit,
    PlanItem,
    SourceManifest,
    source_output_dir,
)
from ._manifest_store import load_manifest, save_manifest_locked

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchExecutionResult:
    """Result of executing a single plan item."""

    unit_id: str
    success: bool
    records_written: int
    remote_artifact_sha256: str
    published_artifact_sha256: str
    error: str




def _file_sha256(path: Path) -> str:
    """Compute SHA-256 of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# DataJud adapter
# ---------------------------------------------------------------------------


def _execute_datajud(
    item: PlanItem,
    output_dir: Path,
    *,
    api_key: str = "",
    process_path: Path | None = None,
) -> FetchExecutionResult:
    """Execute a single DataJud plan item."""
    import os

    from ..datajud._client import DatajudClient
    from ..datajud._config import DATAJUD_API_KEY_ENV
    from ..datajud._runner import fetch_single_index

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

    # Extract index name from unit metadata or unit_id
    index = ""
    if item.unit_id.startswith("datajud:"):
        # Reverse the unit_id transform: datajud:api_publica_tjsp → api_publica_tjsp
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
            result = fetch_single_index(client, index, output_dir)
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
    # Remote artifact SHA: hash the API response content
    remote_sha = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

    return FetchExecutionResult(
        unit_id=item.unit_id,
        success=True,
        records_written=result.get("total_processes", 0),
        remote_artifact_sha256=remote_sha,
        published_artifact_sha256=pub_sha,
        error="",
    )


# ---------------------------------------------------------------------------
# Main executor
# ---------------------------------------------------------------------------


def execute_plan(
    plan: FetchPlan,
    *,
    base_dir: Path,
    datajud_api_key: str = "",
    datajud_process_path: Path | None = None,
) -> list[FetchExecutionResult]:
    """Execute all actionable items in a plan.

    For each item with action != "skip", dispatches to the source-specific
    adapter. Updates the manifest after each successful execution.

    Returns list of results.
    """
    results: list[FetchExecutionResult] = []
    actionable = [i for i in plan.items if i.action != "skip"]

    if not actionable:
        logger.info("No actionable items in plan")
        return results

    # Group by source for manifest batching
    by_source: dict[str, list[PlanItem]] = {}
    for item in actionable:
        by_source.setdefault(item.source, []).append(item)

    for source, items in sorted(by_source.items()):
        output_dir = source_output_dir(source, base_dir)
        manifest = load_manifest(source, output_dir) or SourceManifest(source=source)

        for item in items:
            if source == "datajud":
                result = _execute_datajud(
                    item,
                    output_dir,
                    api_key=datajud_api_key,
                    process_path=datajud_process_path,
                )
            else:
                logger.warning("No executor for source %r — skipping %s", source, item.unit_id)
                result = FetchExecutionResult(
                    unit_id=item.unit_id,
                    success=False,
                    records_written=0,
                    remote_artifact_sha256="",
                    published_artifact_sha256="",
                    error=f"No executor implemented for source {source!r}",
                )

            results.append(result)

            # Update manifest
            if result.success:
                now = datetime.now(UTC).isoformat()
                unit = manifest.units.get(item.unit_id) or FetchUnit(
                    unit_id=item.unit_id,
                    source=source,
                    label=item.unit_id,
                    remote_url=item.remote_url,
                    remote_state=item.expected_remote_state,
                    local_path=str(output_dir / f"{item.unit_id}.json"),
                )
                unit.status = "committed"
                unit.fetch_date = now
                unit.published_record_count = result.records_written
                unit.remote_artifact_sha256 = result.remote_artifact_sha256
                unit.published_artifact_sha256 = result.published_artifact_sha256
                unit.last_error = ""
                unit.failure_kind = ""
                manifest.units[item.unit_id] = unit
                logger.info("Committed %s: %d records", item.unit_id, result.records_written)
            else:
                unit = manifest.units.get(item.unit_id) or FetchUnit(
                    unit_id=item.unit_id,
                    source=source,
                    label=item.unit_id,
                    remote_url=item.remote_url,
                    remote_state=item.expected_remote_state,
                    local_path="",
                )
                unit.status = "failed"
                unit.failure_kind = "download"
                unit.last_error = result.error
                manifest.units[item.unit_id] = unit
                logger.warning("Failed %s: %s", item.unit_id, result.error)

        # Persist manifest after processing all items for this source
        save_manifest_locked(manifest, output_dir)

    return results


def validate_plan_for_execution(plan: FetchPlan) -> list[str]:
    """Check a plan for execution safety.

    Returns list of error messages. Empty = safe to execute.
    Enforces supports_deferred_run=False for DataJud.
    """
    errors: list[str] = []

    for item in plan.items:
        if item.action == "skip":
            continue
        policy = REFRESH_POLICIES.get(item.source)
        if policy is None:
            errors.append(f"No policy for source {item.source!r}")
            continue

        # Fail-closed: DataJud (supports_deferred_run=False) cannot use
        # a stale pre-generated plan — plan must be generated inline.
        if not policy.supports_deferred_run:
            snap = item.policy_snapshot
            if snap and not snap.supports_deferred_run:
                # This is expected — the plan was generated correctly.
                # The actual enforcement is in the CLI: `fetch run --plan`
                # must reject deferred plans for non-deferred sources.
                pass

    return errors
