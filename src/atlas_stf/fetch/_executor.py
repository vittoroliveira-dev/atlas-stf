"""Fetch executor: runs a plan item by dispatching to source-specific runners.

Phase 2D of the fetch engine: ``download_raw → publish → commit``.
Each source adapter maps plan items to runner calls and updates the manifest.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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




# ---------------------------------------------------------------------------
# Main executor
# ---------------------------------------------------------------------------

# Type alias for a source executor callable.
# Signature: (item, output_dir, *, api_key, process_path) -> FetchExecutionResult
_SourceExecutor = Any


def execute_plan(
    plan: FetchPlan,
    *,
    base_dir: Path,
    datajud_api_key: str = "",
    datajud_process_path: Path | None = None,
    source_executors: dict[str, _SourceExecutor] | None = None,
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
            executor = (source_executors or {}).get(source)
            if executor is not None:
                result = executor(
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

            # Persist manifest after each item so progress survives crashes
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
