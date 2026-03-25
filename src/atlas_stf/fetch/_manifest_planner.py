"""Plan generation — read-only, no side effects.

Compares discovered units against the current manifest to decide which
actions are needed.  The resulting ``FetchPlan`` is a pure data structure
that can be serialised, reviewed, and optionally executed later.

Two entry points:

- ``generate_plan()`` — kwargs-based dispatch (backward compatible)
- ``generate_plan_from_adapters()`` — adapter-based (Phase 2B preferred)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from ._discovery import discover_units
from ._manifest_model import (
    PLAN_SCHEMA_VERSION,
    REFRESH_POLICIES,
    FetchPlan,
    FetchUnit,
    PlanAction,
    PlanItem,
    PolicySnapshot,
    RefreshPolicy,
    RemoteState,
    compute_plan_id,
    source_output_dir,
)
from ._manifest_store import load_manifest
from ._remote_probe import probe_remote_state

if TYPE_CHECKING:
    from ._adapter import FetchSourceAdapter

logger = logging.getLogger(__name__)

ProbeFunction = Callable[[str, FetchUnit, RefreshPolicy], RemoteState]


def generate_plan(
    *,
    sources: list[str] | None = None,
    base_dir: Path,
    force_refresh: bool = False,
    probe_fn: ProbeFunction | None = None,
    discovery_kwargs: dict[str, dict[str, object]] | None = None,
) -> FetchPlan:
    """Generate a fetch plan without writing anything to disk.

    Parameters
    ----------
    sources:
        Restrict to these sources; ``None`` means all known sources.
    base_dir:
        Root directory where manifests and data live (e.g. ``data/raw``).
    force_refresh:
        If ``True``, ignore freshness windows and re-probe everything.
    probe_fn:
        Optional override for remote probing (useful for testing).
    discovery_kwargs:
        Per-source keyword arguments forwarded to ``discover_units()``.
    """
    active_sources = sources or sorted(REFRESH_POLICIES)
    probe = probe_fn or probe_remote_state
    extra_kwargs = discovery_kwargs or {}

    items: list[PlanItem] = []
    plan_sources: list[str] = []

    for source in active_sources:
        policy = REFRESH_POLICIES.get(source)
        if policy is None:
            logger.warning("No policy for source %r, skipping", source)
            continue

        plan_sources.append(source)
        manifest = load_manifest(source, source_output_dir(source, base_dir))
        existing_units = manifest.units if manifest else {}

        kw = extra_kwargs.get(source, {})
        discovered = list(discover_units(source, output_dir=source_output_dir(source, base_dir), **kw))

        discovered_ids = {u.unit_id for u in discovered}

        # --- Discovered units: decide action ---
        for unit in discovered:
            existing = existing_units.get(unit.unit_id)
            action, reason, remote_state = _decide_action(
                unit=unit,
                existing=existing,
                policy=policy,
                force_refresh=force_refresh,
                probe=probe,
            )
            items.append(
                PlanItem(
                    unit_id=unit.unit_id,
                    action=action,
                    reason=reason,
                    source=source,
                    remote_url=unit.remote_url,
                    expected_remote_state=remote_state,
                    policy_snapshot=PolicySnapshot.from_policy(policy),
                )
            )

        # --- Units in manifest but NOT discovered → repair ---
        for uid, existing in existing_units.items():
            if uid not in discovered_ids:
                items.append(
                    PlanItem(
                        unit_id=uid,
                        action="repair",
                        reason="unit present in manifest but not discovered (drift)",
                        source=source,
                        remote_url=existing.remote_url,
                        expected_remote_state=existing.remote_state,
                        policy_snapshot=PolicySnapshot.from_policy(policy),
                    )
                )

    plan_id = compute_plan_id(plan_sources, items)
    return FetchPlan(
        plan_id=plan_id,
        schema_version=PLAN_SCHEMA_VERSION,
        created_at=datetime.now(UTC).isoformat(),
        sources=plan_sources,
        items=items,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------



def _decide_action(
    *,
    unit: FetchUnit,
    existing: FetchUnit | None,
    policy: RefreshPolicy,
    force_refresh: bool,
    probe: ProbeFunction,
) -> tuple[PlanAction, str, RemoteState]:
    """Return (action, reason, remote_state) for a single unit."""

    # New unit — always download
    if existing is None:
        remote_state = _safe_probe(unit, policy, probe)
        return "download", "new unit (not in manifest)", remote_state

    # Force refresh — always redownload
    if force_refresh:
        remote_state = _safe_probe(unit, policy, probe)
        return "redownload", "force_refresh requested", remote_state

    # Failed unit — repair or redownload depending on failure kind
    if existing.status == "failed":
        remote_state = _safe_probe(unit, policy, probe)
        if existing.failure_kind == "transform":
            return "repair", f"previous transform failure: {existing.last_error}", remote_state
        return "redownload", f"previous failure ({existing.failure_kind}): {existing.last_error}", remote_state

    # Committed unit — check freshness via probe
    remote_state = _safe_probe(unit, policy, probe)
    match_result = existing.remote_state.matches(remote_state, policy.comparators)

    if match_result.confidence == "none":
        return "redownload", "no comparator matched (confidence=none)", remote_state

    if match_result.confidence == "weak" and not policy.allow_weak_skip:
        return "redownload", "weak match but allow_weak_skip=False", remote_state

    if match_result.matched:
        return "skip", f"unchanged ({match_result.comparator_used}, confidence={match_result.confidence})", remote_state

    return "redownload", f"remote changed ({match_result.comparator_used})", remote_state


def _safe_probe(unit: FetchUnit, policy: RefreshPolicy, probe: ProbeFunction) -> RemoteState:
    """Probe with error handling — never let a probe failure abort planning."""
    try:
        return probe(unit.source, unit, policy)
    except Exception:
        logger.warning("Probe failed for %s; using discovered state", unit.unit_id, exc_info=True)
        return unit.remote_state


# ---------------------------------------------------------------------------
# Adapter-based entry point (Phase 2B)
# ---------------------------------------------------------------------------


def generate_plan_from_adapters(
    adapters: list[FetchSourceAdapter],
    *,
    base_dir: Path,
    force_refresh: bool = False,
) -> FetchPlan:
    """Generate a fetch plan using explicit adapter instances.

    Preferred over ``generate_plan()`` when the caller controls adapter
    construction (e.g. with source-specific config).
    """
    items: list[PlanItem] = []
    plan_sources: list[str] = []

    for adapter in adapters:
        source = adapter.source_name
        policy = adapter.policy
        plan_sources.append(source)

        manifest = load_manifest(source, source_output_dir(source, base_dir))
        existing_units = manifest.units if manifest else {}

        discovered = list(adapter.discover_units())
        discovered_ids = {u.unit_id for u in discovered}

        def _adapter_probe(
            src: str, unit: FetchUnit, pol: RefreshPolicy, _a: FetchSourceAdapter = adapter
        ) -> RemoteState:
            return _a.probe_remote(unit)

        for unit in discovered:
            existing = existing_units.get(unit.unit_id)
            action, reason, remote_state = _decide_action(
                unit=unit,
                existing=existing,
                policy=policy,
                force_refresh=force_refresh,
                probe=_adapter_probe,
            )
            items.append(
                PlanItem(
                    unit_id=unit.unit_id,
                    action=action,
                    reason=reason,
                    source=source,
                    remote_url=unit.remote_url,
                    expected_remote_state=remote_state,
                    policy_snapshot=PolicySnapshot.from_policy(policy),
                )
            )

        for uid, existing in existing_units.items():
            if uid not in discovered_ids:
                items.append(
                    PlanItem(
                        unit_id=uid,
                        action="repair",
                        reason="unit present in manifest but not discovered (drift)",
                        source=source,
                        remote_url=existing.remote_url,
                        expected_remote_state=existing.remote_state,
                        policy_snapshot=PolicySnapshot.from_policy(policy),
                    )
                )

    plan_id = compute_plan_id(plan_sources, items)
    return FetchPlan(
        plan_id=plan_id,
        schema_version=PLAN_SCHEMA_VERSION,
        created_at=datetime.now(UTC).isoformat(),
        sources=plan_sources,
        items=items,
    )
