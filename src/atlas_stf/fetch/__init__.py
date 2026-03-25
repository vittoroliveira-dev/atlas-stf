"""Unified fetch manifest — single source of truth for data provenance."""

from __future__ import annotations

from ._adapter import FetchSourceAdapter, get_adapter, list_sources
from ._manifest_model import (
    PLAN_SCHEMA_VERSION,
    REFRESH_POLICIES,
    FetchPlan,
    FetchUnit,
    MatchResult,
    PlanItem,
    PolicySnapshot,
    RefreshPolicy,
    RemoteState,
    SourceManifest,
    build_unit_id,
    compute_plan_id,
)
from ._manifest_store import load_all_manifests, load_manifest, save_manifest_locked

__all__ = [
    "PLAN_SCHEMA_VERSION",
    "REFRESH_POLICIES",
    "FetchPlan",
    "FetchSourceAdapter",
    "FetchUnit",
    "MatchResult",
    "PlanItem",
    "PolicySnapshot",
    "RefreshPolicy",
    "RemoteState",
    "SourceManifest",
    "build_unit_id",
    "compute_plan_id",
    "get_adapter",
    "list_sources",
    "load_all_manifests",
    "load_manifest",
    "save_manifest_locked",
]
