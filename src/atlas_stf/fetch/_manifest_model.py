"""Fetch manifest domain model — types, policies, serialisation.

Every type here is a pure data structure with no I/O.  Serialisation
helpers produce deterministic JSON suitable for fingerprinting.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

# ---------------------------------------------------------------------------
# Literal types
# ---------------------------------------------------------------------------

ComparatorType = Literal["etag", "size", "content_length_date", "version_string"]
FreshnessWindow = Literal["always_check", "24h", "weekly", "monthly"]
UnitStatus = Literal["pending", "downloaded", "committed", "failed"]
PlanAction = Literal["skip", "download", "redownload", "repair"]
FailureKind = Literal["probe", "download", "transform", ""]
MatchConfidence = Literal["strong", "weak", "none"]

# ---------------------------------------------------------------------------
# unit_id helpers
# ---------------------------------------------------------------------------

_UNIT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_]*(?::[a-z0-9][a-z0-9_]*)*$")


def build_unit_id(source: str, *parts: str) -> str:
    """Build a canonical ``source:part1:part2`` unit id.

    All segments are lowercased, stripped, and validated against the
    ``[a-z0-9_]`` alphabet.  Empty parts are rejected.
    """
    segments = [source.strip().lower(), *(p.strip().lower() for p in parts)]
    if any(not s for s in segments):
        msg = f"Empty segment in unit_id: {segments!r}"
        raise ValueError(msg)
    uid = ":".join(segments)
    if not _UNIT_ID_RE.match(uid):
        msg = f"Invalid unit_id '{uid}' — only [a-z0-9_] separated by ':'"
        raise ValueError(msg)
    return uid


# ---------------------------------------------------------------------------
# Source → subdirectory mapping (single source of truth)
# ---------------------------------------------------------------------------

# Maps logical source names to filesystem subdirectories under base_dir.
# TSE splits into three logical sources that share one physical directory.
SOURCE_SUBDIRS: dict[str, str] = {
    "tse_donations": "tse",
    "tse_expenses": "tse",
    "tse_party_org": "tse",
    "cgu": "cgu",
    "cvm": "cvm",
    "rfb": "rfb",
    "datajud": "datajud",
}


def source_output_dir(source: str, base_dir: Path) -> Path:
    """Resolve the output directory for a logical source under *base_dir*."""
    subdir = SOURCE_SUBDIRS.get(source, source)
    return base_dir / subdir


# ---------------------------------------------------------------------------
# MatchResult
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatchResult:
    """Outcome of comparing local vs. remote state."""

    matched: bool
    comparator_used: ComparatorType | None
    confidence: MatchConfidence


# ---------------------------------------------------------------------------
# RefreshPolicy
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RefreshPolicy:
    """Per-source rules governing freshness checks and skipping logic."""

    source: str
    comparators: tuple[ComparatorType, ...]
    freshness_window: FreshnessWindow
    force_refresh_supported: bool = True
    allow_weak_skip: bool = True
    supports_deferred_run: bool = True


# ---------------------------------------------------------------------------
# PolicySnapshot — minimal audit record embedded in PlanItem
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PolicySnapshot:
    """Immutable snapshot of the policy fields that influenced a plan decision."""

    source: str
    comparators: tuple[ComparatorType, ...]
    freshness_window: FreshnessWindow
    allow_weak_skip: bool
    supports_deferred_run: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "comparators": list(self.comparators),
            "freshness_window": self.freshness_window,
            "allow_weak_skip": self.allow_weak_skip,
            "supports_deferred_run": self.supports_deferred_run,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PolicySnapshot:
        return cls(
            source=data["source"],
            comparators=tuple(data["comparators"]),
            freshness_window=data["freshness_window"],
            allow_weak_skip=data["allow_weak_skip"],
            supports_deferred_run=data["supports_deferred_run"],
        )

    @classmethod
    def from_policy(cls, policy: RefreshPolicy) -> PolicySnapshot:
        return cls(
            source=policy.source,
            comparators=policy.comparators,
            freshness_window=policy.freshness_window,
            allow_weak_skip=policy.allow_weak_skip,
            supports_deferred_run=policy.supports_deferred_run,
        )


# ---------------------------------------------------------------------------
# RemoteState
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RemoteState:
    """Snapshot of remote artifact metadata obtained via HEAD / probe."""

    url: str
    etag: str = ""
    content_length: int = 0
    last_modified: str = ""
    version_string: str = ""
    probed_at: str = ""

    def matches(
        self,
        other: RemoteState,
        comparators: tuple[ComparatorType, ...],
    ) -> MatchResult:
        """Compare *self* (stored) against *other* (fresh probe)."""
        for comp in comparators:
            if comp == "etag":
                if self.etag and other.etag:
                    matched = self.etag == other.etag
                    return MatchResult(
                        matched=matched, comparator_used="etag", confidence="strong" if matched else "none"
                    )
            elif comp == "size":
                if self.content_length > 0 and other.content_length > 0:
                    matched = self.content_length == other.content_length
                    return MatchResult(
                        matched=matched, comparator_used="size", confidence="weak" if matched else "none"
                    )
            elif comp == "content_length_date":
                if self.content_length > 0 and other.content_length > 0 and self.last_modified and other.last_modified:
                    matched = self.content_length == other.content_length and self.last_modified == other.last_modified
                    conf: MatchConfidence = "strong" if matched else "none"
                    return MatchResult(matched=matched, comparator_used="content_length_date", confidence=conf)
                if self.content_length > 0 and other.content_length > 0:
                    matched = self.content_length == other.content_length
                    return MatchResult(
                        matched=matched, comparator_used="content_length_date", confidence="weak" if matched else "none"
                    )
            elif comp == "version_string":
                if self.version_string and other.version_string:
                    matched = self.version_string == other.version_string
                    return MatchResult(
                        matched=matched, comparator_used="version_string", confidence="strong" if matched else "none"
                    )
        return MatchResult(matched=False, comparator_used=None, confidence="none")

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"url": self.url}
        if self.etag:
            d["etag"] = self.etag
        if self.content_length:
            d["content_length"] = self.content_length
        if self.last_modified:
            d["last_modified"] = self.last_modified
        if self.version_string:
            d["version_string"] = self.version_string
        if self.probed_at:
            d["probed_at"] = self.probed_at
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RemoteState:
        return cls(
            url=data.get("url", ""),
            etag=data.get("etag", ""),
            content_length=int(data.get("content_length", 0)),
            last_modified=data.get("last_modified", ""),
            version_string=data.get("version_string", ""),
            probed_at=data.get("probed_at", ""),
        )


# ---------------------------------------------------------------------------
# FetchUnit
# ---------------------------------------------------------------------------


@dataclass
class FetchUnit:
    """Tracks a single downloadable artifact within a source manifest."""

    unit_id: str
    source: str
    label: str
    remote_url: str
    remote_state: RemoteState
    local_path: str = ""
    status: UnitStatus = "pending"
    failure_kind: FailureKind = ""
    fetch_date: str = ""
    published_record_count: int = 0
    last_error: str = ""
    remote_artifact_sha256: str = ""
    published_artifact_sha256: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "unit_id": self.unit_id,
            "source": self.source,
            "label": self.label,
            "remote_url": self.remote_url,
            "remote_state": self.remote_state.to_dict(),
            "local_path": self.local_path,
            "status": self.status,
            "failure_kind": self.failure_kind,
            "fetch_date": self.fetch_date,
            "published_record_count": self.published_record_count,
            "last_error": self.last_error,
        }
        if self.remote_artifact_sha256:
            d["remote_artifact_sha256"] = self.remote_artifact_sha256
        if self.published_artifact_sha256:
            d["published_artifact_sha256"] = self.published_artifact_sha256
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FetchUnit:
        return cls(
            unit_id=data["unit_id"],
            source=data["source"],
            label=data["label"],
            remote_url=data.get("remote_url", ""),
            remote_state=RemoteState.from_dict(data.get("remote_state", {"url": ""})),
            local_path=data.get("local_path", ""),
            status=data.get("status", "pending"),
            failure_kind=data.get("failure_kind", ""),
            fetch_date=data.get("fetch_date", ""),
            published_record_count=int(data.get("published_record_count", 0)),
            last_error=data.get("last_error", ""),
            remote_artifact_sha256=data.get("remote_artifact_sha256", ""),
            published_artifact_sha256=data.get("published_artifact_sha256", ""),
            metadata=data.get("metadata", {}),
        )


# ---------------------------------------------------------------------------
# PlanItem
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlanItem:
    """A single action in a fetch plan, with full audit context."""

    unit_id: str
    action: PlanAction
    reason: str
    source: str
    remote_url: str
    expected_remote_state: RemoteState
    policy_snapshot: PolicySnapshot

    def to_dict(self) -> dict[str, Any]:
        return {
            "unit_id": self.unit_id,
            "action": self.action,
            "reason": self.reason,
            "source": self.source,
            "remote_url": self.remote_url,
            "expected_remote_state": self.expected_remote_state.to_dict(),
            "policy_snapshot": self.policy_snapshot.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanItem:
        return cls(
            unit_id=data["unit_id"],
            action=data["action"],
            reason=data["reason"],
            source=data["source"],
            remote_url=data.get("remote_url", ""),
            expected_remote_state=RemoteState.from_dict(data.get("expected_remote_state", {"url": ""})),
            policy_snapshot=PolicySnapshot.from_dict(data["policy_snapshot"]),
        )


# ---------------------------------------------------------------------------
# FetchPlan
# ---------------------------------------------------------------------------

PLAN_SCHEMA_VERSION = "1"


@dataclass
class FetchPlan:
    """Immutable execution plan — the full set of actions to perform."""

    plan_id: str
    schema_version: str = PLAN_SCHEMA_VERSION
    created_at: str = ""
    sources: list[str] = field(default_factory=list)
    items: list[PlanItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "schema_version": self.schema_version,
            "created_at": self.created_at,
            "sources": sorted(self.sources),
            "items": [item.to_dict() for item in sorted(self.items, key=lambda i: (i.source, i.unit_id))],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FetchPlan:
        return cls(
            plan_id=data["plan_id"],
            schema_version=data.get("schema_version", PLAN_SCHEMA_VERSION),
            created_at=data.get("created_at", ""),
            sources=data.get("sources", []),
            items=[PlanItem.from_dict(i) for i in data.get("items", [])],
        )


def compute_plan_id(sources: list[str], items: list[PlanItem]) -> str:
    """Derive plan_id from canonical payload (sources + sorted items).

    Excludes volatile fields (``probed_at``, ``reason``) so that the same
    logical plan (same units, same actions) produces the same plan_id
    regardless of when the probe ran or how the reason text was phrased.
    """

    def _stable_item(item: PlanItem) -> dict[str, Any]:
        rs = item.expected_remote_state
        stable_rs: dict[str, Any] = {"url": rs.url}
        if rs.etag:
            stable_rs["etag"] = rs.etag
        if rs.content_length:
            stable_rs["content_length"] = rs.content_length
        if rs.last_modified:
            stable_rs["last_modified"] = rs.last_modified
        if rs.version_string:
            stable_rs["version_string"] = rs.version_string
        # probed_at excluded — volatile
        return {
            "unit_id": item.unit_id,
            "action": item.action,
            "source": item.source,
            "remote_url": item.remote_url,
            "expected_remote_state": stable_rs,
            # reason excluded — textual, may vary
        }

    canonical = {
        "sources": sorted(sources),
        "items": [_stable_item(i) for i in sorted(items, key=lambda i: (i.source, i.unit_id))],
    }
    blob = json.dumps(canonical, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(blob).hexdigest()


# ---------------------------------------------------------------------------
# SourceManifest
# ---------------------------------------------------------------------------


@dataclass
class SourceManifest:
    """Per-source manifest — the single source of truth after migration."""

    source: str
    schema_version: str = "2.0"
    last_updated: str = ""
    units: dict[str, FetchUnit] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        ordered_units = dict(sorted(self.units.items()))
        return {
            "source": self.source,
            "schema_version": self.schema_version,
            "last_updated": self.last_updated,
            "units": {uid: u.to_dict() for uid, u in ordered_units.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceManifest:
        units: dict[str, FetchUnit] = {}
        for uid, udata in data.get("units", {}).items():
            units[uid] = FetchUnit.from_dict(udata)
        return cls(
            source=data["source"],
            schema_version=data.get("schema_version", "2.0"),
            last_updated=data.get("last_updated", ""),
            units=units,
        )


# ---------------------------------------------------------------------------
# Deterministic serialisation
# ---------------------------------------------------------------------------


def serialize_manifest(manifest: SourceManifest) -> str:
    """Serialise a manifest to deterministic JSON."""
    return json.dumps(manifest.to_dict(), sort_keys=True, ensure_ascii=False, indent=2)


def serialize_plan(plan: FetchPlan) -> str:
    """Serialise a plan to deterministic JSON."""
    return json.dumps(plan.to_dict(), sort_keys=True, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Refresh policies
# ---------------------------------------------------------------------------

REFRESH_POLICIES: dict[str, RefreshPolicy] = {
    "tse_donations": RefreshPolicy("tse_donations", ("etag", "size"), "monthly"),
    "tse_expenses": RefreshPolicy("tse_expenses", ("etag", "size"), "monthly"),
    "tse_party_org": RefreshPolicy("tse_party_org", ("etag", "size"), "monthly"),
    "cgu": RefreshPolicy("cgu", ("content_length_date",), "24h", allow_weak_skip=False),
    "cvm": RefreshPolicy("cvm", ("etag", "size"), "weekly"),
    "rfb": RefreshPolicy("rfb", ("size",), "monthly", allow_weak_skip=False),
    "datajud": RefreshPolicy("datajud", ("version_string",), "weekly", supports_deferred_run=False),
}
