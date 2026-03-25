"""Tests for fetch manifest model — types, serialisation, policies."""

from __future__ import annotations

import json

import pytest

from atlas_stf.fetch._manifest_model import (
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
    serialize_manifest,
    serialize_plan,
)

# ---------------------------------------------------------------------------
# build_unit_id
# ---------------------------------------------------------------------------


class TestBuildUnitId:
    def test_simple(self) -> None:
        assert build_unit_id("tse_donations", "2022") == "tse_donations:2022"

    def test_multiple_parts(self) -> None:
        assert build_unit_id("cgu", "ceis", "20260323") == "cgu:ceis:20260323"

    def test_normalised_to_lowercase(self) -> None:
        assert build_unit_id("TSE_DONATIONS", "2022") == "tse_donations:2022"

    def test_strips_whitespace(self) -> None:
        assert build_unit_id(" cvm ", " sanctions ") == "cvm:sanctions"

    def test_rejects_empty_segment(self) -> None:
        with pytest.raises(ValueError, match="Empty segment"):
            build_unit_id("cvm", "")

    def test_rejects_special_chars(self) -> None:
        with pytest.raises(ValueError, match="Invalid unit_id"):
            build_unit_id("cgu", "ceis/2024")

    def test_stable_across_calls(self) -> None:
        a = build_unit_id("rfb", "socios_pass1", "0")
        b = build_unit_id("rfb", "socios_pass1", "0")
        assert a == b


# ---------------------------------------------------------------------------
# RemoteState.matches
# ---------------------------------------------------------------------------


class TestRemoteStateMatches:
    def test_etag_strong_match(self) -> None:
        stored = RemoteState(url="x", etag='"abc"')
        fresh = RemoteState(url="x", etag='"abc"')
        result = stored.matches(fresh, ("etag",))
        assert result == MatchResult(matched=True, comparator_used="etag", confidence="strong")

    def test_etag_no_match(self) -> None:
        stored = RemoteState(url="x", etag='"abc"')
        fresh = RemoteState(url="x", etag='"def"')
        result = stored.matches(fresh, ("etag",))
        assert result.matched is False
        assert result.confidence == "none"

    def test_size_weak_match(self) -> None:
        stored = RemoteState(url="x", content_length=1000)
        fresh = RemoteState(url="x", content_length=1000)
        result = stored.matches(fresh, ("size",))
        assert result.matched is True
        assert result.confidence == "weak"

    def test_content_length_date_strong(self) -> None:
        stored = RemoteState(url="x", content_length=500, last_modified="Mon, 01 Jan 2024")
        fresh = RemoteState(url="x", content_length=500, last_modified="Mon, 01 Jan 2024")
        result = stored.matches(fresh, ("content_length_date",))
        assert result.matched is True
        assert result.confidence == "strong"

    def test_content_length_date_weak_no_date(self) -> None:
        stored = RemoteState(url="x", content_length=500)
        fresh = RemoteState(url="x", content_length=500)
        result = stored.matches(fresh, ("content_length_date",))
        assert result.matched is True
        assert result.confidence == "weak"

    def test_version_string_match(self) -> None:
        stored = RemoteState(url="x", version_string="v2.1")
        fresh = RemoteState(url="x", version_string="v2.1")
        result = stored.matches(fresh, ("version_string",))
        assert result.matched is True
        assert result.confidence == "strong"

    def test_no_comparator_data(self) -> None:
        stored = RemoteState(url="x")
        fresh = RemoteState(url="x")
        result = stored.matches(fresh, ("etag",))
        assert result.matched is False
        assert result.confidence == "none"

    def test_fallback_chain(self) -> None:
        """First comparator has no data, second matches."""
        stored = RemoteState(url="x", content_length=1000)
        fresh = RemoteState(url="x", content_length=1000)
        result = stored.matches(fresh, ("etag", "size"))
        assert result.matched is True
        assert result.comparator_used == "size"


# ---------------------------------------------------------------------------
# PolicySnapshot
# ---------------------------------------------------------------------------


class TestPolicySnapshot:
    def test_from_policy(self) -> None:
        policy = RefreshPolicy("cgu", ("content_length_date",), "24h", allow_weak_skip=False)
        snap = PolicySnapshot.from_policy(policy)
        assert snap.source == "cgu"
        assert snap.comparators == ("content_length_date",)
        assert snap.allow_weak_skip is False

    def test_roundtrip(self) -> None:
        snap = PolicySnapshot("tse_donations", ("etag", "size"), "monthly", True, True)
        d = snap.to_dict()
        restored = PolicySnapshot.from_dict(d)
        assert restored == snap


# ---------------------------------------------------------------------------
# FetchUnit serialisation
# ---------------------------------------------------------------------------


class TestFetchUnit:
    def test_roundtrip(self) -> None:
        unit = FetchUnit(
            unit_id="tse_donations:2022",
            source="tse_donations",
            label="TSE donations 2022",
            remote_url="https://example.com/2022.zip",
            remote_state=RemoteState(url="https://example.com/2022.zip", etag='"abc"', content_length=5000),
            status="committed",
            published_record_count=42,
            metadata={"extra": True},
        )
        d = unit.to_dict()
        restored = FetchUnit.from_dict(d)
        assert restored.unit_id == unit.unit_id
        assert restored.status == "committed"
        assert restored.published_record_count == 42
        assert restored.metadata == {"extra": True}

    def test_optional_fields_omitted(self) -> None:
        unit = FetchUnit(
            unit_id="cvm:sanctions",
            source="cvm",
            label="CVM",
            remote_url="",
            remote_state=RemoteState(url=""),
        )
        d = unit.to_dict()
        assert "remote_artifact_sha256" not in d
        assert "published_artifact_sha256" not in d
        assert "metadata" not in d


# ---------------------------------------------------------------------------
# PlanItem serialisation
# ---------------------------------------------------------------------------


class TestPlanItem:
    def test_roundtrip(self) -> None:
        item = PlanItem(
            unit_id="cgu:ceis",
            action="download",
            reason="new unit",
            source="cgu",
            remote_url="https://example.com/ceis",
            expected_remote_state=RemoteState(url="https://example.com/ceis", content_length=1000),
            policy_snapshot=PolicySnapshot("cgu", ("content_length_date",), "24h", False, True),
        )
        d = item.to_dict()
        restored = PlanItem.from_dict(d)
        assert restored == item


# ---------------------------------------------------------------------------
# FetchPlan
# ---------------------------------------------------------------------------


class TestFetchPlan:
    def test_deterministic_serialisation(self) -> None:
        snap_b = PolicySnapshot("b", (), "monthly", True, True)
        snap_a = PolicySnapshot("a", (), "monthly", True, True)
        items = [
            PlanItem("b:1", "skip", "ok", "b", "", RemoteState(url=""), snap_b),
            PlanItem("a:1", "download", "new", "a", "", RemoteState(url=""), snap_a),
        ]
        plan = FetchPlan(
            plan_id=compute_plan_id(["b", "a"], items),
            sources=["b", "a"],
            items=items,
        )
        s1 = serialize_plan(plan)
        s2 = serialize_plan(plan)
        assert s1 == s2

        # Sources sorted
        parsed = json.loads(s1)
        assert parsed["sources"] == ["a", "b"]
        # Items sorted by source, unit_id
        assert parsed["items"][0]["unit_id"] == "a:1"
        assert parsed["items"][1]["unit_id"] == "b:1"

    def test_plan_id_excludes_created_at(self) -> None:
        snap = PolicySnapshot("x", (), "monthly", True, True)
        items = [PlanItem("x:1", "download", "new", "x", "", RemoteState(url=""), snap)]
        id1 = compute_plan_id(["x"], items)
        id2 = compute_plan_id(["x"], items)
        assert id1 == id2

    def test_plan_id_changes_with_content(self) -> None:
        snap = PolicySnapshot("x", (), "monthly", True, True)
        items_a = [PlanItem("x:1", "download", "new", "x", "", RemoteState(url=""), snap)]
        items_b = [PlanItem("x:1", "skip", "ok", "x", "", RemoteState(url=""), snap)]
        assert compute_plan_id(["x"], items_a) != compute_plan_id(["x"], items_b)

    def test_roundtrip(self) -> None:
        plan = FetchPlan(
            plan_id="abc123",
            schema_version=PLAN_SCHEMA_VERSION,
            created_at="2024-01-01T00:00:00",
            sources=["tse_donations"],
            items=[],
        )
        d = plan.to_dict()
        restored = FetchPlan.from_dict(d)
        assert restored.plan_id == plan.plan_id
        assert restored.schema_version == plan.schema_version


# ---------------------------------------------------------------------------
# SourceManifest serialisation
# ---------------------------------------------------------------------------


class TestSourceManifest:
    def test_deterministic_unit_order(self) -> None:
        m = SourceManifest(
            source="tse_donations",
            units={
                "tse_donations:2024": FetchUnit("tse_donations:2024", "tse_donations", "2024", "", RemoteState(url="")),
                "tse_donations:2002": FetchUnit("tse_donations:2002", "tse_donations", "2002", "", RemoteState(url="")),
            },
        )
        s = serialize_manifest(m)
        parsed = json.loads(s)
        keys = list(parsed["units"])
        assert keys == ["tse_donations:2002", "tse_donations:2024"]

    def test_roundtrip(self) -> None:
        m = SourceManifest(
            source="cvm",
            units={
                "cvm:sanctions": FetchUnit(
                    "cvm:sanctions", "cvm", "CVM", "url", RemoteState(url="url", etag="e"), status="committed"
                ),
            },
        )
        d = m.to_dict()
        restored = SourceManifest.from_dict(d)
        assert restored.source == "cvm"
        assert "cvm:sanctions" in restored.units
        assert restored.units["cvm:sanctions"].status == "committed"


# ---------------------------------------------------------------------------
# Policies completeness
# ---------------------------------------------------------------------------


class TestPolicies:
    def test_all_sources_have_policy(self) -> None:
        expected = {"tse_donations", "tse_expenses", "tse_party_org", "cgu", "cvm", "rfb", "datajud"}
        assert set(REFRESH_POLICIES) == expected

    def test_datajud_no_deferred_run(self) -> None:
        assert REFRESH_POLICIES["datajud"].supports_deferred_run is False

    def test_cgu_no_weak_skip(self) -> None:
        assert REFRESH_POLICIES["cgu"].allow_weak_skip is False

    def test_rfb_no_weak_skip(self) -> None:
        assert REFRESH_POLICIES["rfb"].allow_weak_skip is False
