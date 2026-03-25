"""Tests for fetch executor — DataJud adapter and plan execution."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas_stf.fetch._executor import (
    execute_plan,
    validate_plan_for_execution,
)
from atlas_stf.fetch._manifest_model import (
    FetchPlan,
    PlanItem,
    PolicySnapshot,
    RemoteState,
    compute_plan_id,
)
from atlas_stf.fetch._manifest_store import load_manifest


def _make_datajud_plan(unit_ids: list[str]) -> FetchPlan:
    snap = PolicySnapshot("datajud", ("version_string",), "weekly", True, False)
    items = [
        PlanItem(
            unit_id=uid,
            action="download",
            reason="new unit",
            source="datajud",
            remote_url="",
            expected_remote_state=RemoteState(url=""),
            policy_snapshot=snap,
        )
        for uid in unit_ids
    ]
    return FetchPlan(
        plan_id=compute_plan_id(["datajud"], items),
        sources=["datajud"],
        items=items,
    )


class TestExecuteDatajud:
    @patch("atlas_stf.datajud._client.DatajudClient")
    def test_single_index(self, mock_cls: MagicMock, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.search.return_value = {
            "hits": {"total": {"value": 42}},
            "aggregations": {
                "top_assuntos": {"buckets": []},
                "top_orgaos": {"buckets": []},
                "classes": {"buckets": []},
            },
        }
        mock_cls.return_value = mock_client

        plan = _make_datajud_plan(["datajud:api_publica_stj"])
        results = execute_plan(
            plan,
            base_dir=tmp_path,
            datajud_api_key="test_key",
        )

        assert len(results) == 1
        assert results[0].success is True
        assert results[0].records_written == 42

        # JSON output written
        out = tmp_path / "datajud" / "api_publica_stj.json"
        assert out.exists()
        data = json.loads(out.read_text("utf-8"))
        assert data["total_processes"] == 42

        # Manifest updated
        manifest = load_manifest("datajud", tmp_path / "datajud")
        assert manifest is not None
        assert "datajud:api_publica_stj" in manifest.units
        unit = manifest.units["datajud:api_publica_stj"]
        assert unit.status == "committed"
        assert unit.published_record_count == 42
        assert unit.published_artifact_sha256 != ""

    def test_no_api_key_fails(self, tmp_path: Path) -> None:
        plan = _make_datajud_plan(["datajud:api_publica_stj"])
        with patch.dict("os.environ", {}, clear=True):
            results = execute_plan(plan, base_dir=tmp_path, datajud_api_key="")

        assert len(results) == 1
        assert results[0].success is False
        assert "API key" in results[0].error

    @patch("atlas_stf.datajud._client.DatajudClient")
    def test_api_error_records_failure(self, mock_cls: MagicMock, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.search.side_effect = RuntimeError("API timeout")
        mock_cls.return_value = mock_client

        plan = _make_datajud_plan(["datajud:api_publica_stj"])
        results = execute_plan(plan, base_dir=tmp_path, datajud_api_key="key")

        assert len(results) == 1
        assert results[0].success is False
        assert "API timeout" in results[0].error

        # Manifest records failure
        manifest = load_manifest("datajud", tmp_path / "datajud")
        assert manifest is not None
        unit = manifest.units["datajud:api_publica_stj"]
        assert unit.status == "failed"

    def test_skip_items_not_executed(self, tmp_path: Path) -> None:
        snap = PolicySnapshot("datajud", ("version_string",), "weekly", True, False)
        items = [
            PlanItem("datajud:x", "skip", "cached", "datajud", "", RemoteState(url=""), snap),
        ]
        plan = FetchPlan(
            plan_id=compute_plan_id(["datajud"], items),
            sources=["datajud"],
            items=items,
        )
        results = execute_plan(plan, base_dir=tmp_path, datajud_api_key="key")
        assert results == []


class TestExecuteUnknownSource:
    def test_unknown_source_fails_gracefully(self, tmp_path: Path) -> None:
        snap = PolicySnapshot("unknown", (), "monthly", True, True)
        items = [
            PlanItem("unknown:x", "download", "new", "unknown", "", RemoteState(url=""), snap),
        ]
        plan = FetchPlan(
            plan_id=compute_plan_id(["unknown"], items),
            sources=["unknown"],
            items=items,
        )
        results = execute_plan(plan, base_dir=tmp_path)
        assert len(results) == 1
        assert results[0].success is False
        assert "No executor" in results[0].error


class TestValidatePlan:
    def test_empty_plan_valid(self) -> None:
        plan = FetchPlan(plan_id="x", items=[])
        assert validate_plan_for_execution(plan) == []

    def test_skip_items_ignored(self) -> None:
        snap = PolicySnapshot("datajud", ("version_string",), "weekly", True, False)
        items = [PlanItem("datajud:x", "skip", "ok", "datajud", "", RemoteState(url=""), snap)]
        plan = FetchPlan(plan_id="x", items=items)
        assert validate_plan_for_execution(plan) == []
