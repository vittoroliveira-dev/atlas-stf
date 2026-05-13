from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.serving._builder_loaders import load_metrics
from atlas_stf.serving._builder_utils import _read_json, _read_jsonl, _validate_inputs


def test_read_json_returns_mapping(tmp_path: Path) -> None:
    path = tmp_path / "summary.json"
    path.write_text('{"alert_count": 1}', encoding="utf-8")

    assert _read_json(path) == {"alert_count": 1}


def test_read_json_invalid_reports_file_context(tmp_path: Path) -> None:
    path = tmp_path / "summary.json"
    path.write_text('{"alert_count": ', encoding="utf-8")

    with pytest.raises(json.JSONDecodeError, match=r"Invalid JSON at .*summary\.json"):
        _read_json(path)


def test_read_json_rejects_non_object(tmp_path: Path) -> None:
    path = tmp_path / "summary.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match=r"Expected JSON object at .*summary\.json"):
        _read_json(path)


def test_read_jsonl_reports_file_and_line_for_invalid_record(tmp_path: Path) -> None:
    path = tmp_path / "process.jsonl"
    path.write_text('{"process_id":"p1"}\n{"process_id": \n', encoding="utf-8")

    with pytest.raises(json.JSONDecodeError, match=r"Invalid JSONL record at .*process\.jsonl:2"):
        list(_read_jsonl(path))


def test_read_jsonl_rejects_non_object_record_with_context(tmp_path: Path) -> None:
    path = tmp_path / "process.jsonl"
    path.write_text('{"process_id":"p1"}\n[]\n', encoding="utf-8")

    with pytest.raises(ValueError, match=r"Expected JSON object at .*process\.jsonl:2"):
        list(_read_jsonl(path))


def test_read_jsonl_nominal_flow_unchanged(tmp_path: Path) -> None:
    path = tmp_path / "process.jsonl"
    rows = [{"process_id": "p1"}, {"process_id": "p2"}]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    assert list(_read_jsonl(path)) == rows


def test_load_metrics_fails_at_json_boundary_for_non_object_summary(tmp_path: Path) -> None:
    analytics_dir = tmp_path / "analytics"
    analytics_dir.mkdir(parents=True, exist_ok=True)
    (analytics_dir / "outlier_alert_summary.json").write_text("[]", encoding="utf-8")
    (analytics_dir / "comparison_group_summary.json").write_text('{"valid_group_count": 1}', encoding="utf-8")
    (analytics_dir / "baseline_summary.json").write_text('{"baseline_count": 1}', encoding="utf-8")

    with pytest.raises(ValueError, match=r"Expected JSON object at .*outlier_alert_summary\.json") as exc_info:
        load_metrics(analytics_dir)

    assert "AttributeError" not in str(exc_info.value)


def test_validate_inputs_fails_at_jsonl_boundary_for_invalid_record(tmp_path: Path) -> None:
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    curated_dir.mkdir(parents=True, exist_ok=True)
    analytics_dir.mkdir(parents=True, exist_ok=True)
    (curated_dir / "process.jsonl").write_text('{"process_id":"p1"}\n{"process_id": \n', encoding="utf-8")

    with pytest.raises(json.JSONDecodeError, match=r"Invalid JSONL record at .*process\.jsonl:2") as exc_info:
        _validate_inputs(curated_dir, analytics_dir)

    assert "AttributeError" not in str(exc_info.value)


def test_validate_inputs_fails_at_jsonl_boundary_for_non_object_record(tmp_path: Path) -> None:
    curated_dir = tmp_path / "curated"
    analytics_dir = tmp_path / "analytics"
    curated_dir.mkdir(parents=True, exist_ok=True)
    analytics_dir.mkdir(parents=True, exist_ok=True)
    (curated_dir / "process.jsonl").write_text('{"process_id":"p1"}\n[]\n', encoding="utf-8")

    with pytest.raises(ValueError, match=r"Expected JSON object at .*process\.jsonl:2") as exc_info:
        _validate_inputs(curated_dir, analytics_dir)

    assert "AttributeError" not in str(exc_info.value)
