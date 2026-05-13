"""Tests for the CLI fetch subcommands."""

from __future__ import annotations

import json
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from atlas_stf.cli import main


class TestFetchPlan:
    def test_plan_json_output(self, tmp_path: Path) -> None:
        buf = StringIO()
        with patch.object(sys, "stdout", buf):
            rc = main(["fetch", "plan", "--sources", "cvm", "--output-dir", str(tmp_path), "--json"])
        assert rc == 0
        data = json.loads(buf.getvalue())
        assert "plan_id" in data
        assert len(data["items"]) >= 1

    def test_plan_text_output(self, tmp_path: Path) -> None:
        buf = StringIO()
        with patch("builtins.print", lambda *a, **kw: buf.write(" ".join(str(x) for x in a) + "\n")):
            rc = main(["fetch", "plan", "--sources", "cvm", "--output-dir", str(tmp_path)])
        assert rc == 0
        assert "Plan" in buf.getvalue()

    def test_plan_with_invalid_datajud_process_jsonl_returns_error(self, tmp_path: Path, caplog) -> None:
        process_path = tmp_path / "process.jsonl"
        process_path.write_text('{"origin_court_or_body": "STJ"}\n{bad-json}\n', encoding="utf-8")

        rc = main(
            [
                "fetch",
                "plan",
                "--sources",
                "datajud",
                "--output-dir",
                str(tmp_path),
                "--process-path",
                str(process_path),
            ]
        )

        assert rc == 1
        assert "Failed to generate fetch plan:" in caplog.text
        assert f"{process_path}:2 contains invalid JSON" in caplog.text


class TestFetchStatus:
    def test_status_no_manifests(self, tmp_path: Path) -> None:
        buf = StringIO()
        with patch("builtins.print", lambda *a, **kw: buf.write(" ".join(str(x) for x in a) + "\n")):
            rc = main(["fetch", "status", "--sources", "cvm", "--output-dir", str(tmp_path)])
        assert rc == 0
        assert "no manifest" in buf.getvalue()

    def test_status_json(self, tmp_path: Path) -> None:
        buf = StringIO()
        with patch.object(sys, "stdout", buf):
            rc = main(["fetch", "status", "--sources", "cvm", "--output-dir", str(tmp_path), "--json"])
        assert rc == 0
        data = json.loads(buf.getvalue())
        assert "cvm" in data


class TestFetchMigrate:
    def test_migrate_dry_run_no_legacy(self, tmp_path: Path) -> None:
        rc = main(["fetch", "migrate", "--sources", "cvm", "--output-dir", str(tmp_path), "--dry-run"])
        assert rc == 0


class TestFetchRun:
    def test_run_inline_with_invalid_datajud_process_jsonl_returns_error(self, tmp_path: Path, caplog) -> None:
        process_path = tmp_path / "process.jsonl"
        process_path.write_text('{"origin_court_or_body": "STJ"}\n{bad-json}\n', encoding="utf-8")

        rc = main(
            [
                "fetch",
                "run",
                "--sources",
                "datajud",
                "--output-dir",
                str(tmp_path),
                "--process-path",
                str(process_path),
            ]
        )

        assert rc == 1
        assert "Failed to generate fetch plan:" in caplog.text
        assert f"{process_path}:2 contains invalid JSON" in caplog.text

    def test_run_with_invalid_plan_json_returns_error(self, tmp_path: Path, caplog) -> None:
        plan_path = tmp_path / "bad-plan.json"
        plan_path.write_text("{bad json", encoding="utf-8")

        rc = main(["fetch", "run", "--plan", str(plan_path), "--output-dir", str(tmp_path)])

        assert rc == 1
        assert f"Failed to load fetch plan from {plan_path}" in caplog.text

    def test_run_with_missing_plan_file_returns_error(self, tmp_path: Path, caplog) -> None:
        plan_path = tmp_path / "missing-plan.json"

        rc = main(["fetch", "run", "--plan", str(plan_path), "--output-dir", str(tmp_path)])

        assert rc == 1
        assert f"Failed to load fetch plan from {plan_path}" in caplog.text

    def test_run_with_structurally_invalid_plan_returns_error(self, tmp_path: Path, caplog) -> None:
        plan_path = tmp_path / "invalid-plan.json"
        plan_path.write_text(
            json.dumps(
                {
                    "schema_version": "1",
                    "sources": ["cvm"],
                    "items": [],
                }
            ),
            encoding="utf-8",
        )

        rc = main(["fetch", "run", "--plan", str(plan_path), "--output-dir", str(tmp_path)])

        assert rc == 1
        assert f"Failed to load fetch plan from {plan_path}" in caplog.text

    def test_run_with_wrong_root_type_plan_returns_error(self, tmp_path: Path, caplog) -> None:
        plan_path = tmp_path / "wrong-root-plan.json"
        plan_path.write_text(json.dumps(["not", "a", "mapping"]), encoding="utf-8")

        rc = main(["fetch", "run", "--plan", str(plan_path), "--output-dir", str(tmp_path)])

        assert rc == 1
        assert f"Failed to load fetch plan from {plan_path}" in caplog.text

    def test_run_with_invalid_numeric_field_in_plan_returns_error(self, tmp_path: Path, caplog) -> None:
        plan_path = tmp_path / "bad-content-length-plan.json"
        plan_path.write_text(
            json.dumps(
                {
                    "plan_id": "plan-bad-content-length",
                    "schema_version": "1",
                    "sources": ["cvm"],
                    "items": [
                        {
                            "unit_id": "cvm:item_1",
                            "action": "download",
                            "reason": "test",
                            "source": "cvm",
                            "remote_url": "https://dados.cvm.gov.br/item_1.zip",
                            "expected_remote_state": {
                                "url": "https://dados.cvm.gov.br/item_1.zip",
                                "content_length": "NaN",
                            },
                            "policy_snapshot": {
                                "source": "cvm",
                                "comparators": ["etag"],
                                "freshness_window": "24h",
                                "allow_weak_skip": True,
                                "supports_deferred_run": True,
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        rc = main(["fetch", "run", "--plan", str(plan_path), "--output-dir", str(tmp_path)])

        assert rc == 1
        assert f"Failed to load fetch plan from {plan_path}" in caplog.text

    def test_run_with_semantically_invalid_plan_returns_error(self, tmp_path: Path, caplog) -> None:
        plan_path = tmp_path / "unknown-source-plan.json"
        plan_path.write_text(
            json.dumps(
                {
                    "plan_id": "plan-unknown-source",
                    "schema_version": "1",
                    "sources": ["unknown_source"],
                    "items": [
                        {
                            "unit_id": "unknown_source:item_1",
                            "action": "download",
                            "reason": "test",
                            "source": "unknown_source",
                            "remote_url": "https://example.invalid/file.zip",
                            "expected_remote_state": {"url": "https://example.invalid/file.zip"},
                            "policy_snapshot": {
                                "source": "unknown_source",
                                "comparators": ["etag"],
                                "freshness_window": "24h",
                                "allow_weak_skip": True,
                                "supports_deferred_run": True,
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        rc = main(["fetch", "run", "--plan", str(plan_path), "--output-dir", str(tmp_path)])

        assert rc == 1
        assert "Plan validation: No policy for source 'unknown_source'" in caplog.text

    def test_run_with_valid_skip_only_plan_succeeds(self, tmp_path: Path) -> None:
        plan_path = tmp_path / "valid-plan.json"
        plan_path.write_text(
            json.dumps(
                {
                    "plan_id": "plan-valid-skip",
                    "schema_version": "1",
                    "sources": ["cvm"],
                    "items": [
                        {
                            "unit_id": "cvm:item_1",
                            "action": "skip",
                            "reason": "already up to date",
                            "source": "cvm",
                            "remote_url": "https://dados.cvm.gov.br/item_1.zip",
                            "expected_remote_state": {"url": "https://dados.cvm.gov.br/item_1.zip"},
                            "policy_snapshot": {
                                "source": "cvm",
                                "comparators": ["etag"],
                                "freshness_window": "24h",
                                "allow_weak_skip": True,
                                "supports_deferred_run": True,
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        rc = main(["fetch", "run", "--plan", str(plan_path), "--output-dir", str(tmp_path)])

        assert rc == 0
