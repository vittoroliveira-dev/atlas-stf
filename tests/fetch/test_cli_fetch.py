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
