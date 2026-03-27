"""Tests for scripts/audit_builder_validation.py.

Each test creates synthetic builder files in tmp_path and runs the auditor's
analysis functions, asserting the expected findings.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from scripts.audit_builder_validation import (
    analyze_builder,
    audit_builders,
    discover_builders,
    is_builder,
    verify_builder,
)


def _write_py(directory: Path, name: str, content: str) -> Path:
    """Write a Python file with dedented content."""
    path = directory / name
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    return path


def _write_schema(schemas_dir: Path, name: str) -> Path:
    """Create a minimal schema JSON file."""
    path = schemas_dir / name
    path.write_text('{"type": "object"}', encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Discovery tests
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_discovers_builder_with_atomic_writer(
        self, tmp_path: Path
    ) -> None:
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        _write_py(analytics, "my_builder.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            SCHEMA_PATH = Path("schemas/my_builder.schema.json")
            def build():
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
        """)
        assert is_builder(analytics / "my_builder.py")

    def test_discovers_builder_with_write_jsonl(
        self, tmp_path: Path
    ) -> None:
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        _write_py(analytics, "my_builder.py", """\
            from pathlib import Path
            def build():
                write_jsonl(records, Path("out.jsonl"))
        """)
        assert is_builder(analytics / "my_builder.py")

    def test_skips_helper_files(self, tmp_path: Path) -> None:
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        _write_py(analytics, "__init__.py", "# empty")
        _write_py(analytics, "score.py", "x = 1")
        found = discover_builders(analytics)
        assert found == []

    def test_skips_non_builder(self, tmp_path: Path) -> None:
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        _write_py(analytics, "helper.py", """\
            def compute():
                return 42
        """)
        assert not is_builder(analytics / "helper.py")


# ---------------------------------------------------------------------------
# Verification tests
# ---------------------------------------------------------------------------


class TestVerification:
    def test_builder_with_schema_and_validation(
        self, tmp_path: Path
    ) -> None:
        """Builder with correct schema + validate_records before write."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "good_builder.schema.json")
        _write_schema(schemas, "good_builder_summary.schema.json")

        path = _write_py(analytics, "good_builder.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            SCHEMA_PATH = Path("schemas/good_builder.schema.json")
            SUMMARY_SCHEMA_PATH = Path("schemas/good_builder_summary.schema.json")

            def build():
                records = [{"a": 1}]
                validate_records(records, SCHEMA_PATH)
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
                summary = {"count": 1}
                validate_records([summary], SUMMARY_SCHEMA_PATH)
                summary_path.write_text("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        statuses = {(f.artifact_type, f.status) for f in findings}
        assert ("main", "ok") in statuses
        assert ("summary", "ok") in statuses
        assert all(f.status == "ok" for f in findings)

    def test_builder_missing_schema_path(self, tmp_path: Path) -> None:
        """Builder that writes JSONL but has no SCHEMA_PATH."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        path = _write_py(analytics, "no_schema.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter

            def build():
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        assert any(
            f.status == "missing_schema" and f.artifact_type == "main"
            for f in findings
        )

    def test_builder_missing_validate_records(
        self, tmp_path: Path
    ) -> None:
        """Builder with SCHEMA_PATH but no validate_records call."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "no_validate.schema.json")

        path = _write_py(analytics, "no_validate.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter

            SCHEMA_PATH = Path("schemas/no_validate.schema.json")

            def build():
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        assert any(
            f.status == "missing_validation" and f.artifact_type == "main"
            for f in findings
        )

    def test_builder_validate_after_write(self, tmp_path: Path) -> None:
        """Builder with validate_records AFTER write (wrong order)."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "bad_order.schema.json")

        path = _write_py(analytics, "bad_order.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            SCHEMA_PATH = Path("schemas/bad_order.schema.json")

            def build():
                records = [{"a": 1}]
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
                validate_records(records, SCHEMA_PATH)
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        assert any(
            f.status == "wrong_order" and f.artifact_type == "main"
            for f in findings
        )

    def test_builder_missing_summary_validation(
        self, tmp_path: Path
    ) -> None:
        """Builder writes summary but doesn't validate it."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "no_summary_val.schema.json")
        _write_schema(schemas, "no_summary_val_summary.schema.json")

        path = _write_py(analytics, "no_summary_val.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            SCHEMA_PATH = Path("schemas/no_summary_val.schema.json")
            SUMMARY_SCHEMA_PATH = Path("schemas/no_summary_val_summary.schema.json")

            def build():
                records = [{"a": 1}]
                validate_records(records, SCHEMA_PATH)
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
                summary_path.write_text("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        assert any(
            f.status == "ok" and f.artifact_type == "main"
            for f in findings
        )
        assert any(
            f.status == "missing_validation" and f.artifact_type == "summary"
            for f in findings
        )

    def test_builder_schema_file_missing(self, tmp_path: Path) -> None:
        """Builder references schema that doesn't exist on disk."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()
        # Deliberately do NOT create the schema file

        path = _write_py(analytics, "missing_file.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            SCHEMA_PATH = Path("schemas/missing_file.schema.json")

            def build():
                records = [{"a": 1}]
                validate_records(records, SCHEMA_PATH)
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        assert any(
            f.status == "schema_file_missing" and f.artifact_type == "main"
            for f in findings
        )

    def test_builder_empty_output_still_needs_schema(
        self, tmp_path: Path
    ) -> None:
        """Builder that may produce empty output still needs schema def."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "empty_ok.schema.json")

        path = _write_py(analytics, "empty_ok.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            SCHEMA_PATH = Path("schemas/empty_ok.schema.json")

            def build():
                records = []
                validate_records(records, SCHEMA_PATH)
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    pass
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        main_findings = [f for f in findings if f.artifact_type == "main"]
        assert len(main_findings) == 1
        assert main_findings[0].status == "ok"


# ---------------------------------------------------------------------------
# Imported schema vars
# ---------------------------------------------------------------------------


class TestImportedSchemaVars:
    def test_imported_schema_path_detected(self, tmp_path: Path) -> None:
        """Schema vars imported from another module are recognized."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        path = _write_py(analytics, "temporal.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records
            from ._temporal_utils import SCHEMA_PATH, SUMMARY_SCHEMA_PATH

            def build():
                records = [{"a": 1}]
                validate_records(records, SCHEMA_PATH)
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
                summary = {"count": 1}
                validate_records([summary], SUMMARY_SCHEMA_PATH)
                summary_path.write_text("{}")
        """)

        info = analyze_builder(path)
        assert "SCHEMA_PATH" in info.imported_schema_vars
        assert "SUMMARY_SCHEMA_PATH" in info.imported_schema_vars
        findings = verify_builder(info, schemas)
        assert all(f.status == "ok" for f in findings)


# ---------------------------------------------------------------------------
# Helper function wrapper pattern
# ---------------------------------------------------------------------------


class TestHelperFunctionWrapper:
    def test_writer_inside_helper_uses_callsite_line(
        self, tmp_path: Path
    ) -> None:
        """AtomicJsonlWriter inside _helper() uses call-site line."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "groups.schema.json")
        _write_schema(schemas, "groups_summary.schema.json")

        path = _write_py(analytics, "groups.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            PROCESS_SCHEMA = Path("schemas/groups.schema.json")
            SUMMARY_SCHEMA = Path("schemas/groups_summary.schema.json")

            def _write_jsonl(records, output_path):
                with AtomicJsonlWriter(output_path) as fh:
                    for r in records:
                        fh.write(str(r))
                return output_path

            def build():
                records = [{"a": 1}]
                validate_records(records, PROCESS_SCHEMA)
                validate_records([{}], SUMMARY_SCHEMA)
                _write_jsonl(records, Path("out.jsonl"))
                summary_path.write_text("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        main_findings = [f for f in findings if f.artifact_type == "main"]
        assert len(main_findings) == 1
        assert main_findings[0].status == "ok"


# ---------------------------------------------------------------------------
# json_other classification (non-summary write_text)
# ---------------------------------------------------------------------------


class TestJsonOtherClassification:
    def test_non_summary_write_text_not_flagged_as_summary(
        self, tmp_path: Path
    ) -> None:
        """A .write_text() to a non-summary path is json_other, not summary."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "flow.schema.json")

        path = _write_py(analytics, "flow_builder.py", """\
            from pathlib import Path
            from ..schema_validate import validate_records

            FLOW_SCHEMA = Path("schemas/flow.schema.json")

            def build():
                record = {"a": 1}
                validate_records([record], FLOW_SCHEMA)
                final_output.write_text("{}")
        """)

        info = analyze_builder(path)
        # final_output.write_text should be json_other, not summary
        summary_ops = [op for op in info.write_ops if op.kind == "summary"]
        assert len(summary_ops) == 0
        # No summary write means no summary finding
        findings = verify_builder(info, schemas)
        summary_findings = [
            f for f in findings if f.artifact_type == "summary"
        ]
        assert len(summary_findings) == 0


# ---------------------------------------------------------------------------
# Full audit integration test
# ---------------------------------------------------------------------------


class TestFullAudit:
    def test_audit_mixed_results(self, tmp_path: Path) -> None:
        """Audit across multiple builders produces correct mixed results."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "good.schema.json")
        _write_schema(schemas, "good_summary.schema.json")

        # Good builder
        _write_py(analytics, "good.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            SCHEMA_PATH = Path("schemas/good.schema.json")
            SUMMARY_SCHEMA_PATH = Path("schemas/good_summary.schema.json")

            def build():
                validate_records([], SCHEMA_PATH)
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    pass
                validate_records([{}], SUMMARY_SCHEMA_PATH)
                summary_path.write_text("{}")
        """)

        # Bad builder: no schema
        _write_py(analytics, "bad.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter

            def build():
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    fh.write("{}")
        """)

        findings = audit_builders(analytics_dir=analytics, schemas_dir=schemas)
        builder_names = {f.builder for f in findings}
        assert "good" in builder_names
        assert "bad" in builder_names

        good_findings = [f for f in findings if f.builder == "good"]
        assert all(f.status == "ok" for f in good_findings)

        bad_findings = [f for f in findings if f.builder == "bad"]
        assert any(f.status == "missing_schema" for f in bad_findings)

    def test_allowlist_produces_exempt(self, tmp_path: Path) -> None:
        """Builders in the allowlist produce exempt findings."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        # donation_empirical.py is in the allowlist
        _write_py(analytics, "donation_empirical.py", """\
            from pathlib import Path
            def build():
                output_path.write_text("{}")
        """)

        findings = audit_builders(
            analytics_dir=analytics, schemas_dir=schemas
        )
        assert len(findings) == 1
        assert findings[0].status == "exempt"
        assert findings[0].builder == "donation_empirical"


# ---------------------------------------------------------------------------
# Non-standard schema variable names
# ---------------------------------------------------------------------------


class TestNonStandardSchemaNames:
    def test_baseline_schema_detected(self, tmp_path: Path) -> None:
        """Builders using BASELINE_SCHEMA instead of SCHEMA_PATH."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "baseline.schema.json")
        _write_schema(schemas, "baseline_summary.schema.json")

        path = _write_py(analytics, "baseline.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            BASELINE_SCHEMA = Path("schemas/baseline.schema.json")
            SUMMARY_SCHEMA = Path("schemas/baseline_summary.schema.json")

            def build():
                validate_records([], BASELINE_SCHEMA)
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    pass
                validate_records([{}], SUMMARY_SCHEMA)
                summary_path.write_text("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        assert all(f.status == "ok" for f in findings)

    def test_alert_schema_detected(self, tmp_path: Path) -> None:
        """Builders using ALERT_SCHEMA are detected."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "outlier_alert.schema.json")
        _write_schema(schemas, "outlier_alert_summary.schema.json")

        path = _write_py(analytics, "build_alerts.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            ALERT_SCHEMA = Path("schemas/outlier_alert.schema.json")
            SUMMARY_SCHEMA = Path("schemas/outlier_alert_summary.schema.json")

            def build():
                validate_records([], ALERT_SCHEMA)
                with AtomicJsonlWriter(Path("out.jsonl")) as fh:
                    pass
                validate_records([{}], SUMMARY_SCHEMA)
                summary_path.write_text("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        assert all(f.status == "ok" for f in findings)


# ---------------------------------------------------------------------------
# write_jsonl pattern
# ---------------------------------------------------------------------------


class TestWriteJsonlPattern:
    def test_write_jsonl_function_detected(self, tmp_path: Path) -> None:
        """Builder using write_jsonl() instead of AtomicJsonlWriter."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "graph.schema.json")

        path = _write_py(analytics, "graph.py", """\
            from pathlib import Path
            from ..schema_validate import validate_records
            from ..curated.common import write_jsonl

            SCHEMA_PATH = Path("schemas/graph.schema.json")

            def build():
                records = [{"a": 1}]
                validate_records(records, SCHEMA_PATH)
                write_jsonl(records, Path("out.jsonl"))
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        main_findings = [f for f in findings if f.artifact_type == "main"]
        assert len(main_findings) == 1
        assert main_findings[0].status == "ok"


# ---------------------------------------------------------------------------
# Inline summary path pattern
# ---------------------------------------------------------------------------


class TestInlineSummaryPath:
    def test_inline_summary_write_detected(self, tmp_path: Path) -> None:
        """(output_dir / 'xxx_summary.json').write_text(...)."""
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        schemas = tmp_path / "schemas"
        schemas.mkdir()

        _write_schema(schemas, "inline.schema.json")
        _write_schema(schemas, "inline_summary.schema.json")

        path = _write_py(analytics, "inline.py", """\
            from pathlib import Path
            from ._atomic_io import AtomicJsonlWriter
            from ..schema_validate import validate_records

            SCHEMA_PATH = Path("schemas/inline.schema.json")
            SUMMARY_SCHEMA_PATH = Path("schemas/inline_summary.schema.json")

            def build(output_dir):
                records = [{"a": 1}]
                validate_records(records, SCHEMA_PATH)
                with AtomicJsonlWriter(output_dir / "inline.jsonl") as fh:
                    fh.write("{}")
                summary = {"count": 1}
                validate_records([summary], SUMMARY_SCHEMA_PATH)
                (output_dir / "inline_summary.json").write_text("{}")
        """)

        info = analyze_builder(path)
        findings = verify_builder(info, schemas)
        assert all(f.status == "ok" for f in findings)
        assert any(f.artifact_type == "summary" for f in findings)
