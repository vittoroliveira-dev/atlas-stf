"""Data preparation subparsers: manifest, stage, profile, validate, audit."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import (
    DEFAULT_ANALYTICS_DIR,
    DEFAULT_CURATED_DIR,
    DEFAULT_RAW_DIR,
    DEFAULT_STAGING_DIR,
)


def _add_data_prep_parsers(subparsers: Any) -> None:
    manifest = subparsers.add_parser("manifest", help="Build manifests for project datasets")
    manifest_sub = manifest.add_subparsers(dest="manifest_target", required=True)
    manifest_raw = manifest_sub.add_parser("raw", help="Generate raw CSV manifest")
    manifest_raw.add_argument("--dir", type=Path, default=DEFAULT_RAW_DIR, help="Directory with raw CSV files")
    manifest_raw.add_argument("--output", type=Path, help="Output JSONL path")
    manifest_raw.add_argument("--source-id", default="STF-TRANSP-REGDIST", help="Source registry identifier")
    manifest_raw.add_argument("--filter-description", default="INCERTO", help="Observed filter description")
    manifest_raw.add_argument("--coverage-note", default="INCERTO", help="Observed coverage note")
    stage = subparsers.add_parser("stage", help="Run staging pipeline")
    stage.add_argument("--file", help="Process a single file (e.g. acervo.csv)")
    stage.add_argument("--dry-run", action="store_true", help="Show transforms without writing")
    stage.add_argument("--verbose", "-v", action="store_true", help="Debug-level logging")
    stage.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR, help="Raw data directory")
    stage.add_argument("--staging-dir", type=Path, default=DEFAULT_STAGING_DIR, help="Staging output directory")
    profile = subparsers.add_parser("profile", help="Generate profiling reports")
    profile_sub = profile.add_subparsers(dest="profile_target", required=True)
    profile_staging_parser = profile_sub.add_parser("staging", help="Profile staging CSV files")
    profile_staging_parser.add_argument("--dir", type=Path, default=DEFAULT_STAGING_DIR, help="Staging CSV directory")
    profile_staging_parser.add_argument("--output-dir", type=Path, help="Directory for profile JSON files")
    profile_staging_parser.add_argument("--file", help="Profile a single CSV file")
    validate = subparsers.add_parser("validate", help="Validate staged datasets")
    validate_sub = validate.add_subparsers(dest="validate_target", required=True)
    validate_staging_parser = validate_sub.add_parser("staging", help="Validate staging CSV files")
    validate_staging_parser.add_argument("--dir", type=Path, default=DEFAULT_STAGING_DIR, help="Staging CSV directory")
    validate_staging_parser.add_argument("--output", type=Path, help="Output JSON path")
    validate_staging_parser.add_argument("--file", help="Validate a single CSV file")
    audit = subparsers.add_parser("audit", help="Run deterministic audit gates")
    audit_sub = audit.add_subparsers(dest="audit_target", required=True)
    audit_stage = audit_sub.add_parser("stage", help="Audit staging artifacts")
    audit_stage.add_argument(
        "--staging-dir",
        type=Path,
        default=DEFAULT_STAGING_DIR,
        help="Staging CSV directory",
    )
    audit_stage.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_STAGING_DIR / "_audit_report.json",
        help="Output JSON audit report path",
    )

    audit_curated = audit_sub.add_parser("curated", help="Audit curated artifacts")
    audit_curated.add_argument(
        "--curated-dir",
        type=Path,
        default=DEFAULT_CURATED_DIR,
        help="Curated JSONL directory",
    )
    audit_curated.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_CURATED_DIR / "_audit_report.json",
        help="Output JSON audit report path",
    )

    audit_representation = audit_sub.add_parser("representation", help="Audit representation network artifacts")
    audit_representation.add_argument(
        "--curated-dir",
        type=Path,
        default=DEFAULT_CURATED_DIR,
        help="Curated JSONL directory",
    )
    audit_representation.add_argument(
        "--analytics-dir",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR,
        help="Analytics artifact directory",
    )
    audit_representation.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_CURATED_DIR / "_audit_representation_report.json",
        help="Output JSON audit report path",
    )

    audit_analytics = audit_sub.add_parser("analytics", help="Audit analytics artifacts")
    audit_analytics.add_argument(
        "--comparison-group-path",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR / "comparison_group.jsonl",
        help="Comparison group JSONL path",
    )
    audit_analytics.add_argument(
        "--link-path",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR / "decision_event_group_link.jsonl",
        help="Decision-event group link JSONL path",
    )
    audit_analytics.add_argument(
        "--baseline-path",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR / "baseline.jsonl",
        help="Baseline JSONL path",
    )
    audit_analytics.add_argument(
        "--alert-path",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR / "outlier_alert.jsonl",
        help="Outlier alert JSONL path",
    )
    audit_analytics.add_argument(
        "--decision-event-path",
        type=Path,
        default=DEFAULT_CURATED_DIR / "decision_event.jsonl",
        help="Decision-event JSONL path",
    )
    audit_analytics.add_argument(
        "--process-path",
        type=Path,
        default=DEFAULT_CURATED_DIR / "process.jsonl",
        help="Process JSONL path",
    )
    audit_analytics.add_argument(
        "--evidence-dir",
        type=Path,
        default=Path("data/evidence"),
        help="Evidence JSON directory for optional existence checks",
    )
    audit_analytics.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_ANALYTICS_DIR / "_audit_report.json",
        help="Output JSON audit report path",
    )
