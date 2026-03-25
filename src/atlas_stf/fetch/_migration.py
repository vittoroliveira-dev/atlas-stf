"""Migration from legacy checkpoint files to unified manifests.

The migration is transactional per source:

1. Read legacy checkpoint
2. Convert to ``SourceManifest``
3. ``--dry-run`` → return report without writing
4. Write marker ``._migration_{source}_in_progress``
5. ``save_manifest_locked()`` → write manifest
6. Validate manifest (re-read + compare)
7. Remove legacy checkpoint
8. Remove marker

Recovery semantics when a marker file is found on a subsequent run:

- Manifest present + valid → conclude (remove legacy + marker)
- Manifest present + corrupt → abort with error
- Manifest absent + legacy present → restart migration from step 1
- Manifest absent + legacy absent → abort (irrecoverable)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ._manifest_model import FetchUnit, RemoteState, SourceManifest, build_unit_id, source_output_dir
from ._manifest_store import _manifest_path, load_manifest, save_manifest_locked

logger = logging.getLogger(__name__)


@dataclass
class MigrationReport:
    """Summary of a single-source migration."""

    source: str
    units_inferred: int = 0
    units_committed: int = 0
    fields_absent: list[str] = field(default_factory=list)
    ambiguities: list[str] = field(default_factory=list)
    fidelity_losses: list[str] = field(default_factory=list)
    legacy_path: Path | None = None
    manifest: SourceManifest | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def migrate_source(source: str, output_dir: Path, *, dry_run: bool = False) -> MigrationReport:
    """Migrate a single source from legacy checkpoint to unified manifest.

    Raises ``MigrationError`` on irrecoverable failure.
    """
    marker = _marker_path(source, output_dir)

    # --- Recovery: marker from interrupted previous run ---
    if marker.exists():
        return _recover_interrupted(source, output_dir, marker, dry_run=dry_run)

    # --- Normal path ---
    legacy_path, legacy_data = _load_legacy(source, output_dir)
    if legacy_data is None:
        logger.info("No legacy checkpoint for %r — nothing to migrate", source)
        return MigrationReport(source=source)

    report = MigrationReport(source=source, legacy_path=legacy_path)
    manifest = _convert(source, legacy_data, output_dir, report)
    report.manifest = manifest

    if dry_run:
        return report

    # Transactional write
    _write_marker(marker)
    save_manifest_locked(manifest, output_dir)
    _validate_written(source, output_dir, manifest)
    _remove_legacy(legacy_path)
    _remove_marker(marker)

    logger.info("Migrated %r: %d units", source, len(manifest.units))
    return report


def migrate_all(base_dir: Path, *, sources: list[str] | None = None, dry_run: bool = False) -> list[MigrationReport]:
    """Migrate all (or selected) sources."""
    from ._manifest_model import REFRESH_POLICIES

    targets = sources or sorted(REFRESH_POLICIES)
    reports: list[MigrationReport] = []
    for source in targets:
        out_dir = source_output_dir(source, base_dir)
        reports.append(migrate_source(source, out_dir, dry_run=dry_run))
    return reports


# ---------------------------------------------------------------------------
# Marker helpers
# ---------------------------------------------------------------------------


def _marker_path(source: str, output_dir: Path) -> Path:
    return output_dir / f"._migration_{source}_in_progress"


def _write_marker(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def _remove_marker(path: Path) -> None:
    path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


class MigrationError(RuntimeError):
    """Irrecoverable migration failure."""


def _recover_interrupted(source: str, output_dir: Path, marker: Path, *, dry_run: bool) -> MigrationReport:
    """Handle a previous interrupted migration."""
    logger.warning("Found migration marker for %r — attempting recovery", source)

    manifest_file = _manifest_path(source, output_dir)
    manifest_file_exists = manifest_file.exists()
    manifest = load_manifest(source, output_dir)
    legacy_path, legacy_data = _load_legacy(source, output_dir)

    if manifest is not None:
        # Manifest loaded successfully — validate structure
        try:
            _validate_manifest_integrity(manifest)
        except ValueError as exc:
            raise MigrationError(
                f"Manifest for {source!r} is corrupt after interrupted migration: {exc}. Manual intervention required."
            ) from exc

        # Manifest is valid → conclude
        if not dry_run:
            _remove_legacy(legacy_path)
            _remove_marker(marker)
        logger.info("Recovered migration for %r — manifest valid, concluded", source)
        return MigrationReport(
            source=source,
            units_inferred=len(manifest.units),
            units_committed=len(manifest.units),
            legacy_path=legacy_path,
            manifest=manifest,
        )

    # Manifest file exists but could not be loaded → corrupt
    if manifest_file_exists:
        raise MigrationError(
            f"Manifest for {source!r} is corrupt after interrupted migration "
            f"(file exists at {manifest_file} but failed to load). "
            "Manual intervention required."
        )

    # Manifest truly absent
    if legacy_data is not None:
        # Retry from scratch
        logger.info("Manifest absent for %r but legacy present — retrying migration", source)
        if not dry_run:
            _remove_marker(marker)
        return migrate_source(source, output_dir, dry_run=dry_run)

    # Both absent — irrecoverable
    raise MigrationError(
        f"Both manifest and legacy checkpoint are absent for {source!r}. "
        "Migration marker found but no data to recover. Manual intervention required."
    )


def _validate_manifest_integrity(manifest: SourceManifest) -> None:
    """Basic structural validation."""
    if not manifest.source:
        msg = "manifest.source is empty"
        raise ValueError(msg)
    if not manifest.units:
        msg = "manifest has zero units"
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_written(source: str, output_dir: Path, expected: SourceManifest) -> None:
    """Re-read and compare to ensure write was successful."""
    written = load_manifest(source, output_dir)
    if written is None:
        raise MigrationError(f"Manifest for {source!r} not found after write")
    if set(written.units) != set(expected.units):
        raise MigrationError(
            f"Manifest mismatch for {source!r}: expected {sorted(expected.units)}, got {sorted(written.units)}"
        )


# ---------------------------------------------------------------------------
# Legacy loaders
# ---------------------------------------------------------------------------

_LEGACY_CHECKPOINT_FILES: dict[str, str] = {
    "tse_donations": "_checkpoint.json",
    "tse_expenses": "_checkpoint_expenses.json",
    "tse_party_org": "_checkpoint_party_org.json",
    "cgu": "_checkpoint.json",
    "cvm": "_checkpoint.json",
    "rfb": "_rfb_checkpoint.json",
    "datajud": "_checkpoint.json",
}



def _load_legacy(source: str, output_dir: Path) -> tuple[Path | None, dict[str, Any] | None]:
    filename = _LEGACY_CHECKPOINT_FILES.get(source)
    if not filename:
        return None, None
    path = output_dir / filename
    if not path.exists():
        return None, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return path, data
    except json.JSONDecodeError, ValueError:
        logger.warning("Corrupt legacy checkpoint at %s", path)
        return path, None


def _remove_legacy(path: Path | None) -> None:
    if path is not None:
        path.unlink(missing_ok=True)
        logger.info("Removed legacy checkpoint: %s", path)


# ---------------------------------------------------------------------------
# Converters
# ---------------------------------------------------------------------------


def _convert(source: str, data: dict[str, Any], output_dir: Path, report: MigrationReport) -> SourceManifest:
    """Dispatch to the appropriate per-source converter."""
    converters: dict[str, Any] = {
        "tse_donations": _convert_tse_donations,
        "tse_expenses": _convert_tse_expenses,
        "tse_party_org": _convert_tse_party_org,
        "cgu": _convert_cgu,
        "cvm": _convert_cvm,
        "rfb": _convert_rfb,
        "datajud": _convert_datajud,
    }
    converter = converters.get(source)
    if converter is None:
        msg = f"No migration converter for source {source!r}"
        raise MigrationError(msg)
    return converter(data, output_dir, report)


def _convert_tse_donations(data: dict[str, Any], output_dir: Path, report: MigrationReport) -> SourceManifest:
    return _convert_tse_common("tse_donations", data, output_dir, report, zip_prefix="tse")


def _convert_tse_expenses(data: dict[str, Any], output_dir: Path, report: MigrationReport) -> SourceManifest:
    return _convert_tse_common("tse_expenses", data, output_dir, report, zip_prefix="tse_expenses")


def _convert_tse_party_org(data: dict[str, Any], output_dir: Path, report: MigrationReport) -> SourceManifest:
    return _convert_tse_common("tse_party_org", data, output_dir, report, zip_prefix="tse_party_org")


def _convert_tse_common(
    source: str,
    data: dict[str, Any],
    output_dir: Path,
    report: MigrationReport,
    *,
    zip_prefix: str,
) -> SourceManifest:
    units: dict[str, FetchUnit] = {}
    completed_years: set[int] = set(data.get("completed_years", []))
    year_meta: dict[str, dict[str, Any]] = data.get("year_meta", {})

    for year in completed_years:
        uid = build_unit_id(source, str(year))
        meta = year_meta.get(str(year), {})
        url = meta.get("url", "")
        etag = meta.get("etag", "")
        content_length = int(meta.get("content_length", 0))

        if not url:
            report.fields_absent.append(f"{uid}: url")
        if not etag and content_length <= 0:
            report.fidelity_losses.append(f"{uid}: no etag or content_length")

        units[uid] = FetchUnit(
            unit_id=uid,
            source=source,
            label=f"{source} {year}",
            remote_url=url,
            remote_state=RemoteState(url=url, etag=etag, content_length=content_length),
            local_path=str(output_dir / f"{zip_prefix}_{year}.zip"),
            status="committed",
        )
        report.units_committed += 1

    report.units_inferred = len(units)
    return SourceManifest(source=source, units=units)


def _convert_cgu(data: dict[str, Any], output_dir: Path, report: MigrationReport) -> SourceManifest:
    units: dict[str, FetchUnit] = {}
    download_date = data.get("csv_download_date", "")
    completed: dict[str, dict[str, Any]] = data.get("csv_completed_datasets", {})

    for dataset_name, meta in completed.items():
        uid = build_unit_id("cgu", dataset_name.replace("-", "_"))
        content_length = int(meta.get("content_length", 0))
        ds_date = meta.get("download_date", download_date)

        units[uid] = FetchUnit(
            unit_id=uid,
            source="cgu",
            label=f"CGU {dataset_name}",
            remote_url="",
            remote_state=RemoteState(
                url="",
                content_length=content_length,
                last_modified=ds_date,
            ),
            local_path=str(output_dir / f"{dataset_name}.csv"),
            status="committed",
            fetch_date=ds_date,
        )
        report.units_committed += 1

        if not content_length:
            report.fidelity_losses.append(f"{uid}: no content_length")

    report.units_inferred = len(units)
    return SourceManifest(source="cgu", units=units)


def _convert_cvm(data: dict[str, Any], output_dir: Path, report: MigrationReport) -> SourceManifest:
    uid = build_unit_id("cvm", "sanctions")
    content_length = int(data.get("content_length", 0))
    etag = data.get("etag", "")
    record_count = int(data.get("record_count", 0))

    unit = FetchUnit(
        unit_id=uid,
        source="cvm",
        label="CVM sanctions",
        remote_url="",
        remote_state=RemoteState(url="", etag=etag, content_length=content_length),
        local_path=str(output_dir / "processo_sancionador.zip"),
        status="committed",
        published_record_count=record_count,
    )

    report.units_inferred = 1
    report.units_committed = 1
    if not etag and content_length <= 0:
        report.fidelity_losses.append(f"{uid}: no etag or content_length")

    return SourceManifest(source="cvm", units={uid: unit})


def _convert_rfb(data: dict[str, Any], output_dir: Path, report: MigrationReport) -> SourceManifest:
    units: dict[str, FetchUnit] = {}

    pass_keys = [
        ("completed_socios_pass1", "socios_pass1", "Socios"),
        ("completed_socios_pass2", "socios_pass2", "Socios"),
        ("completed_empresas", "empresas", "Empresas"),
        ("completed_estabelecimentos", "estabelecimentos", "Estabelecimentos"),
    ]

    for ckpt_key, pass_name, file_prefix in pass_keys:
        completed_indices: list[int] = data.get(ckpt_key, [])
        for i in completed_indices:
            uid = build_unit_id("rfb", f"{pass_name}_{i}")
            units[uid] = FetchUnit(
                unit_id=uid,
                source="rfb",
                label=f"RFB {pass_name} #{i}",
                remote_url="",
                remote_state=RemoteState(url=""),
                local_path=str(output_dir / f"{file_prefix}{i}.zip"),
                status="committed",
                metadata={"pass_name": pass_name, "file_index": i},
            )
            report.units_committed += 1

    # Reference tables
    if data.get("completed_reference", False):
        uid = build_unit_id("rfb", "reference")
        units[uid] = FetchUnit(
            unit_id=uid,
            source="rfb",
            label="RFB reference tables",
            remote_url="",
            remote_state=RemoteState(url=""),
            status="committed",
        )
        report.units_committed += 1

    report.units_inferred = len(units)
    report.fidelity_losses.append("rfb: no remote_state metadata in legacy checkpoint")
    return SourceManifest(source="rfb", units=units)


def _convert_datajud(data: dict[str, Any], output_dir: Path, report: MigrationReport) -> SourceManifest:
    units: dict[str, FetchUnit] = {}
    completed: list[str] = data.get("completed", [])

    for index in completed:
        uid = build_unit_id("datajud", index.lower().replace(".", "_"))
        units[uid] = FetchUnit(
            unit_id=uid,
            source="datajud",
            label=f"DataJud {index}",
            remote_url="",
            remote_state=RemoteState(url=""),
            local_path=str(output_dir / f"{index}.json"),
            status="committed",
            metadata={"index": index},
        )
        report.units_committed += 1

    report.units_inferred = len(units)
    return SourceManifest(source="datajud", units=units)
