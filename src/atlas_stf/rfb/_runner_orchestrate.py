"""RFB fetch orchestration: main fetch_rfb_data flow and artifact commit helpers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from ..core.fetch_lock import FetchLock
from ..core.fetch_result import FetchTimer
from ..fetch._manifest_model import SourceManifest
from ..fetch._manifest_store import load_manifest
from ._config import (
    RFB_EMPRESAS_FILE_COUNT,
    RFB_ESTABELECIMENTOS_FILE_COUNT,
    RFB_LEGACY_BASE_URL,
    RFB_SOCIOS_FILE_COUNT,
    RFB_WEBDAV_BASE,
    RfbFetchConfig,
)
from ._reference import fetch_reference_tables
from ._runner_checkpoint import (
    _build_target_names,
    _checkpoint_to_manifest,  # noqa: F401 — kept accessible via this module
    _compute_tse_targets_hash,
    _extract_tse_donor_targets,
    _manifest_to_checkpoint,
    _save_checkpoint_via_manifest,
)
from ._runner_fetch import (
    run_pass1_socios,
    run_pass2_socios,
    run_pass3_empresas,
    run_pass4_estabelecimentos,
    write_companies_jsonl,
    write_establishments_jsonl,
    write_partners_jsonl,
)
from ._runner_http import (
    _discover_latest_month,
    _download_zip,
    _parse_csv_from_zip_text,
)

logger = logging.getLogger(__name__)

# Artifact names used as keys in artifact_commits
_ARTIFACT_PARTNERS = "partners_raw.jsonl"
_ARTIFACT_COMPANIES = "companies_raw.jsonl"
_ARTIFACT_ESTABLISHMENTS = "establishments_raw.jsonl"
_ALL_ARTIFACTS = (_ARTIFACT_PARTNERS, _ARTIFACT_COMPANIES, _ARTIFACT_ESTABLISHMENTS)


def fetch_rfb_data(
    config: RfbFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Download + parse Socios, Empresas, and Estabelecimentos ZIPs from RFB."""
    config.output_dir.mkdir(parents=True, exist_ok=True)

    target_names = _build_target_names(config)
    logger.info("Target names for RFB matching: %d", len(target_names))

    # Extract TSE donor targets (backward-compatible: no-op when file missing)
    pj_cnpjs_basico, pf_cpfs, pj_cnpjs_full = _extract_tse_donor_targets(config.donations_path)

    # Discover latest month from NextCloud, fallback to legacy URL
    month = _discover_latest_month(timeout=15)
    if month:
        base_url = f"{RFB_WEBDAV_BASE}/{month}"
        logger.info("Using NextCloud URL: %s", base_url)
    else:
        base_url = RFB_LEGACY_BASE_URL
        logger.info("Falling back to legacy URL: %s", base_url)
        try:
            httpx.head(base_url, timeout=httpx.Timeout(10.0, connect=10.0), follow_redirects=True)
        except httpx.RequestError as exc:
            logger.error("RFB server unreachable (%s): %s", base_url, exc)
            logger.error("Set ATLAS_STF_RFB_NEXTCLOUD_SHARE_TOKEN or retry later")
            return config.output_dir

    if config.dry_run:
        logger.info(
            "[dry-run] Would download %d Socios, %d Empresas, %d Estabelecimentos ZIPs",
            RFB_SOCIOS_FILE_COUNT,
            RFB_EMPRESAS_FILE_COUNT,
            RFB_ESTABELECIMENTOS_FILE_COUNT,
        )
        return config.output_dir

    with FetchLock(config.output_dir, "rfb"):
        return _fetch_rfb_data_locked(
            config,
            target_names=target_names,
            base_url=base_url,
            pj_cnpjs_basico=pj_cnpjs_basico,
            pf_cpfs=pf_cpfs,
            pj_cnpjs_full=pj_cnpjs_full,
            on_progress=on_progress,
        )


def _artifact_commit_is_valid(
    artifact_name: str,
    path: Path,
    pass_keys: list[str],
    checkpoint: dict[str, Any],
) -> bool:
    """Return True if the artifact output file is backed by a recorded commit.

    Logic:
    - File missing or empty → not valid (re-materialize needed).
    - Artifact commit present with record_count > 0 → valid; do not re-run passes.
    - No artifact commit but passes are complete → passes survived but write didn't;
      the passes are still valid (DO NOT invalidate them), just re-write the artifact.
    - No artifact commit and no completed passes → fresh start; nothing to invalidate.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False

    artifact_commits: dict[str, Any] = checkpoint.get("artifact_commits", {})
    commit = artifact_commits.get(artifact_name)
    if commit and commit.get("record_count", 0) > 0:
        return True

    # File exists but no artifact commit recorded.
    # Check whether any of the upstream passes are marked complete.
    any_complete = any(checkpoint.get(k) for k in pass_keys)
    if any_complete:
        # Passes completed but the artifact write was never stamped in the manifest.
        # This indicates a crash after pass completion but before the artifact commit.
        # The passes are still valid — only the artifact needs to be re-written.
        logger.warning(
            "%s: passes complete but no artifact_commit recorded — artifact will be re-materialized",
            artifact_name,
        )
        return False

    # File exists, no passes completed, no commit — treat as externally placed file.
    return True


def _fetch_rfb_data_locked(
    config: RfbFetchConfig,
    *,
    target_names: set[str],
    base_url: str,
    pj_cnpjs_basico: set[str],
    pf_cpfs: set[str],
    pj_cnpjs_full: set[str],
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Inner implementation guarded by FetchLock."""
    import uuid as _uuid

    timer = FetchTimer("rfb")
    timer.start()
    try:
        manifest = load_manifest("rfb", config.output_dir) or SourceManifest(source="rfb")
        checkpoint = _manifest_to_checkpoint(manifest)

        run_id = str(_uuid.uuid4())[:8]

        if config.force_refresh:
            logger.info("RFB: force-refresh — clearing checkpoint")
            checkpoint = {
                "completed_socios_pass1": [],
                "completed_socios_pass2": [],
                "completed_empresas": [],
                "completed_estabelecimentos": [],
                "completed_reference": False,
                "cnpjs": [],
                "artifact_commits": {},
            }
            _save_checkpoint_via_manifest(config.output_dir, checkpoint)

        # TSE targets checkpoint invalidation
        has_tse_targets = bool(pj_cnpjs_basico or pf_cpfs or pj_cnpjs_full)
        if has_tse_targets:
            new_hash = _compute_tse_targets_hash(pj_cnpjs_basico, pf_cpfs, pj_cnpjs_full)
            old_hash = checkpoint.get("tse_targets_hash", "")
            if old_hash and old_hash != new_hash:
                logger.info("TSE targets changed — invalidating RFB passes for rescan")
                checkpoint["completed_socios_pass1"] = []
                checkpoint["completed_socios_pass2"] = []
                checkpoint["completed_empresas"] = []
                checkpoint["completed_estabelecimentos"] = []
                checkpoint["cnpjs"] = []
                checkpoint["artifact_commits"] = {}
                _save_checkpoint_via_manifest(config.output_dir, checkpoint)
            checkpoint["tse_targets_hash"] = new_hash

        partners_path = config.output_dir / _ARTIFACT_PARTNERS
        companies_path = config.output_dir / _ARTIFACT_COMPANIES
        establishments_path = config.output_dir / _ARTIFACT_ESTABLISHMENTS

        # Guard: check each artifact independently.
        #
        # If passes are complete but artifact commit is absent, the process crashed
        # between pass completion and the write.  In that case the passes remain valid
        # and only the artifact write needs to be repeated — we must NOT clear the pass
        # lists (which was the flaw in the old global output_commit model).
        #
        # _artifact_commit_is_valid() emits a warning and returns False when it detects
        # this condition; the main flow below will re-materialize only the missing files.
        partners_valid = _artifact_commit_is_valid(
            _ARTIFACT_PARTNERS,
            partners_path,
            ["completed_socios_pass1", "completed_socios_pass2"],
            checkpoint,
        )
        companies_valid = _artifact_commit_is_valid(
            _ARTIFACT_COMPANIES,
            companies_path,
            ["completed_empresas"],
            checkpoint,
        )
        establishments_valid = _artifact_commit_is_valid(
            _ARTIFACT_ESTABLISHMENTS,
            establishments_path,
            ["completed_estabelecimentos"],
            checkpoint,
        )

        # Full cache hit: all passes done + all artifacts committed with content
        all_socios_p1 = set(checkpoint.get("completed_socios_pass1", []))
        all_socios_p2 = set(checkpoint.get("completed_socios_pass2", []))
        all_empresas = set(checkpoint.get("completed_empresas", []))
        all_estabelecimentos = set(checkpoint.get("completed_estabelecimentos", []))

        if (
            len(all_socios_p1) == RFB_SOCIOS_FILE_COUNT
            and len(all_socios_p2) == RFB_SOCIOS_FILE_COUNT
            and len(all_empresas) == RFB_EMPRESAS_FILE_COUNT
            and len(all_estabelecimentos) == RFB_ESTABELECIMENTOS_FILE_COUNT
            and partners_valid
            and companies_valid
            and establishments_valid
        ):
            logger.info("RFB fetch already complete — output files exist with content")
            if on_progress:
                on_progress(1, 1, "RFB: Ja completo (cache)")
            timer.log_success(records_written=0, detail="already complete (cache)")
            return config.output_dir

        # Total steps: reference(1) + socios*2 + empresas + estabelecimentos
        total_steps = 1 + RFB_SOCIOS_FILE_COUNT * 2 + RFB_EMPRESAS_FILE_COUNT + RFB_ESTABELECIMENTOS_FILE_COUNT
        step = 0

        # Reference tables
        if not checkpoint.get("completed_reference", False):
            if on_progress:
                on_progress(step, total_steps, "RFB: Tabelas de referencia")
            fetch_reference_tables(
                config.output_dir,
                base_url,
                config.timeout_seconds,
                download_zip=_download_zip,
                parse_csv_from_zip_text=_parse_csv_from_zip_text,
            )
            checkpoint["completed_reference"] = True
            _save_checkpoint_via_manifest(config.output_dir, checkpoint)
        step += 1

        # ------------------------------------------------------------------ #
        # Partners artifact (passes 1 + 2)
        # ------------------------------------------------------------------ #
        unique_partners: list[dict[str, Any]] = []

        if not partners_valid:
            # Pass 1: Socios by name + TSE CPF/CNPJ targets
            all_partners, matched_cnpjs, step = run_pass1_socios(
                base_url=base_url,
                socios_file_count=RFB_SOCIOS_FILE_COUNT,
                config_output_dir=config.output_dir,
                config_timeout=config.timeout_seconds,
                target_names=target_names,
                checkpoint=checkpoint,
                download_zip=_download_zip,
                parse_csv_from_zip_text=_parse_csv_from_zip_text,
                save_checkpoint=_save_checkpoint_via_manifest,
                on_progress=on_progress,
                step=step,
                total_steps=total_steps,
                target_cpfs=pf_cpfs,
                target_partner_cnpjs=pj_cnpjs_full,
                manifest_dir=config.output_dir,
            )

            # Inject PJ cnpj_basico from TSE donors (empresa própria — caminho A)
            if pj_cnpjs_basico:
                pre_count = len(matched_cnpjs)
                matched_cnpjs.update(pj_cnpjs_basico)
                logger.info(
                    "Injected %d TSE PJ cnpj_basico into matched_cnpjs (%d -> %d)",
                    len(pj_cnpjs_basico),
                    pre_count,
                    len(matched_cnpjs),
                )

            # Pass 2: Socios co-partners by CNPJ
            pass2_partners, step = run_pass2_socios(
                base_url=base_url,
                socios_file_count=RFB_SOCIOS_FILE_COUNT,
                config_output_dir=config.output_dir,
                config_timeout=config.timeout_seconds,
                matched_cnpjs=matched_cnpjs,
                checkpoint=checkpoint,
                download_zip=_download_zip,
                parse_csv_from_zip_text=_parse_csv_from_zip_text,
                save_checkpoint=_save_checkpoint_via_manifest,
                on_progress=on_progress,
                step=step,
                total_steps=total_steps,
                manifest_dir=config.output_dir,
            )
            all_partners.extend(pass2_partners)

            # Deduplicate partners
            seen: set[str] = set()
            for p in all_partners:
                key = f"{p['cnpj_basico']}:{p.get('partner_name_normalized', '')}"
                if key not in seen:
                    seen.add(key)
                    unique_partners.append(p)

            # Write and commit partners artifact immediately
            record_count = write_partners_jsonl(config.output_dir, unique_partners)
            _stamp_artifact_commit(
                _ARTIFACT_PARTNERS,
                record_count,
                run_id,
                checkpoint,
                config.output_dir,
            )
        else:
            # Partners already committed; reconstruct matched_cnpjs from checkpoint
            matched_cnpjs = set(checkpoint.get("cnpjs", []))
            if pj_cnpjs_basico:
                matched_cnpjs.update(pj_cnpjs_basico)
            # Advance step counter to stay consistent with total_steps accounting
            step += RFB_SOCIOS_FILE_COUNT * 2

        # ------------------------------------------------------------------ #
        # Companies artifact (pass 3)
        # ------------------------------------------------------------------ #
        all_companies: list[dict[str, Any]] = []

        if not companies_valid:
            all_companies, step = run_pass3_empresas(
                base_url=base_url,
                empresas_file_count=RFB_EMPRESAS_FILE_COUNT,
                config_output_dir=config.output_dir,
                config_timeout=config.timeout_seconds,
                matched_cnpjs=matched_cnpjs,
                checkpoint=checkpoint,
                download_zip=_download_zip,
                parse_csv_from_zip_text=_parse_csv_from_zip_text,
                save_checkpoint=_save_checkpoint_via_manifest,
                on_progress=on_progress,
                step=step,
                total_steps=total_steps,
                manifest_dir=config.output_dir,
            )

            record_count = write_companies_jsonl(config.output_dir, all_companies)
            _stamp_artifact_commit(
                _ARTIFACT_COMPANIES,
                record_count,
                run_id,
                checkpoint,
                config.output_dir,
            )
        else:
            step += RFB_EMPRESAS_FILE_COUNT

        # ------------------------------------------------------------------ #
        # Establishments artifact (pass 4)
        # ------------------------------------------------------------------ #
        all_establishments: list[dict[str, Any]] = []

        if not establishments_valid:
            all_establishments, step = run_pass4_estabelecimentos(
                base_url=base_url,
                estabelecimentos_file_count=RFB_ESTABELECIMENTOS_FILE_COUNT,
                config_output_dir=config.output_dir,
                config_timeout=config.timeout_seconds,
                matched_cnpjs=matched_cnpjs,
                checkpoint=checkpoint,
                download_zip=_download_zip,
                parse_csv_from_zip_text=_parse_csv_from_zip_text,
                save_checkpoint=_save_checkpoint_via_manifest,
                on_progress=on_progress,
                step=step,
                total_steps=total_steps,
                manifest_dir=config.output_dir,
            )

            record_count = write_establishments_jsonl(config.output_dir, all_establishments)
            _stamp_artifact_commit(
                _ARTIFACT_ESTABLISHMENTS,
                record_count,
                run_id,
                checkpoint,
                config.output_dir,
            )
        else:
            step += RFB_ESTABELECIMENTOS_FILE_COUNT

        # Cleanup cached Socios ZIPs (Empresas/Estabelecimentos are deleted by pass runners)
        for i in range(RFB_SOCIOS_FILE_COUNT):
            cache_path = config.output_dir / f"Socios{i}.zip"
            if cache_path.exists():
                cache_path.unlink()

        if on_progress:
            on_progress(total_steps, total_steps, "RFB: Concluido")
        total_records = len(unique_partners) + len(all_companies) + len(all_establishments)
        timer.log_success(
            records_written=total_records,
            detail=f"{len(unique_partners)} partners, {len(all_companies)} companies, "
            f"{len(all_establishments)} establishments",
        )
        return config.output_dir
    except Exception as exc:
        timer.log_failure(exc)
        raise


def _stamp_artifact_commit(
    artifact_name: str,
    record_count: int,
    run_id: str,
    checkpoint: dict[str, Any],
    output_dir: Path,
) -> None:
    """Record that an artifact was successfully written to disk.

    This commit proves the JSONL file corresponds to the data produced by the
    completed passes.  Written immediately after the atomic JSONL write so that
    a crash between write and stamp is recoverable without re-running passes.
    """
    artifact_commits: dict[str, Any] = checkpoint.setdefault("artifact_commits", {})
    artifact_commits[artifact_name] = {
        "run_id": run_id,
        "record_count": record_count,
        "committed_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_checkpoint_via_manifest(output_dir, checkpoint)
    logger.info(
        "Artifact commit stamped: %s (%d records, run_id=%s)",
        artifact_name,
        record_count,
        run_id,
    )


__all__ = [
    "_artifact_commit_is_valid",
    "_fetch_rfb_data_locked",
    "_stamp_artifact_commit",
    "fetch_rfb_data",
]
