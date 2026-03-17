"""RFB fetch runner: downloads Socios/Empresas/Estabelecimentos ZIPs from RFB open data."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from ..core.identity import is_valid_cnpj, is_valid_cpf, normalize_entity_name, normalize_tax_id
from ._config import (
    RFB_EMPRESAS_FILE_COUNT,
    RFB_ESTABELECIMENTOS_FILE_COUNT,
    RFB_LEGACY_BASE_URL,
    RFB_SOCIOS_FILE_COUNT,
    RFB_WEBDAV_BASE,
    RfbFetchConfig,
)
from ._reference import fetch_reference_tables
from ._runner_fetch import (
    enrich_and_write_results,
    run_pass1_socios,
    run_pass2_socios,
    run_pass3_empresas,
    run_pass4_estabelecimentos,
)
from ._runner_http import (  # noqa: F401 — re-exports for test compatibility
    _discover_latest_month,
    _download_zip,
    _extract_csv_from_zip,
    _is_rfb_data_member,
    _parse_csv_from_zip_text,
)

logger = logging.getLogger(__name__)


def _build_target_names(config: RfbFetchConfig) -> set[str]:
    """Build set of normalized target names from minister_bio + party + counsel."""
    names: set[str] = set()

    if config.minister_bio_path.exists():
        data = json.loads(config.minister_bio_path.read_text(encoding="utf-8"))
        for _key, entry in data.items():
            name = entry.get("minister_name", "")
            norm = normalize_entity_name(name)
            if norm:
                names.add(norm)
            civil = entry.get("civil_name", "")
            civil_norm = normalize_entity_name(civil)
            if civil_norm:
                names.add(civil_norm)

    for path in (config.party_path, config.counsel_path):
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSONL line in %s", path)
                    continue
                raw = (
                    record.get("party_name_normalized")
                    or record.get("counsel_name_normalized")
                    or record.get("party_name_raw", "")
                    or record.get("counsel_name_raw", "")
                )
                norm = normalize_entity_name(raw)
                if norm:
                    names.add(norm)

    return names


def _load_checkpoint(output_dir: Path) -> dict[str, Any]:
    """Load checkpoint state."""
    checkpoint_path = output_dir / "_rfb_checkpoint.json"
    if checkpoint_path.exists():
        return json.loads(checkpoint_path.read_text(encoding="utf-8"))
    return {
        "completed_socios_pass1": [],
        "completed_socios_pass2": [],
        "completed_empresas": [],
        "completed_estabelecimentos": [],
        "completed_reference": False,
        "cnpjs": [],
    }


def _save_checkpoint(output_dir: Path, state: dict[str, Any]) -> None:
    """Save checkpoint state."""
    checkpoint_path = output_dir / "_rfb_checkpoint.json"
    checkpoint_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_tse_donor_targets(
    donations_path: Path,
) -> tuple[set[str], set[str], set[str]]:
    """Extract TSE donor CPF/CNPJ targets for RFB scan.

    Returns ``(pj_cnpjs_basico, pf_cpfs, pj_cnpjs_full)``:
    - pj_cnpjs_basico: first 8 digits of valid 14-digit CNPJs (for direct matched_cnpjs injection)
    - pf_cpfs: normalized valid 11-digit CPFs (for partner scan)
    - pj_cnpjs_full: normalized valid 14-digit CNPJs (for partner-as-PJ scan)
    """
    pj_basico: set[str] = set()
    pf_cpfs: set[str] = set()
    pj_full: set[str] = set()

    if not donations_path.exists():
        return pj_basico, pf_cpfs, pj_full

    with donations_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSONL line in %s", donations_path)
                continue
            raw_doc = record.get("donor_cpf_cnpj", "")
            normalized = normalize_tax_id(raw_doc)
            if not normalized:
                continue
            if len(normalized) == 14 and is_valid_cnpj(normalized):
                pj_basico.add(normalized[:8])
                pj_full.add(normalized)
            elif len(normalized) == 11 and is_valid_cpf(normalized):
                pf_cpfs.add(normalized)

    logger.info(
        "TSE donor targets: %d PJ cnpj_basico, %d PF CPFs, %d PJ full CNPJs",
        len(pj_basico),
        len(pf_cpfs),
        len(pj_full),
    )
    return pj_basico, pf_cpfs, pj_full


def _compute_tse_targets_hash(
    pj_cnpjs: set[str],
    pf_cpfs: set[str],
    pj_cnpjs_full: set[str],
) -> str:
    """Compute a stable hash of TSE targets for checkpoint invalidation."""
    payload = "\n".join(sorted(pj_cnpjs) + ["---"] + sorted(pf_cpfs) + ["---"] + sorted(pj_cnpjs_full))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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

    checkpoint = _load_checkpoint(config.output_dir)

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
            _save_checkpoint(config.output_dir, checkpoint)
        checkpoint["tse_targets_hash"] = new_hash

    # Cache check: all passes complete and output files exist
    partners_path = config.output_dir / "partners_raw.jsonl"
    companies_path = config.output_dir / "companies_raw.jsonl"
    establishments_path = config.output_dir / "establishments_raw.jsonl"
    all_socios_p1 = set(checkpoint.get("completed_socios_pass1", []))
    all_socios_p2 = set(checkpoint.get("completed_socios_pass2", []))
    all_empresas = set(checkpoint.get("completed_empresas", []))
    all_estabelecimentos = set(checkpoint.get("completed_estabelecimentos", []))
    if (
        len(all_socios_p1) == RFB_SOCIOS_FILE_COUNT
        and len(all_socios_p2) == RFB_SOCIOS_FILE_COUNT
        and len(all_empresas) == RFB_EMPRESAS_FILE_COUNT
        and len(all_estabelecimentos) == RFB_ESTABELECIMENTOS_FILE_COUNT
        and partners_path.exists()
        and partners_path.stat().st_size > 0
        and companies_path.exists()
        and companies_path.stat().st_size > 0
        and establishments_path.exists()
        and establishments_path.stat().st_size > 0
    ):
        logger.info("RFB fetch already complete — output files exist with content")
        if on_progress:
            on_progress(1, 1, "RFB: Ja completo (cache)")
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
        _save_checkpoint(config.output_dir, checkpoint)
    step += 1

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
        save_checkpoint=_save_checkpoint,
        on_progress=on_progress,
        step=step,
        total_steps=total_steps,
        target_cpfs=pf_cpfs,
        target_partner_cnpjs=pj_cnpjs_full,
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
        save_checkpoint=_save_checkpoint,
        on_progress=on_progress,
        step=step,
        total_steps=total_steps,
    )
    all_partners.extend(pass2_partners)

    # Deduplicate partners
    seen: set[str] = set()
    unique_partners: list[dict[str, Any]] = []
    for p in all_partners:
        key = f"{p['cnpj_basico']}:{p.get('partner_name_normalized', '')}"
        if key not in seen:
            seen.add(key)
            unique_partners.append(p)

    # Pass 3: Empresas
    all_companies, step = run_pass3_empresas(
        base_url=base_url,
        empresas_file_count=RFB_EMPRESAS_FILE_COUNT,
        config_output_dir=config.output_dir,
        config_timeout=config.timeout_seconds,
        matched_cnpjs=matched_cnpjs,
        checkpoint=checkpoint,
        download_zip=_download_zip,
        parse_csv_from_zip_text=_parse_csv_from_zip_text,
        save_checkpoint=_save_checkpoint,
        on_progress=on_progress,
        step=step,
        total_steps=total_steps,
    )

    # Pass 4: Estabelecimentos
    all_establishments, step = run_pass4_estabelecimentos(
        base_url=base_url,
        estabelecimentos_file_count=RFB_ESTABELECIMENTOS_FILE_COUNT,
        config_output_dir=config.output_dir,
        config_timeout=config.timeout_seconds,
        matched_cnpjs=matched_cnpjs,
        checkpoint=checkpoint,
        download_zip=_download_zip,
        parse_csv_from_zip_text=_parse_csv_from_zip_text,
        save_checkpoint=_save_checkpoint,
        on_progress=on_progress,
        step=step,
        total_steps=total_steps,
    )

    # Enrich and write all results
    enrich_and_write_results(
        config_output_dir=config.output_dir,
        unique_partners=unique_partners,
        all_companies=all_companies,
        all_establishments=all_establishments,
    )

    # Cleanup cached ZIPs
    for i in range(RFB_SOCIOS_FILE_COUNT):
        cache_path = config.output_dir / f"Socios{i}.zip"
        if cache_path.exists():
            cache_path.unlink()

    if on_progress:
        on_progress(total_steps, total_steps, "RFB: Concluido")
    logger.info(
        "RFB fetch complete: %d partners, %d companies, %d establishments, %d CNPJs",
        len(unique_partners),
        len(all_companies),
        len(all_establishments),
        len(matched_cnpjs),
    )
    return config.output_dir
