"""RFB fetch pass logic: download + parse Socios, Empresas, Estabelecimentos."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ._enrichment import (
    enrich_company_record,
    enrich_establishment_record,
    enrich_partner_record,
)
from ._parser import (
    parse_empresas_csv_filtered_text,
    parse_socios_csv_filtered_text,
)
from ._parser_estabelecimentos import parse_estabelecimentos_csv_filtered_text
from ._reference import load_all_reference_tables

logger = logging.getLogger(__name__)


def run_pass1_socios(
    *,
    base_url: str,
    socios_file_count: int,
    config_output_dir: Path,
    config_timeout: int,
    target_names: set[str],
    checkpoint: dict[str, Any],
    download_zip: Callable[..., Path | None],
    parse_csv_from_zip_text: Callable[..., Any],
    save_checkpoint: Callable[[Path, dict[str, Any]], None],
    on_progress: Callable[[int, int, str], None] | None,
    step: int,
    total_steps: int,
    target_cpfs: set[str] | None = None,
    target_partner_cnpjs: set[str] | None = None,
    manifest_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], set[str], int]:
    """Pass 1: Scan Socios for name matches. Returns (partners, matched_cnpjs, step)."""
    all_partners: list[dict[str, Any]] = []
    matched_cnpjs: set[str] = set(checkpoint.get("cnpjs", []))
    completed_p1 = set(checkpoint.get("completed_socios_pass1", []))
    _cpfs = target_cpfs or set()
    _partner_cnpjs = target_partner_cnpjs or set()

    for i in range(socios_file_count):
        if i in completed_p1:
            step += 1
            continue

        if on_progress:
            on_progress(step, total_steps, f"RFB: Pass 1 — Socios{i}.zip")
        url = f"{base_url}/Socios{i}.zip"
        cache_path = config_output_dir / f"Socios{i}.zip"
        zip_path = download_zip(url, cache_path, config_timeout)
        if zip_path is None:
            continue

        cpfs_snapshot = frozenset(_cpfs)
        pcnpjs_snapshot = frozenset(_partner_cnpjs)
        parsed = parse_csv_from_zip_text(
            zip_path,
            lambda text_fh, _c=cpfs_snapshot, _p=pcnpjs_snapshot: parse_socios_csv_filtered_text(
                text_fh, target_names, set(), target_cpfs=set(_c), target_partner_cnpjs=set(_p)
            ),
            manifest_dir=manifest_dir,
        )
        if parsed is None:
            continue

        records, new_cnpjs = parsed
        all_partners.extend(records)
        matched_cnpjs.update(new_cnpjs)

        completed_p1.add(i)
        checkpoint["completed_socios_pass1"] = sorted(completed_p1)
        checkpoint["cnpjs"] = sorted(matched_cnpjs)
        save_checkpoint(config_output_dir, checkpoint)
        step += 1
        logger.info("Pass 1 - Socios%d: %d records, %d CNPJs so far", i, len(records), len(matched_cnpjs))

    return all_partners, matched_cnpjs, step


def run_pass2_socios(
    *,
    base_url: str,
    socios_file_count: int,
    config_output_dir: Path,
    config_timeout: int,
    matched_cnpjs: set[str],
    checkpoint: dict[str, Any],
    download_zip: Callable[..., Path | None],
    parse_csv_from_zip_text: Callable[..., Any],
    save_checkpoint: Callable[[Path, dict[str, Any]], None],
    on_progress: Callable[[int, int, str], None] | None,
    step: int,
    total_steps: int,
    manifest_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Pass 2: Re-scan Socios for co-partners of matched CNPJs."""
    all_partners: list[dict[str, Any]] = []
    completed_p2 = set(checkpoint.get("completed_socios_pass2", []))

    for i in range(socios_file_count):
        if i in completed_p2:
            step += 1
            continue

        if on_progress:
            on_progress(step, total_steps, f"RFB: Pass 2 — Socios{i}.zip")
        cache_path = config_output_dir / f"Socios{i}.zip"
        if cache_path.exists():
            zip_path = cache_path
        else:
            url = f"{base_url}/Socios{i}.zip"
            zip_path_opt = download_zip(url, cache_path, config_timeout)
            if zip_path_opt is None:
                continue
            zip_path = zip_path_opt

        parsed = parse_csv_from_zip_text(
            zip_path,
            lambda text_fh: parse_socios_csv_filtered_text(text_fh, set(), matched_cnpjs),
            manifest_dir=manifest_dir,
        )
        if parsed is None:
            continue

        records, _ = parsed
        all_partners.extend(records)

        completed_p2.add(i)
        checkpoint["completed_socios_pass2"] = sorted(completed_p2)
        save_checkpoint(config_output_dir, checkpoint)
        step += 1
        logger.info("Pass 2 - Socios%d: %d co-partner records", i, len(records))

    return all_partners, step


def run_pass3_empresas(
    *,
    base_url: str,
    empresas_file_count: int,
    config_output_dir: Path,
    config_timeout: int,
    matched_cnpjs: set[str],
    checkpoint: dict[str, Any],
    download_zip: Callable[..., Path | None],
    parse_csv_from_zip_text: Callable[..., Any],
    save_checkpoint: Callable[[Path, dict[str, Any]], None],
    on_progress: Callable[[int, int, str], None] | None,
    step: int,
    total_steps: int,
    manifest_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Pass 3: Download Empresas matching previously found CNPJs."""
    all_companies: list[dict[str, Any]] = []
    completed_e = set(checkpoint.get("completed_empresas", []))

    for i in range(empresas_file_count):
        if i in completed_e:
            step += 1
            continue

        if on_progress:
            on_progress(step, total_steps, f"RFB: Pass 3 — Empresas{i}.zip")
        url = f"{base_url}/Empresas{i}.zip"
        zip_path = download_zip(url, config_output_dir / f"Empresas{i}.zip", config_timeout)
        if zip_path is None:
            continue

        try:
            parsed = parse_csv_from_zip_text(
                zip_path,
                lambda text_fh: parse_empresas_csv_filtered_text(text_fh, matched_cnpjs),
                manifest_dir=manifest_dir,
            )
            if parsed is None:
                continue
            records = parsed
            all_companies.extend(records)
        finally:
            zip_path.unlink(missing_ok=True)

        completed_e.add(i)
        checkpoint["completed_empresas"] = sorted(completed_e)
        save_checkpoint(config_output_dir, checkpoint)
        step += 1
        logger.info("Empresas%d: %d company records", i, len(records))

    return all_companies, step


def run_pass4_estabelecimentos(
    *,
    base_url: str,
    estabelecimentos_file_count: int,
    config_output_dir: Path,
    config_timeout: int,
    matched_cnpjs: set[str],
    checkpoint: dict[str, Any],
    download_zip: Callable[..., Path | None],
    parse_csv_from_zip_text: Callable[..., Any],
    save_checkpoint: Callable[[Path, dict[str, Any]], None],
    on_progress: Callable[[int, int, str], None] | None,
    step: int,
    total_steps: int,
    manifest_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Pass 4: Download Estabelecimentos matching previously found CNPJs."""
    all_establishments: list[dict[str, Any]] = []
    completed_est = set(checkpoint.get("completed_estabelecimentos", []))

    for i in range(estabelecimentos_file_count):
        if i in completed_est:
            step += 1
            continue

        if on_progress:
            on_progress(step, total_steps, f"RFB: Pass 4 — Estabelecimentos{i}.zip")
        url = f"{base_url}/Estabelecimentos{i}.zip"
        zip_path = download_zip(url, config_output_dir / f"Estabelecimentos{i}.zip", config_timeout)
        if zip_path is None:
            continue

        try:
            parsed = parse_csv_from_zip_text(
                zip_path,
                lambda text_fh: parse_estabelecimentos_csv_filtered_text(text_fh, matched_cnpjs),
                manifest_dir=manifest_dir,
            )
            if parsed is None:
                continue
            records = parsed
            all_establishments.extend(records)
        finally:
            zip_path.unlink(missing_ok=True)

        completed_est.add(i)
        checkpoint["completed_estabelecimentos"] = sorted(completed_est)
        save_checkpoint(config_output_dir, checkpoint)
        step += 1
        logger.info("Estabelecimentos%d: %d records", i, len(records))

    return all_establishments, step


def _safe_write_jsonl(
    path: Path,
    records: list[dict[str, Any]],
    enrich_fn: Callable[[dict[str, Any]], Any],
    label: str,
) -> None:
    """Write enriched records to JSONL, with guard against empty overwrites.

    Never overwrites an existing file that has content with an empty result.
    This prevents data loss when checkpoint-skipped passes return empty lists.
    """
    if not records:
        if path.exists() and path.stat().st_size > 0:
            logger.info("Skipping %s write — no new data and file already has content", label)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
        return
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            enrich_fn(r)
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    logger.info("Wrote %d %s records", len(records), label)


def write_partners_jsonl(
    config_output_dir: Path,
    unique_partners: list[dict[str, Any]],
) -> int:
    """Enrich and write partners_raw.jsonl. Returns record count written (or preserved)."""
    tables = load_all_reference_tables(config_output_dir)
    qualificacoes = tables.get("qualificacoes", {})
    _safe_write_jsonl(
        config_output_dir / "partners_raw.jsonl",
        unique_partners,
        lambda p: enrich_partner_record(p, qualificacoes),
        "partner",
    )
    path = config_output_dir / "partners_raw.jsonl"
    if path.exists() and path.stat().st_size > 0:
        return len(unique_partners) if unique_partners else _count_jsonl_lines(path)
    return 0


def write_companies_jsonl(
    config_output_dir: Path,
    all_companies: list[dict[str, Any]],
) -> int:
    """Enrich and write companies_raw.jsonl. Returns record count written (or preserved)."""
    tables = load_all_reference_tables(config_output_dir)
    naturezas = tables.get("naturezas", {})
    _safe_write_jsonl(
        config_output_dir / "companies_raw.jsonl",
        all_companies,
        lambda c: enrich_company_record(c, naturezas),
        "company",
    )
    path = config_output_dir / "companies_raw.jsonl"
    if path.exists() and path.stat().st_size > 0:
        return len(all_companies) if all_companies else _count_jsonl_lines(path)
    return 0


def write_establishments_jsonl(
    config_output_dir: Path,
    all_establishments: list[dict[str, Any]],
) -> int:
    """Enrich and write establishments_raw.jsonl. Returns record count written (or preserved)."""
    tables = load_all_reference_tables(config_output_dir)
    cnaes = tables.get("cnaes", {})
    municipios = tables.get("municipios", {})
    motivos = tables.get("motivos", {})
    _safe_write_jsonl(
        config_output_dir / "establishments_raw.jsonl",
        all_establishments,
        lambda e: enrich_establishment_record(e, cnaes, municipios, motivos),
        "establishment",
    )
    path = config_output_dir / "establishments_raw.jsonl"
    if path.exists() and path.stat().st_size > 0:
        return len(all_establishments) if all_establishments else _count_jsonl_lines(path)
    return 0


def _count_jsonl_lines(path: Path) -> int:
    """Count non-empty lines in a JSONL file."""
    count = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


def enrich_and_write_results(
    *,
    config_output_dir: Path,
    unique_partners: list[dict[str, Any]],
    all_companies: list[dict[str, Any]],
    all_establishments: list[dict[str, Any]],
) -> None:
    """Enrich records with reference table labels and write JSONL output.

    This function is kept for backward compatibility with tests that call it directly.
    In the main pipeline, prefer the per-artifact write functions to eliminate the gap
    between pass completion and output commit.
    """
    write_partners_jsonl(config_output_dir, unique_partners)
    write_companies_jsonl(config_output_dir, all_companies)
    write_establishments_jsonl(config_output_dir, all_establishments)
