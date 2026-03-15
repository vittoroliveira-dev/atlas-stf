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
) -> tuple[list[dict[str, Any]], set[str], int]:
    """Pass 1: Scan Socios for name matches. Returns (partners, matched_cnpjs, step)."""
    all_partners: list[dict[str, Any]] = []
    matched_cnpjs: set[str] = set(checkpoint.get("cnpjs", []))
    completed_p1 = set(checkpoint.get("completed_socios_pass1", []))

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

        parsed = parse_csv_from_zip_text(
            zip_path,
            lambda text_fh: parse_socios_csv_filtered_text(text_fh, target_names, set()),
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


def enrich_and_write_results(
    *,
    config_output_dir: Path,
    unique_partners: list[dict[str, Any]],
    all_companies: list[dict[str, Any]],
    all_establishments: list[dict[str, Any]],
) -> None:
    """Enrich records with reference table labels and write JSONL output."""
    tables = load_all_reference_tables(config_output_dir)
    qualificacoes = tables.get("qualificacoes", {})
    naturezas = tables.get("naturezas", {})
    cnaes = tables.get("cnaes", {})
    municipios = tables.get("municipios", {})
    motivos = tables.get("motivos", {})

    # Enrich and write partners
    partners_path = config_output_dir / "partners_raw.jsonl"
    with partners_path.open("w", encoding="utf-8") as fh:
        for p in unique_partners:
            enrich_partner_record(p, qualificacoes)
            fh.write(json.dumps(p, ensure_ascii=False) + "\n")
    logger.info("Wrote %d unique partner records", len(unique_partners))

    # Enrich and write companies
    companies_path = config_output_dir / "companies_raw.jsonl"
    with companies_path.open("w", encoding="utf-8") as fh:
        for c in all_companies:
            enrich_company_record(c, naturezas)
            fh.write(json.dumps(c, ensure_ascii=False) + "\n")
    logger.info("Wrote %d company records", len(all_companies))

    # Enrich and write establishments
    establishments_path = config_output_dir / "establishments_raw.jsonl"
    with establishments_path.open("w", encoding="utf-8") as fh:
        for e in all_establishments:
            enrich_establishment_record(e, cnaes, municipios, motivos)
            fh.write(json.dumps(e, ensure_ascii=False) + "\n")
    logger.info("Wrote %d establishment records", len(all_establishments))
