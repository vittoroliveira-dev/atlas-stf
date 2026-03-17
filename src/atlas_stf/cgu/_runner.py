"""CGU fetch runner: downloads CEIS/CNEP bulk CSVs (primary) or queries REST API (fallback)."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name
from ._checkpoint import _CguCheckpoint
from ._client import CguClient
from ._config import CguFetchConfig
from ._queries import (
    build_ceis_name_params,
    build_cnep_name_params,
    build_leniencia_name_params,
    normalize_ceis_record,
    normalize_cnep_record,
    normalize_leniencia_record,
)
from ._runner_csv import (  # noqa: F401 — re-exports for test/smoke compatibility
    _CEIS_COL,
    _canonicalize_tipo_pessoa,
    _download_and_extract_csv,
    _find_latest_download_date,
    _load_csv_sanctions,
    _load_leniencia_csv,
    _normalize_csv_record,
    _normalize_date,
    _normalize_leniencia_record,
)

logger = logging.getLogger(__name__)

_CGU_MAX_API_PAGES = 100

# Entity-detection heuristic for filtering party names (API-only path)
_ENTITY_RE = re.compile(
    r"""(?x)
      \bLTDA\.?\b | \bS[./]A\.?\b | \bS[./]C\b | \bEIRELI\b | \bEPP\b
    | \bFUNDA[CÇ][AÃ]O\b | \bASSOCIA[CÇ][AÃ]O\b
    | \bINSTITUTO\b | \bCOOPERATIVA\b | \bCONS[OÓ]RCIO\b
    | \bCONSTRUTORA\b | \bENGENHARIA\b | \bIND[UÚ]STRIA\b
    | \bCOM[EÉ]RCIO\b | \bDISTRIBUIDORA\b | \bIMPORTADORA\b
    | \bINCORPORA[CÇ][AÃ]O\b | \bEMPREENDIMENTOS\b
    | \bCONCESSION[AÁ]RIA\b | \bPETROQU[IÍ]MICA\b
    | \bMINERA[CÇ][AÃ]O\b | \bSIDER[UÚ]RGICA\b
    | \bRECUPERA[CÇ][AÃ]O\s+JUDICIAL\b
    """,
)
_PERSON_NOISE_RE = re.compile(r"\bE\s+OUTRO\(?A/?S?\)?\b")


def _looks_like_entity(name: str) -> bool:
    """Heuristic: return True if name looks like a legal entity."""
    cleaned = _PERSON_NOISE_RE.sub("", name.upper()).strip()
    return bool(_ENTITY_RE.search(cleaned))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _extract_unique_names(party_path: Path) -> list[str]:
    """Extract unique normalized party names that look like legal entities."""
    seen: set[str] = set()
    names: list[str] = []
    for record in _read_jsonl(party_path):
        raw_name = record.get("party_name_normalized") or record.get("party_name_raw", "")
        normalized = normalize_entity_name(raw_name)
        if normalized and normalized not in seen and _looks_like_entity(normalized):
            seen.add(normalized)
            names.append(normalized)
    return names


def _load_checkpoint(output_dir: Path) -> set[str]:
    checkpoint_path = output_dir / "_checkpoint.json"
    if checkpoint_path.exists():
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return set(data.get("completed", []))
    return set()


def _save_checkpoint(output_dir: Path, completed: set[str]) -> None:
    checkpoint_path = output_dir / "_checkpoint.json"
    checkpoint_path.write_text(
        json.dumps({"completed": sorted(completed)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _search_all_pages(
    client: CguClient,
    search_fn: str,
    build_params_fn: Any,
    normalize_fn: Any,
    name: str,
    *,
    max_pages: int = _CGU_MAX_API_PAGES,
) -> list[dict[str, Any]]:
    """Paginate through all results for a given name."""
    results: list[dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        params = build_params_fn(name, page)
        raw_list = getattr(client, search_fn)(params)
        if not raw_list:
            break
        for raw in raw_list:
            results.append(normalize_fn(raw))
        if len(raw_list) < 15:
            break
        page += 1
    else:
        logger.warning(
            "Reached max pages (%d) for %s query=%r",
            max_pages,
            search_fn,
            name,
        )
    return results


def _fetch_via_csv(
    config: CguFetchConfig,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[dict[str, Any]] | None:
    """Primary strategy: download bulk CSVs and return all sanction records."""
    date_str = _find_latest_download_date()
    if not date_str:
        logger.warning("Could not find available download date")
        return None

    logger.info("Using Portal da Transparencia bulk download for %s", date_str)
    checkpoint = _CguCheckpoint.load(config.output_dir)
    checkpoint.download_date = date_str
    records: list[dict[str, Any]] = []

    _DATASETS: list[tuple[str, str]] = [
        ("ceis", "ceis"),
        ("cnep", "cnep"),
        ("acordos-leniencia", "leniencia"),
    ]

    total = len(_DATASETS)
    for i, (dataset, source) in enumerate(_DATASETS):
        if on_progress:
            on_progress(i, total, f"CGU: Baixando {dataset.upper()}...")
        csv_path = _download_and_extract_csv(dataset, date_str, config.output_dir, checkpoint)
        if csv_path is None:
            if source == "leniencia":
                logger.warning("Leniência CSV unavailable — skipping (optional)")
                continue
            logger.warning("CSV download failed for %s, aborting CSV strategy", dataset)
            return None
        if source == "leniencia":
            dataset_records = _load_leniencia_csv(csv_path)
        else:
            dataset_records = _load_csv_sanctions(csv_path, source)
        records.extend(dataset_records)
        logger.info("Loaded %d records from %s CSV", len(dataset_records), dataset.upper())
        checkpoint.save(config.output_dir)

    if on_progress:
        on_progress(total, total, "CGU: Concluído")
    return records


def _fetch_via_api(
    config: CguFetchConfig,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[dict[str, Any]]:
    """Fallback strategy: query REST API per entity name."""
    names = _extract_unique_names(config.party_path)
    logger.info("API fallback: %d entity names to query", len(names))

    completed = _load_checkpoint(config.output_dir)
    all_records: list[dict[str, Any]] = []
    output_path = config.output_dir / "sanctions_raw.jsonl"
    total = len(names)

    with CguClient(
        config.api_key,
        timeout=config.timeout_seconds,
        rate_limit=config.rate_limit_seconds,
        max_retries=config.max_retries,
    ) as client:
        with output_path.open("a", encoding="utf-8") as fh:
            for i, name in enumerate(names):
                if name in completed:
                    continue

                if on_progress:
                    on_progress(i, total, f"CGU API: {i + 1}/{total} entidades")
                logger.info("Querying name %d/%d: %s", i + 1, len(names), name)

                ceis_results = _search_all_pages(
                    client,
                    "search_ceis",
                    build_ceis_name_params,
                    normalize_ceis_record,
                    name,
                )
                cnep_results = _search_all_pages(
                    client,
                    "search_cnep",
                    build_cnep_name_params,
                    normalize_cnep_record,
                    name,
                )
                leniencia_results = _search_all_pages(
                    client,
                    "search_leniencia",
                    build_leniencia_name_params,
                    normalize_leniencia_record,
                    name,
                )

                for record in ceis_results + cnep_results + leniencia_results:
                    record["query_name"] = name
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                    all_records.append(record)

                completed.add(name)
                _save_checkpoint(config.output_dir, completed)

                if ceis_results or cnep_results or leniencia_results:
                    logger.info(
                        "Found %d CEIS + %d CNEP + %d Leniência records for: %s",
                        len(ceis_results),
                        len(cnep_results),
                        len(leniencia_results),
                        name,
                    )

    if on_progress:
        on_progress(total, total, "CGU API: Concluído")
    return all_records


def fetch_sanctions_data(
    config: CguFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Fetch CEIS/CNEP/Leniência data: CSV download (primary) with API REST fallback.

    Returns the output directory path.
    """
    config.output_dir.mkdir(parents=True, exist_ok=True)

    if config.dry_run:
        logger.info("[dry-run] Would download CEIS/CNEP/Leniência CSVs from Portal da Transparencia")
        if config.api_key:
            logger.info("[dry-run] API key provided — would fall back to REST API if CSV fails")
        return config.output_dir

    # Strategy 1: Bulk CSV download (no auth, fast, complete)
    records = _fetch_via_csv(config, on_progress)
    strategy = "csv"

    # Strategy 2: REST API fallback (requires auth, slow, filtered by entity name)
    if records is None:
        if not config.api_key:
            raise RuntimeError(
                "CSV download failed and no API key provided. Set CGU_API_KEY or pass --api-key for REST API fallback."
            )
        logger.info("CSV download unavailable, falling back to REST API")
        records = _fetch_via_api(config, on_progress)
        strategy = "api"

    # Write sanctions_raw.jsonl (CSV strategy writes full dataset; API appends incrementally)
    if strategy == "csv":
        output_path = config.output_dir / "sanctions_raw.jsonl"
        with output_path.open("w", encoding="utf-8") as fh:
            for record in records:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info(
        "CGU fetch complete (%s): %d sanction records written to %s",
        strategy,
        len(records),
        config.output_dir,
    )
    return config.output_dir
