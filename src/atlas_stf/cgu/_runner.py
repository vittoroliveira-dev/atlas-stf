"""CGU fetch runner: downloads CEIS/CNEP bulk CSVs (primary) or queries REST API (fallback)."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..core.fetch_lock import FetchLock
from ..core.fetch_result import FetchTimer
from ..core.identity import normalize_entity_name
from ..fetch._manifest_model import FetchUnit, RemoteState, SourceManifest, build_unit_id
from ..fetch._manifest_store import load_manifest, write_manifest_unlocked
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
    manifest = load_manifest("cgu", config.output_dir) or SourceManifest(source="cgu")
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
        csv_path = _download_and_extract_csv(dataset, date_str, config.output_dir)
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

        uid = build_unit_id("cgu", dataset.replace("-", "_"))
        manifest.units[uid] = FetchUnit(
            unit_id=uid,
            source="cgu",
            label=f"CGU {dataset}",
            remote_url=f"https://portaldatransparencia.gov.br/download-de-dados/{dataset}/{date_str}",
            remote_state=RemoteState(url=""),
            status="committed",
            fetch_date=date_str,
        )
        manifest.last_updated = date_str
        write_manifest_unlocked(manifest, config.output_dir)

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

    manifest = load_manifest("cgu", config.output_dir) or SourceManifest(source="cgu")
    api_completed: set[str] = set()
    # Load API progress from manifest metadata if available
    api_unit = manifest.units.get("cgu:api_progress")
    if api_unit and api_unit.metadata:
        api_completed = set(api_unit.metadata.get("completed_names", []))

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
                if name in api_completed:
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

                api_completed.add(name)
                manifest.units["cgu:api_progress"] = FetchUnit(
                    unit_id="cgu:api_progress",
                    source="cgu",
                    label="CGU API progress",
                    remote_url="",
                    remote_state=RemoteState(url=""),
                    status="downloaded",
                    metadata={"completed_names": sorted(api_completed)},
                )
                write_manifest_unlocked(manifest, config.output_dir)

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

    if config.force_refresh:
        logger.info("CGU: force-refresh — clearing manifest")
        # Just start fresh — manifest will be overwritten by _fetch_via_csv

    if config.dry_run:
        logger.info("[dry-run] Would download CEIS/CNEP/Leniência CSVs from Portal da Transparencia")
        if config.api_key:
            logger.info("[dry-run] API key provided — would fall back to REST API if CSV fails")
        return config.output_dir

    with FetchLock(config.output_dir, "cgu"):
        return _fetch_sanctions_data_locked(config, on_progress=on_progress)


def _fetch_sanctions_data_locked(
    config: CguFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Inner implementation guarded by FetchLock."""
    timer = FetchTimer("cgu")
    timer.start()
    try:
        # Strategy 1: Bulk CSV download (no auth, fast, complete)
        records = _fetch_via_csv(config, on_progress)
        strategy = "csv"

        # Strategy 2: REST API fallback (requires auth, slow, filtered by entity name)
        if records is None:
            if not config.api_key:
                raise RuntimeError(
                    "CSV download failed and no API key provided. "
                    "Set CGU_API_KEY or pass --api-key for REST API fallback."
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

        timer.log_success(records_written=len(records), detail=strategy)
        return config.output_dir
    except Exception as exc:
        timer.log_failure(exc)
        raise
