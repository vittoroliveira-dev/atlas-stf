"""CGU fetch runner: downloads CEIS/CNEP bulk CSVs (primary) or queries REST API (fallback)."""

from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import httpx

from ..core.identity import normalize_entity_name
from ..core.zip_safety import enforce_max_uncompressed_size
from ._client import CguClient
from ._config import CGU_DOWNLOAD_URL, CguFetchConfig
from ._queries import (
    build_ceis_name_params,
    build_cnep_name_params,
    build_leniencia_name_params,
    normalize_ceis_record,
    normalize_cnep_record,
    normalize_leniencia_record,
)

logger = logging.getLogger(__name__)

_CGU_MAX_ZIP_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
_CGU_MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024
_CGU_DOWNLOAD_CHUNK_SIZE = 64 * 1024
_CGU_MAX_API_PAGES = 100

# ---------------------------------------------------------------------------
# CSV column indices (Portal da Transparencia bulk download)
# ---------------------------------------------------------------------------
#  0: CADASTRO           1: CÓDIGO DA SANÇÃO          2: TIPO DE PESSOA
#  3: CPF OU CNPJ        4: NOME DO SANCIONADO        5: NOME ORG SANCIONADOR
#  6: RAZÃO SOCIAL       7: NOME FANTASIA             8: NÚMERO DO PROCESSO
#  9: CATEGORIA DA SANÇÃO
# CEIS: 10: DATA INÍCIO  11: DATA FINAL  ...  17: ÓRGÃO SANCIONADOR
# CNEP: 10: VALOR MULTA  11: DATA INÍCIO 12: DATA FINAL ... 18: ÓRGÃO SANCIONADOR

_CEIS_COL = {"name": 4, "cpf_cnpj": 3, "type": 9, "start": 10, "end": 11, "body": 17, "desc": 13, "uf": 18}
_CNEP_COL = {"name": 4, "cpf_cnpj": 3, "type": 9, "start": 11, "end": 12, "body": 18, "desc": 14, "uf": 19}

# Leniência CSV columns (Portal da Transparência bulk download — validated from real CSV)
#  0: ID DO ACORDO
#  1: CNPJ DO SANCIONADO
#  2: RAZÃO SOCIAL – CADASTRO RECEITA
#  3: NOME FANTASIA – CADASTRO RECEITA
#  4: DATA DE INÍCIO DO ACORDO
#  5: DATA DE FIM DO ACORDO
#  6: SITUAÇÃO DO ACORDO DE LENIÊNCIA
#  7: DATA DA INFORMAÇÃO
#  8: NÚMERO DO PROCESSO
#  9: TERMOS DO ACORDO
# 10: ÓRGÃO SANCIONADOR
_LENIENCIA_COL = {"name": 2, "cpf_cnpj": 1, "type": 6, "start": 4, "end": 5, "body": 10, "desc": 9}


def _safe_str(row: list[str], idx: int) -> str:
    if idx < len(row):
        val = row[idx].strip()
        return "" if val in ("", "nan") else val
    return ""


def _parse_csv_rows(text: str) -> list[list[str]]:
    """Parse semicolon-separated CSV with quoted fields. Skip header."""
    import csv

    reader = csv.reader(io.StringIO(text), delimiter=";", quotechar='"')
    rows = list(reader)
    return rows[1:] if len(rows) > 1 else []


def _normalize_csv_record(row: list[str], source: str, col_map: dict[str, int]) -> dict[str, Any]:
    """Map a CSV row to the internal sanction schema (same as API normalizers)."""
    return {
        "sanction_source": source,
        "sanction_id": _safe_str(row, 1),
        "entity_name": _safe_str(row, col_map["name"]),
        "entity_cnpj_cpf": _safe_str(row, col_map["cpf_cnpj"]),
        "sanctioning_body": _safe_str(row, col_map["body"]),
        "sanction_type": _safe_str(row, col_map["type"]),
        "sanction_start_date": _safe_str(row, col_map["start"]),
        "sanction_end_date": _safe_str(row, col_map["end"]),
        "sanction_description": _safe_str(row, col_map["desc"]),
        "uf_sancionado": _safe_str(row, col_map["uf"]) if "uf" in col_map else "",
    }


def _normalize_leniencia_record(row: list[str]) -> dict[str, Any]:
    """Normalize a leniência CSV row with name fallback to NOME FANTASIA."""
    rec = _normalize_csv_record(row, "leniencia", _LENIENCIA_COL)
    # Leniência uses NÚMERO DO PROCESSO (col 8) as sanction_id
    rec["sanction_id"] = _safe_str(row, 8)
    if not rec["entity_name"]:
        rec["entity_name"] = _safe_str(row, 3)  # NOME FANTASIA fallback
    return rec


# ---------------------------------------------------------------------------
# CSV download (primary strategy)
# ---------------------------------------------------------------------------


def _find_latest_download_date(max_lookback: int = 7) -> str | None:
    """Find the most recent available download date (YYYYMMDD).

    The portal publishes data daily but with a 1-day lag, so we try
    yesterday first and go back up to `max_lookback` days.
    """
    with httpx.Client(follow_redirects=True, timeout=10) as client:
        for offset in range(1, max_lookback + 1):
            d = date.today() - timedelta(days=offset)
            date_str = d.strftime("%Y%m%d")
            try:
                r = client.head(f"{CGU_DOWNLOAD_URL}/ceis/{date_str}")
                if r.status_code == 200:
                    return date_str
            except httpx.RequestError:
                continue
    return None


def _download_and_extract_csv(dataset: str, date_str: str, output_dir: Path) -> Path | None:
    """Download a ZIP from Portal da Transparencia and extract the CSV."""
    url = f"{CGU_DOWNLOAD_URL}/{dataset}/{date_str}"
    logger.info("Downloading %s from %s", dataset.upper(), url)
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / f"{dataset}.zip"
    try:
        request = Request(
            url,
            headers={
                "User-Agent": "Python-urllib/3 atlas-stf",
                "Accept": "application/zip, application/octet-stream;q=0.9, */*;q=0.1",
            },
        )
        total = 0
        with urlopen(request, timeout=120) as response, zip_path.open("wb") as fh:
            while True:
                chunk = response.read(_CGU_DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                total += len(chunk)
                if total > _CGU_MAX_DOWNLOAD_BYTES:
                    raise ValueError(f"download exceeded max bytes ({total} > {_CGU_MAX_DOWNLOAD_BYTES})")
                fh.write(chunk)
    except (ValueError, HTTPError, URLError) as exc:
        logger.warning("Failed to download %s: %s", dataset, exc)
        zip_path.unlink(missing_ok=True)
        return None

    try:
        with zipfile.ZipFile(zip_path) as zf:
            csv_infos = [
                info
                for info in zf.infolist()
                if info.filename.endswith(".csv") and ".." not in info.filename and not info.filename.startswith("/")
            ]
            if not csv_infos:
                logger.warning("No CSV found in %s ZIP", dataset)
                return None
            csv_info = csv_infos[0]
            enforce_max_uncompressed_size(
                [csv_info],
                max_total_uncompressed_bytes=_CGU_MAX_ZIP_UNCOMPRESSED_BYTES,
            )
            csv_content = zf.read(csv_info)
    except zipfile.BadZipFile:
        logger.warning("Invalid ZIP for %s", dataset)
        return None
    except ValueError as exc:
        logger.warning("Refusing %s ZIP: %s", dataset, exc)
        return None
    finally:
        zip_path.unlink(missing_ok=True)

    csv_path = output_dir / f"{dataset}.csv"
    csv_path.write_bytes(csv_content)
    logger.info("Extracted %s (%d bytes)", csv_path, len(csv_content))
    return csv_path


def _load_csv_sanctions(csv_path: Path, source: str) -> list[dict[str, Any]]:
    """Read a CEIS/CNEP CSV and return normalized sanction records."""
    col_map = _CEIS_COL if source == "ceis" else _CNEP_COL
    # Portal CSVs use latin-1 encoding
    text = csv_path.read_text(encoding="latin-1")
    rows = _parse_csv_rows(text)
    records: list[dict[str, Any]] = []
    for row in rows:
        rec = _normalize_csv_record(row, source, col_map)
        if rec["entity_name"]:
            records.append(rec)
    return records


def _load_leniencia_csv(csv_path: Path) -> list[dict[str, Any]]:
    """Read a leniência CSV and return normalized sanction records."""
    text = csv_path.read_text(encoding="latin-1")
    rows = _parse_csv_rows(text)
    records: list[dict[str, Any]] = []
    for row in rows:
        rec = _normalize_leniencia_record(row)
        if rec["entity_name"]:
            records.append(rec)
    return records


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

    if on_progress:
        on_progress(total, total, "CGU: Concluído")
    return records


# ---------------------------------------------------------------------------
# API REST fallback
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


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
