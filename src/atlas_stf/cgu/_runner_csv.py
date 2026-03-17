"""CGU CSV processing helpers: column maps, parsing, normalization, download."""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import httpx

from ..core.identity import normalize_tax_id
from ..core.zip_safety import enforce_max_uncompressed_size, is_safe_zip_member
from ._checkpoint import _CguCheckpoint, _DatasetMeta
from ._config import CGU_DOWNLOAD_URL

logger = logging.getLogger(__name__)

_CGU_MAX_ZIP_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
_CGU_MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024
_CGU_DOWNLOAD_CHUNK_SIZE = 64 * 1024

# CSV column indices — 0: CADASTRO  1: SANÇÃO  2: TIPO PESSOA  3: CPF/CNPJ
# 4: NOME  5: ORG SANCIONADOR  6: RAZÃO SOCIAL  7: FANTASIA  8: PROCESSO  9: CATEGORIA
# CEIS: 10: INÍCIO  11: FINAL  ...  17: ÓRGÃO   CNEP: 10: MULTA  11: INÍCIO  12: FINAL  ... 18: ÓRGÃO

_CEIS_COL = {"name": 4, "cpf_cnpj": 3, "type": 9, "start": 10, "end": 11, "body": 17, "desc": 13, "uf": 18}
_CNEP_COL = {"name": 4, "cpf_cnpj": 3, "type": 9, "start": 11, "end": 12, "body": 18, "desc": 14, "uf": 19}

# Leniência: 0: ID  1: CNPJ  2: RAZÃO SOCIAL  3: NOME FANTASIA  4: INÍCIO
#   5: FIM  6: SITUAÇÃO  7: DATA INFO  8: NÚMERO PROCESSO  9: TERMOS  10: ÓRGÃO
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


_TIPO_PESSOA_MAP: dict[str, str] = {
    "F": "PF",
    "J": "PJ",
    "PF": "PF",
    "PJ": "PJ",
}


def _canonicalize_tipo_pessoa(raw: str) -> str:
    """Canonicalize TIPO PESSOA from CGU CSV. Returns 'PF', 'PJ', or ''."""
    return _TIPO_PESSOA_MAP.get(raw.strip().upper(), "")


def _normalize_date(raw: str) -> str | None:
    """Attempt to parse date to YYYY-MM-DD. Returns None on failure."""
    val = raw.strip()
    if not val:
        return None
    from datetime import datetime as _dt

    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            return _dt.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _normalize_csv_record(row: list[str], source: str, col_map: dict[str, int]) -> dict[str, Any]:
    """Map a CSV row to the internal sanction schema (same as API normalizers)."""
    cpf_cnpj_raw = _safe_str(row, col_map["cpf_cnpj"])
    start_raw = _safe_str(row, col_map["start"])
    end_raw = _safe_str(row, col_map["end"])
    # TIPO PESSOA is in column 2 for CEIS/CNEP
    tipo_raw = _safe_str(row, 2)

    return {
        "sanction_source": source,
        "sanction_id": _safe_str(row, 1),
        "entity_name": _safe_str(row, col_map["name"]),
        "entity_cnpj_cpf": normalize_tax_id(cpf_cnpj_raw) or "",
        "entity_cnpj_cpf_raw": cpf_cnpj_raw,
        "entity_type_pf_pj": _canonicalize_tipo_pessoa(tipo_raw),
        "entity_type_pf_pj_raw": tipo_raw,
        "sanctioning_body": _safe_str(row, col_map["body"]),
        "sanction_type": _safe_str(row, col_map["type"]),
        "sanction_start_date": _normalize_date(start_raw),
        "sanction_start_date_raw": start_raw,
        "sanction_end_date": _normalize_date(end_raw),
        "sanction_end_date_raw": end_raw,
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
    # Leniência has no TIPO PESSOA column — always PJ (acordos de leniência)
    rec["entity_type_pf_pj"] = "PJ"
    rec["entity_type_pf_pj_raw"] = ""
    return rec


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


def _head_content_length(url: str) -> int | None:
    """Issue a HEAD request and return Content-Length, or None on failure."""
    try:
        r = httpx.head(url, follow_redirects=True, timeout=15)
        if r.status_code == 200:
            cl = r.headers.get("content-length")
            return int(cl) if cl else None
    except httpx.RequestError, ValueError:
        pass
    return None


def _download_and_extract_csv(
    dataset: str,
    date_str: str,
    output_dir: Path,
    checkpoint: _CguCheckpoint | None = None,
) -> Path | None:
    """Download a ZIP from Portal da Transparencia and extract the CSV."""
    url = f"{CGU_DOWNLOAD_URL}/{dataset}/{date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{dataset}.csv"

    # --- Incremental check: skip download if unchanged ---
    remote_cl = _head_content_length(url)
    if (
        checkpoint is not None
        and remote_cl is not None
        and dataset in checkpoint.completed_datasets
        and csv_path.exists()
        and checkpoint.completed_datasets[dataset].matches(remote_cl, date_str)
    ):
        logger.info("CGU %s: unchanged, skipping download", dataset)
        return csv_path

    logger.info("Downloading %s from %s", dataset.upper(), url)
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
                if info.filename.endswith(".csv") and is_safe_zip_member(info.filename, output_dir)
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

    # Portal CSVs are Latin-1 encoded — transcode to UTF-8 on disk
    csv_text = csv_content.decode("latin-1")
    csv_path.write_text(csv_text, encoding="utf-8")
    logger.info("Extracted %s (%d chars, transcoded to UTF-8)", csv_path, len(csv_text))

    # --- Save metadata to checkpoint ---
    if checkpoint is not None and remote_cl is not None:
        checkpoint.completed_datasets[dataset] = _DatasetMeta(content_length=remote_cl, download_date=date_str)

    return csv_path


def _read_csv_text(csv_path: Path) -> str:
    """Read CSV text, trying UTF-8 first then falling back to Latin-1."""
    try:
        return csv_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return csv_path.read_text(encoding="latin-1")


def _load_csv_sanctions(csv_path: Path, source: str) -> list[dict[str, Any]]:
    """Read a CEIS/CNEP CSV and return normalized sanction records."""
    col_map = _CEIS_COL if source == "ceis" else _CNEP_COL
    text = _read_csv_text(csv_path)
    rows = _parse_csv_rows(text)
    records: list[dict[str, Any]] = []
    for row in rows:
        rec = _normalize_csv_record(row, source, col_map)
        if rec["entity_name"]:
            records.append(rec)
    return records


def _load_leniencia_csv(csv_path: Path) -> list[dict[str, Any]]:
    """Read a leniência CSV and return normalized sanction records."""
    text = _read_csv_text(csv_path)
    rows = _parse_csv_rows(text)
    records: list[dict[str, Any]] = []
    for row in rows:
        rec = _normalize_leniencia_record(row)
        if rec["entity_name"]:
            records.append(rec)
    return records
