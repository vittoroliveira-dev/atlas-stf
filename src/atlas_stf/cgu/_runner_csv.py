"""CGU CSV processing helpers: column maps, parsing, normalization, download."""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx

from ..core.identity import normalize_tax_id
from ..core.zip_safety import enforce_max_uncompressed_size, is_safe_zip_member
from ..ingest_manifest import capture_csv_manifest, normalize_header_value, write_manifest
from ._config import CGU_DOWNLOAD_URL

logger = logging.getLogger(__name__)

_CGU_MAX_ZIP_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
_CGU_MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024
_CGU_DOWNLOAD_CHUNK_SIZE = 64 * 1024

# Portal da Transparência uses AWS WAF that blocks non-browser User-Agents.
_CGU_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36 atlas-stf/1.0"
)

# CSV column indices — 0: CADASTRO  1: SANÇÃO  2: TIPO PESSOA  3: CPF/CNPJ
# 4: NOME  5: ORG SANCIONADOR  6: RAZÃO SOCIAL  7: FANTASIA  8: PROCESSO  9: CATEGORIA
# CEIS: 10: INÍCIO  11: FINAL  ...  17: ÓRGÃO   CNEP: 10: MULTA  11: INÍCIO  12: FINAL  ... 18: ÓRGÃO

_CEIS_COL = {"name": 4, "cpf_cnpj": 3, "type": 9, "start": 10, "end": 11, "body": 17, "desc": 13, "uf": 18}
_CNEP_COL = {"name": 4, "cpf_cnpj": 3, "type": 9, "start": 11, "end": 12, "body": 18, "desc": 14, "uf": 19}

# Leniência: 0: ID  1: CNPJ  2: RAZÃO SOCIAL  3: NOME FANTASIA  4: INÍCIO
#   5: FIM  6: SITUAÇÃO  7: DATA INFO  8: NÚMERO PROCESSO  9: TERMOS  10: ÓRGÃO
_LENIENCIA_COL = {"name": 2, "cpf_cnpj": 1, "type": 6, "start": 4, "end": 5, "body": 10, "desc": 9}

# Real headers from Portal da Transparencia CSVs (verified against disk)
_CEIS_EXPECTED_HEADER: list[str] = [
    "CADASTRO",
    "CÓDIGO DA SANÇÃO",
    "TIPO DE PESSOA",
    "CPF OU CNPJ DO SANCIONADO",
    "NOME DO SANCIONADO",
    "NOME INFORMADO PELO ÓRGÃO SANCIONADOR",
    "RAZÃO SOCIAL - CADASTRO RECEITA",
    "NOME FANTASIA - CADASTRO RECEITA",
    "NÚMERO DO PROCESSO",
    "CATEGORIA DA SANÇÃO",
    "DATA INÍCIO SANÇÃO",
    "DATA FINAL SANÇÃO",
    "DATA PUBLICAÇÃO",
    "PUBLICAÇÃO",
    "DETALHAMENTO DO MEIO DE PUBLICAÇÃO",
    "DATA DO TRÂNSITO EM JULGADO",
    "ABRAGÊNCIA DA SANÇÃO",
    "ÓRGÃO SANCIONADOR",
    "UF ÓRGÃO SANCIONADOR",
    "ESFERA ÓRGÃO SANCIONADOR",
    "FUNDAMENTAÇÃO LEGAL",
    "DATA ORIGEM INFORMAÇÃO",
    "ORIGEM INFORMAÇÕES",
    "OBSERVAÇÕES",
]

_CNEP_EXPECTED_HEADER: list[str] = [
    "CADASTRO",
    "CÓDIGO DA SANÇÃO",
    "TIPO DE PESSOA",
    "CPF OU CNPJ DO SANCIONADO",
    "NOME DO SANCIONADO",
    "NOME INFORMADO PELO ÓRGÃO SANCIONADOR",
    "RAZÃO SOCIAL - CADASTRO RECEITA",
    "NOME FANTASIA - CADASTRO RECEITA",
    "NÚMERO DO PROCESSO",
    "CATEGORIA DA SANÇÃO",
    "VALOR DA MULTA",
    "DATA INÍCIO SANÇÃO",
    "DATA FINAL SANÇÃO",
    "DATA PUBLICAÇÃO",
    "PUBLICAÇÃO",
    "DETALHAMENTO DO MEIO DE PUBLICAÇÃO",
    "DATA DO TRÂNSITO EM JULGADO",
    "ABRAGÊNCIA DA SANÇÃO",
    "ÓRGÃO SANCIONADOR",
    "UF ÓRGÃO SANCIONADOR",
    "ESFERA ÓRGÃO SANCIONADOR",
    "FUNDAMENTAÇÃO LEGAL",
    "DATA ORIGEM INFORMAÇÃO",
    "ORIGEM INFORMAÇÕES",
    "OBSERVAÇÕES",
]

_LENIENCIA_EXPECTED_HEADER: list[str] = [
    "ID DO ACORDO",
    "CNPJ DO SANCIONADO",
    "RAZÃO SOCIAL  CADASTRO RECEITA",
    "NOME FANTASIA  CADASTRO RECEITA",
    "DATA DE INÍCIO DO ACORDO",
    "DATA DE FIM DO ACORDO",
    "SITUAÇÃO DO ACORDO DE LENIÊNCIA",
    "DATA DA INFORMAÇÃO",
    "NÚMERO DO PROCESSO",
    "TERMOS DO ACORDO",
    "ÓRGÃO SANCIONADOR",
]

# Known historical typos/variations — short, versioned alias table.
# Keys and values are pre-normalized (NFKD) so they match against normalize_header_value() output.
_HEADER_ALIASES: dict[str, str] = {
    normalize_header_value(k): normalize_header_value(v)
    for k, v in {
        "leniênica": "leniência",  # typo observed in real leniência CSVs
        "razão social — cadastro receita": "razão social  cadastro receita",  # em-dash variant
        "razão social - cadastro receita": "razão social  cadastro receita",  # hyphen variant
        "nome fantasia — cadastro receita": "nome fantasia  cadastro receita",
        "nome fantasia - cadastro receita": "nome fantasia  cadastro receita",
        "razão social \x96 cadastro receita": "razão social  cadastro receita",  # mojibake en-dash (0x96)
        "nome fantasia \x96 cadastro receita": "nome fantasia  cadastro receita",
    }.items()
}


def _apply_aliases(value: str) -> str:
    """Apply known historical aliases to a normalized header value."""
    for pattern, replacement in _HEADER_ALIASES.items():
        value = value.replace(pattern, replacement)
    return value


def _validate_header(header: list[str], expected: list[str], source: str) -> dict[str, int]:
    """Validate CSV header against expected layout, return name→index column map.

    Raises ValueError on mismatch with a descriptive message.
    """
    if len(header) != len(expected):
        raise ValueError(
            f"CGU {source}: header length mismatch: got {len(header)} columns, "
            f"expected {len(expected)}. First 5 got: {header[:5]}"
        )
    norm_got = [_apply_aliases(normalize_header_value(h)) for h in header]
    norm_exp = [_apply_aliases(normalize_header_value(e)) for e in expected]
    mismatches: list[str] = []
    for i, (got, exp) in enumerate(zip(norm_got, norm_exp)):
        if got != exp:
            mismatches.append(f"  col {i}: got {header[i]!r}, expected {expected[i]!r}")
    if mismatches:
        detail = "\n".join(mismatches)
        raise ValueError(f"CGU {source}: column mismatch:\n{detail}")
    # Build name→index map from normalized expected names
    return {normalize_header_value(expected[i]): i for i in range(len(expected))}


def _parse_csv_with_header(text: str) -> tuple[list[str], list[list[str]]]:
    """Parse semicolon-separated CSV with quoted fields. Return (header, data_rows)."""
    import csv

    reader = csv.reader(io.StringIO(text), delimiter=";", quotechar='"')
    rows = list(reader)
    if not rows:
        return [], []
    # Strip quotes from header values (csv.reader already handles this)
    header = [h.strip() for h in rows[0]]
    return header, rows[1:]


def _parse_csv_rows(text: str) -> list[list[str]]:
    """Parse semicolon-separated CSV with quoted fields. Skip header."""
    _, rows = _parse_csv_with_header(text)
    return rows


def _safe_str(row: list[str], idx: int) -> str:
    if idx < len(row):
        val = row[idx].strip()
        return "" if val in ("", "nan") else val
    return ""


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
    headers = {"User-Agent": _CGU_UA, "Range": "bytes=0-0"}
    with httpx.Client(follow_redirects=True, timeout=15, headers=headers) as client:
        for offset in range(0, max_lookback + 1):
            d = date.today() - timedelta(days=offset)
            date_str = d.strftime("%Y%m%d")
            try:
                r = client.get(f"{CGU_DOWNLOAD_URL}/ceis/{date_str}")
                if r.status_code in (200, 206):
                    return date_str
            except httpx.RequestError:
                continue
    return None


def _head_content_length(url: str) -> int | None:
    """Probe remote file size via Range request (HEAD blocked by WAF)."""
    try:
        r = httpx.get(
            url,
            follow_redirects=True,
            timeout=15,
            headers={"User-Agent": _CGU_UA, "Range": "bytes=0-0"},
        )
        if r.status_code == 206:
            cr = r.headers.get("content-range", "")
            if "/" in cr:
                return int(cr.rsplit("/", 1)[1])
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
) -> Path | None:
    """Download a ZIP from Portal da Transparencia and extract the CSV."""
    url = f"{CGU_DOWNLOAD_URL}/{dataset}/{date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{dataset}.csv"

    remote_cl = _head_content_length(url)
    logger.info("Downloading %s from %s", dataset.upper(), url)
    zip_path = output_dir / f"{dataset}.zip"
    try:
        total = 0
        with httpx.stream(
            "GET",
            url,
            follow_redirects=True,
            timeout=120,
            headers={
                "User-Agent": _CGU_UA,
                "Accept": "application/zip, application/octet-stream;q=0.9, */*;q=0.1",
            },
        ) as response:
            response.raise_for_status()
            with zip_path.open("wb") as fh:
                for chunk in response.iter_bytes(chunk_size=_CGU_DOWNLOAD_CHUNK_SIZE):
                    total += len(chunk)
                    if total > _CGU_MAX_DOWNLOAD_BYTES:
                        raise ValueError(f"download exceeded max bytes ({total} > {_CGU_MAX_DOWNLOAD_BYTES})")
                    fh.write(chunk)
    except (ValueError, httpx.HTTPStatusError) as exc:
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

    # --- Capture provenance manifest ---
    try:
        manifest = capture_csv_manifest(
            csv_path,
            source=f"cgu_{dataset}",
            year_or_cycle=date_str,
            origin_url=url,
            content_length=remote_cl or 0,
            encoding="utf-8",
        )
        write_manifest(manifest, output_dir)
    except Exception:
        logger.warning("Failed to capture manifest for %s — continuing", dataset)

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
    expected = _CEIS_EXPECTED_HEADER if source == "ceis" else _CNEP_EXPECTED_HEADER
    text = _read_csv_text(csv_path)
    header, rows = _parse_csv_with_header(text)
    if header:
        _validate_header(header, expected, source)
    records: list[dict[str, Any]] = []
    for row in rows:
        rec = _normalize_csv_record(row, source, col_map)
        if rec["entity_name"]:
            records.append(rec)
    return records


def _load_leniencia_csv(csv_path: Path) -> list[dict[str, Any]]:
    """Read a leniência CSV and return normalized sanction records."""
    text = _read_csv_text(csv_path)
    header, rows = _parse_csv_with_header(text)
    if header:
        _validate_header(header, _LENIENCIA_EXPECTED_HEADER, "leniencia")
    records: list[dict[str, Any]] = []
    for row in rows:
        rec = _normalize_leniencia_record(row)
        if rec["entity_name"]:
            records.append(rec)
    return records
