"""RFB fetch runner: downloads Socios/Empresas/Estabelecimentos ZIPs from RFB open data."""

from __future__ import annotations

import json
import logging
import zipfile
from collections.abc import Callable
from io import TextIOWrapper
from pathlib import Path
from typing import Any

import httpx

from ..core.http_stream_safety import write_limited_stream_to_file
from ..core.identity import normalize_entity_name
from ..core.zip_safety import enforce_max_uncompressed_size, is_safe_zip_member
from ._config import (
    RFB_EMPRESAS_FILE_COUNT,
    RFB_ESTABELECIMENTOS_FILE_COUNT,
    RFB_LEGACY_BASE_URL,
    RFB_NEXTCLOUD_BASE,
    RFB_NEXTCLOUD_SHARE_TOKEN,
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

logger = logging.getLogger(__name__)

_RFB_MAX_ZIP_UNCOMPRESSED_BYTES = 16 * 1024 * 1024 * 1024
_RFB_MAX_DOWNLOAD_BYTES = 16 * 1024 * 1024 * 1024


_active_share_token: list[str] = [RFB_NEXTCLOUD_SHARE_TOKEN]


def _nextcloud_auth() -> tuple[str, str] | None:
    """Return NextCloud auth tuple when a token is configured."""
    if not _active_share_token[0]:
        return None
    return (_active_share_token[0], "")


def _discover_share_token(timeout: int = 15) -> str | None:
    """Auto-discover the NextCloud share token from the RFB portal page."""
    import re

    try:
        r = httpx.get(RFB_NEXTCLOUD_BASE, follow_redirects=True, timeout=timeout)
        r.raise_for_status()
        match = re.search(r"/index\.php/s/([A-Za-z0-9]{10,})", r.text)
        if match:
            return match.group(1)
    except (httpx.RequestError, httpx.HTTPStatusError) as exc:
        logger.debug("Failed to discover share token: %s", exc)
    return None


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


def _discover_latest_month(timeout: int) -> str | None:
    """Discover the latest available month folder via NextCloud WebDAV PROPFIND."""
    import re

    import defusedxml.ElementTree as ET

    auth = _nextcloud_auth()
    if auth is None:
        logger.info("NextCloud token not configured — skipping WebDAV discovery")
        return None

    url = f"{RFB_WEBDAV_BASE}/"
    logger.info("Discovering latest RFB month via WebDAV PROPFIND...")
    try:
        with httpx.Client(follow_redirects=True, timeout=timeout) as client:
            r = client.request(
                "PROPFIND",
                url,
                headers={"Depth": "1"},
                auth=auth,
            )
            r.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (401, 403):
            logger.warning("WebDAV auth failed (token may have changed) — attempting auto-discovery")
            new_token = _discover_share_token(timeout)
            if new_token and new_token != _active_share_token[0]:
                _active_share_token[0] = new_token
                masked = new_token[:4] + "..." + new_token[-4:] if len(new_token) > 8 else "****"
                logger.info("Discovered new RFB share token: %s", masked)
                logger.info("Persist the token in ATLAS_STF_RFB_NEXTCLOUD_SHARE_TOKEN")
                return _discover_latest_month(timeout)
        logger.warning("WebDAV PROPFIND failed: %s", exc)
        return None
    except httpx.RequestError as exc:
        logger.warning("WebDAV PROPFIND failed: %s", exc)
        return None

    try:
        root = ET.fromstring(r.text)
        ns = {"d": "DAV:"}
        months: list[str] = []
        for resp in root.findall(".//d:response", ns):
            href_el = resp.find("d:href", ns)
            if href_el is None or href_el.text is None:
                continue
            match = re.search(r"(\d{4}-\d{2})/?$", href_el.text)
            if match:
                months.append(match.group(1))
        if months:
            latest = sorted(months)[-1]
            logger.info("Latest RFB month: %s", latest)
            return latest
    except ET.ParseError as exc:
        logger.warning("Failed to parse WebDAV XML: %s", exc)
    return None


def _download_zip(url: str, destination: Path, timeout: int) -> Path | None:
    """Stream a ZIP file to disk to avoid buffering the full archive in memory."""
    logger.info("Downloading %s", url)
    auth = _nextcloud_auth() if url.startswith(RFB_WEBDAV_BASE) else None
    timeouts = httpx.Timeout(float(timeout), connect=10.0)
    try:
        with httpx.stream("GET", url, timeout=timeouts, follow_redirects=True, auth=auth) as response:
            response.raise_for_status()
            write_limited_stream_to_file(
                response,
                destination,
                max_download_bytes=_RFB_MAX_DOWNLOAD_BYTES,
            )
        return destination
    except (ValueError, httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to download %s: %s", url, exc)
        destination.unlink(missing_ok=True)
        return None


def _extract_csv_from_zip(zip_path: Path) -> bytes | None:
    """Extract the first CSV file from a ZIP archive."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            csv_infos = [
                info
                for info in zf.infolist()
                if (info.filename.lower().endswith(".csv") or "csv" in info.filename.lower())
                and is_safe_zip_member(info.filename, zip_path.parent)
            ]
            if not csv_infos:
                logger.warning("No CSV found in ZIP")
                return None
            csv_info = csv_infos[0]
            enforce_max_uncompressed_size(
                [csv_info],
                max_total_uncompressed_bytes=_RFB_MAX_ZIP_UNCOMPRESSED_BYTES,
            )
            return zf.read(csv_info)
    except zipfile.BadZipFile:
        logger.warning("Invalid ZIP file")
        return None
    except ValueError as exc:
        logger.warning("Refusing RFB ZIP: %s", exc)
        return None


def _parse_csv_from_zip_text(
    zip_path: Path,
    parser: Callable[[TextIOWrapper], Any],
) -> Any | None:
    """Open the first safe CSV member as text and parse it without buffering bytes+text."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            csv_infos = [
                info
                for info in zf.infolist()
                if (info.filename.lower().endswith(".csv") or "csv" in info.filename.lower())
                and is_safe_zip_member(info.filename, zip_path.parent)
            ]
            if not csv_infos:
                logger.warning("No CSV found in ZIP")
                return None

            csv_info = csv_infos[0]
            enforce_max_uncompressed_size(
                [csv_info],
                max_total_uncompressed_bytes=_RFB_MAX_ZIP_UNCOMPRESSED_BYTES,
            )

            for encoding in ("utf-8", "iso-8859-1"):
                try:
                    with zf.open(csv_info) as raw_fh:
                        with TextIOWrapper(raw_fh, encoding=encoding, newline="") as text_fh:
                            return parser(text_fh)
                except UnicodeDecodeError:
                    if encoding == "utf-8":
                        continue
                    raise
    except zipfile.BadZipFile:
        logger.warning("Invalid ZIP file")
        return None
    except ValueError as exc:
        logger.warning("Refusing RFB ZIP: %s", exc)
        return None


def fetch_rfb_data(
    config: RfbFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Download + parse Socios, Empresas, and Estabelecimentos ZIPs from RFB."""
    config.output_dir.mkdir(parents=True, exist_ok=True)

    target_names = _build_target_names(config)
    logger.info("Target names for RFB matching: %d", len(target_names))

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

    # Pass 1: Socios by name
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
