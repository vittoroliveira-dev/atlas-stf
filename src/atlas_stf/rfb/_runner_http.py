"""RFB HTTP/download helpers: NextCloud discovery, ZIP streaming, CSV extraction."""

from __future__ import annotations

import logging
import zipfile
from collections.abc import Callable
from io import TextIOWrapper
from pathlib import Path
from typing import Any

import httpx

from ..core.http_stream_safety import write_limited_stream_to_file
from ..core.zip_safety import enforce_max_uncompressed_size, is_safe_zip_member
from ._config import (
    RFB_NEXTCLOUD_BASE,
    RFB_NEXTCLOUD_SHARE_TOKEN,
    RFB_WEBDAV_BASE,
)

logger = logging.getLogger(__name__)

_RFB_MAX_ZIP_UNCOMPRESSED_BYTES = 16 * 1024 * 1024 * 1024
_RFB_MAX_DOWNLOAD_BYTES = 16 * 1024 * 1024 * 1024

_RFB_DATA_SUFFIXES = (".csv",)
_RFB_DATA_SUBSTRINGS = ("csv", "estabele")

_active_share_token: list[str] = [RFB_NEXTCLOUD_SHARE_TOKEN]


def _is_rfb_data_member(filename: str) -> bool:
    """Check if ZIP member looks like an RFB data file (CSV or mainframe-style)."""
    lower = filename.lower()
    return any(lower.endswith(s) for s in _RFB_DATA_SUFFIXES) or any(sub in lower for sub in _RFB_DATA_SUBSTRINGS)


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
                if _is_rfb_data_member(info.filename) and is_safe_zip_member(info.filename, zip_path.parent)
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
                if _is_rfb_data_member(info.filename) and is_safe_zip_member(info.filename, zip_path.parent)
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
