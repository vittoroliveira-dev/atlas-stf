"""RFB HTTP/download helpers: NextCloud discovery, ZIP streaming, CSV extraction."""

from __future__ import annotations

import logging
import zipfile
from collections.abc import Callable, Iterable
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Protocol

import httpx

from ..core.http_stream_safety import write_stream_resilient
from ..core.zip_safety import enforce_max_uncompressed_size, is_safe_zip_member
from ..ingest_manifest import capture_csv_manifest_from_stream, write_manifest
from ._config import (
    RFB_NEXTCLOUD_BASE,
    RFB_NEXTCLOUD_SHARE_TOKEN,
    RFB_WEBDAV_BASE,
)

logger = logging.getLogger(__name__)


class TextLineStream(Protocol):
    """Minimal contract for CSV-style text stream parsers: iterable of lines."""

    def __iter__(self) -> Iterable[str]: ...
    def readline(self) -> str: ...

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


_MAX_RESUME_RETRIES = 3
_STALL_TIMEOUT = 300.0


def _log_attempt(
    filename: str,
    attempt: int,
    outcome: str,
    bytes_total: int,
    resume_offset: int,
    http_status: int,
    etag: str,
    *,
    error: str = "",
) -> None:
    """Emit structured JSON log for each download attempt (observability)."""
    import json

    entry = {
        "event": "DOWNLOAD_ATTEMPT",
        "file": filename,
        "attempt": attempt,
        "outcome": outcome,
        "bytes_total": bytes_total,
        "resume_offset": resume_offset,
        "http_status": http_status,
        "etag": etag,
    }
    if error:
        entry["error"] = error[:256]
    logger.info("DOWNLOAD_ATTEMPT %s", json.dumps(entry, ensure_ascii=False))


def _download_zip(url: str, destination: Path, timeout: int) -> Path | None:
    """Stream a ZIP file to disk with .part safety, stall detection, and HTTP Range resume.

    On stall or network interruption, retries up to ``_MAX_RESUME_RETRIES`` times
    using HTTP Range to resume from the last byte written.  If the server does not
    support Range (returns 200 instead of 206), falls back to full re-download.
    """
    from ..core.http_stream_safety import get_part_resume_offset

    auth = _nextcloud_auth() if url.startswith(RFB_WEBDAV_BASE) else None
    timeouts = httpx.Timeout(connect=10.0, read=900.0, write=30.0, pool=30.0)

    # Probe remote metadata once for resume validation
    remote_etag = ""
    remote_cl = 0
    try:
        head = httpx.head(url, timeout=httpx.Timeout(10.0), follow_redirects=True, auth=auth)
        if head.status_code == 200:
            remote_etag = head.headers.get("etag", "")
            remote_cl = int(head.headers.get("content-length", "0"))
    except httpx.RequestError:
        pass

    for attempt in range(_MAX_RESUME_RETRIES + 1):
        resume_offset = get_part_resume_offset(
            destination, expected_etag=remote_etag, expected_content_length=remote_cl
        )
        headers: dict[str, str] = {}
        if resume_offset > 0:
            headers["Range"] = f"bytes={resume_offset}-"
            logger.info("Resuming %s from byte %d (attempt %d)", destination.name, resume_offset, attempt + 1)
        else:
            logger.info("Downloading %s (attempt %d)", url, attempt + 1)

        try:
            with httpx.stream(
                "GET", url, timeout=timeouts, follow_redirects=True, auth=auth, headers=headers
            ) as response:
                if response.status_code == 416:
                    # 416 Range Not Satisfiable — partial is stale or complete
                    logger.warning("416 Range Not Satisfiable — discarding partial and restarting")
                    part = destination.with_suffix(destination.suffix + ".part")
                    part.unlink(missing_ok=True)
                    part.with_suffix(".progress").unlink(missing_ok=True)
                    if attempt < _MAX_RESUME_RETRIES:
                        continue
                    return None
                response.raise_for_status()

                # Validate response semantics for Range requests
                actual_offset = resume_offset
                if resume_offset > 0:
                    if response.status_code == 200:
                        logger.info("Server ignored Range (200) — restarting full download")
                        actual_offset = 0
                    elif response.status_code == 206:
                        # Validate Content-Range header
                        cr = response.headers.get("content-range", "")
                        if cr and not cr.startswith(f"bytes {resume_offset}-"):
                            logger.warning(
                                "Content-Range mismatch: expected start=%d, got %r — restarting",
                                resume_offset,
                                cr,
                            )
                            actual_offset = 0
                        # Cross-check ETag from response vs HEAD
                        resp_etag = response.headers.get("etag", "")
                        if remote_etag and resp_etag and resp_etag != remote_etag:
                            logger.warning(
                                "ETag changed between HEAD and GET (%s → %s) — restarting",
                                remote_etag,
                                resp_etag,
                            )
                            actual_offset = 0

                actual = write_stream_resilient(
                    response,
                    destination,
                    max_download_bytes=_RFB_MAX_DOWNLOAD_BYTES,
                    stall_timeout_seconds=_STALL_TIMEOUT,
                    resume_offset=actual_offset,
                    remote_etag=remote_etag,
                    remote_content_length=remote_cl,
                )
            _log_attempt(
                destination.name, attempt + 1, "success", actual, resume_offset, response.status_code, remote_etag
            )
            if not _verify_zip_integrity(destination):
                _log_attempt(
                    destination.name,
                    attempt + 1,
                    "integrity_fail",
                    actual,
                    resume_offset,
                    response.status_code,
                    remote_etag,
                )
                destination.unlink(missing_ok=True)
                if attempt < _MAX_RESUME_RETRIES:
                    continue
                return None
            return destination
        except TimeoutError as exc:
            _log_attempt(
                destination.name, attempt + 1, "stall", resume_offset, resume_offset, 0, remote_etag, error=str(exc)
            )
            if attempt < _MAX_RESUME_RETRIES:
                continue
            logger.error("Exhausted %d resume retries for %s", _MAX_RESUME_RETRIES, url)
            return None
        except httpx.RequestError as exc:
            part_path = destination.with_suffix(destination.suffix + ".part")
            partial_bytes = part_path.stat().st_size if part_path.exists() else 0
            _log_attempt(
                destination.name,
                attempt + 1,
                "transport_error",
                partial_bytes,
                resume_offset,
                0,
                remote_etag,
                error=str(exc),
            )
            if partial_bytes > 0 and attempt < _MAX_RESUME_RETRIES:
                continue
            if partial_bytes == 0:
                destination.unlink(missing_ok=True)
            return None
        except (ValueError, httpx.HTTPStatusError) as exc:
            _log_attempt(destination.name, attempt + 1, "fatal", 0, resume_offset, 0, remote_etag, error=str(exc))
            destination.unlink(missing_ok=True)
            return None

    return None


def _verify_zip_integrity(zip_path: Path) -> bool:
    """Quick integrity check: verify the ZIP central directory is readable.

    Does NOT decompress data (too expensive for multi-GB ZIPs).  Only verifies
    that the file can be opened as a ZIP and lists at least one member.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            members = zf.infolist()
            if not members:
                return False
            # Sanity: first member should have positive uncompressed size
            return members[0].file_size > 0
    except zipfile.BadZipFile, OSError, ValueError:
        return False


def _extract_csv_from_zip(zip_path: Path) -> bytes | None:
    """Extract the first CSV file from a ZIP archive."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            csv_infos = [
                info
                for info in zf.infolist()
                if _is_rfb_data_member(info.filename)
                and is_safe_zip_member(info.filename, zip_path.parent, external_attr=info.external_attr)
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


_MANIFEST_SAMPLE_LINES = 110  # reads more than the 10-row sample + 1 header row


class _ManifestCapturingStream:
    """Transparent wrapper that captures the first N lines for provenance manifest.

    The parser receives this as a regular text stream (iterable of lines).
    Internally, the first ``max_lines`` are stored in ``captured_lines``
    for later manifest generation — everything else streams through
    without buffering.
    """

    def __init__(self, inner: TextIOWrapper, max_lines: int) -> None:
        self._inner = inner
        self._max_lines = max_lines
        self.captured_lines: list[str] = []
        self._count = 0

    def __iter__(self) -> _ManifestCapturingStream:
        return self

    def __next__(self) -> str:
        line = next(self._inner)
        if self._count < self._max_lines:
            self.captured_lines.append(line.rstrip("\n"))
            self._count += 1
        return line

    def readline(self) -> str:
        line = self._inner.readline()
        if line and self._count < self._max_lines:
            self.captured_lines.append(line.rstrip("\n"))
            self._count += 1
        return line

    def read(self, size: int = -1) -> str:
        return self._inner.read(size)


def _parse_csv_from_zip_text(
    zip_path: Path,
    parser: Callable[[TextLineStream], Any],
    manifest_dir: Path | None = None,
) -> Any | None:
    """Open the first safe CSV member as text and parse it without buffering bytes+text.

    When ``manifest_dir`` is given, captures a provenance manifest from the first
    ``_MANIFEST_SAMPLE_LINES`` lines before calling the parser. RFB CSVs are
    positional (no header row), so the first data line is recorded as the layout
    fingerprint. The manifest is written to ``manifest_dir/_source_manifests/``.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            csv_infos = [
                info
                for info in zf.infolist()
                if _is_rfb_data_member(info.filename)
                and is_safe_zip_member(info.filename, zip_path.parent, external_attr=info.external_attr)
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
                            # Single-pass: wrap the stream to capture first N lines
                            # for manifesto while the parser consumes it.
                            if manifest_dir is not None:
                                wrapper = _ManifestCapturingStream(text_fh, _MANIFEST_SAMPLE_LINES)
                                result = parser(wrapper)
                                _write_manifest_safe(
                                    sample_lines=wrapper.captured_lines,
                                    zip_name=zip_path.name,
                                    csv_name=csv_info.filename,
                                    encoding=encoding,
                                    manifest_dir=manifest_dir,
                                )
                                return result
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


def _write_manifest_safe(
    *,
    sample_lines: list[str],
    zip_name: str,
    csv_name: str,
    encoding: str,
    manifest_dir: Path,
) -> None:
    """Capture and write a provenance manifest; logs a warning on failure."""
    try:
        # Derive a stable file_name from the ZIP + CSV member names.
        file_name = f"{zip_name}:{csv_name}"
        # year_or_cycle is not available at this level; use the zip stem as a
        # reasonable proxy (e.g. "Socios0" from "Socios0.zip").
        year_or_cycle = Path(zip_name).stem
        manifest = capture_csv_manifest_from_stream(
            sample_lines,
            source="rfb",
            file_name=file_name,
            year_or_cycle=year_or_cycle,
            encoding=encoding,
            delimiter=";",
        )
        write_manifest(manifest, manifest_dir / "_source_manifests")
    except Exception:
        logger.warning("Failed to capture manifest for %s — continuing", zip_name)
