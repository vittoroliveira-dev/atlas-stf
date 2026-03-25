"""Remote probing — HEAD/Range requests to capture current remote state."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx

from ._manifest_model import FetchUnit, RefreshPolicy, RemoteState

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 30


def probe_remote_state(
    source: str,
    unit: FetchUnit,
    policy: RefreshPolicy,
    *,
    timeout: int = _DEFAULT_TIMEOUT,
) -> RemoteState:
    """Probe remote artifact and return a fresh ``RemoteState``.

    Strategy per source family:

    - **TSE / CVM**: ``HEAD`` → etag + content-length
    - **CGU**: ``HEAD`` with Range header → content-length + date header
    - **RFB**: ``HEAD`` (WebDAV-compatible) → content-length
    - **DataJud**: no probe (API-only) — returns empty state
    """
    now = datetime.now(UTC).isoformat()

    if not policy.force_refresh_supported and source == "datajud":
        return RemoteState(url=unit.remote_url, probed_at=now)

    url = unit.remote_url
    if not url:
        return RemoteState(url="", probed_at=now)

    try:
        if "content_length_date" in policy.comparators:
            return _probe_range(url, timeout=timeout, probed_at=now)
        return _probe_head(url, timeout=timeout, probed_at=now)
    except httpx.RequestError as exc:
        logger.warning("Probe failed for %s (%s): %s", unit.unit_id, url, exc)
        return RemoteState(url=url, probed_at=now)


def _probe_head(url: str, *, timeout: int, probed_at: str) -> RemoteState:
    r = httpx.head(url, timeout=timeout, follow_redirects=True)
    r.raise_for_status()
    return RemoteState(
        url=url,
        etag=r.headers.get("etag", ""),
        content_length=int(r.headers.get("content-length", "0")),
        last_modified=r.headers.get("last-modified", ""),
        probed_at=probed_at,
    )


def _probe_range(url: str, *, timeout: int, probed_at: str) -> RemoteState:
    """Probe via Range header (CGU-style: server returns Content-Range with full size)."""
    headers = {"Range": "bytes=0-0"}
    r = httpx.get(url, headers=headers, timeout=timeout, follow_redirects=True)
    content_length = 0
    if r.status_code == 206:
        cr = r.headers.get("content-range", "")
        if "/" in cr:
            try:
                content_length = int(cr.rsplit("/", 1)[1])
            except ValueError:
                pass
    elif r.status_code == 200:
        content_length = int(r.headers.get("content-length", "0"))
    return RemoteState(
        url=url,
        content_length=content_length,
        last_modified=r.headers.get("last-modified", ""),
        probed_at=probed_at,
    )
