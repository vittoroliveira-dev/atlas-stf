"""HTTP client for the STF ministerial agenda GraphQL API."""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from ._config import GRAPHQL_BASE_URL, USER_AGENT, AgendaFetchConfig

logger = logging.getLogger(__name__)

_GRAPHQL_QUERY_TPL = (
    "{{agendaMinistrosPorDiaCategoria(ano:{year},mes:{month},first:300)"
    "{{data descricaoData ministro{{nomeMinistro evento{{titulo descricao horaInicio}}}}}}}}"
)

_CONTRACT_KEY = "agendaMinistrosPorDiaCategoria"
_HEADER_KEYS = ("content-type", "date", "server", "x-request-id", "cache-control")


def _sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class AgendaClient:
    """GraphQL client for the STF ministerial agenda API."""

    def __init__(self, config: AgendaFetchConfig) -> None:
        self._config = config
        self._client: httpx.Client | None = None
        self._last_request_time: float = 0

    def __enter__(self) -> AgendaClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=self._config.timeout_seconds,
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
        return self._client

    def _rate_limit_wait(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._config.rate_limit_seconds:
            time.sleep(self._config.rate_limit_seconds - elapsed)
        self._last_request_time = time.monotonic()

    def fetch_month(self, year: int, month: int) -> tuple[dict[str, Any], dict[str, Any]]:
        """Fetch agenda data for a single month.

        Returns ``(raw_response_dict, fetch_metadata_dict)``.
        Tries GET first; falls back to POST on 403.
        """
        query = _GRAPHQL_QUERY_TPL.format(year=year, month=month)
        query_hash = _sha256(query)
        url = GRAPHQL_BASE_URL + "graphql"

        meta: dict[str, Any] = {
            "year": year,
            "month": month,
            "query_hash": query_hash,
            "user_agent": USER_AGENT,
            "fetched_at": "",
            "fetch_method": "",
            "http_status": 0,
            "response_sha256": "",
            "response_size_bytes": 0,
            "response_headers_subset": {},
            "contract_version_detected": False,
        }

        for attempt in range(self._config.max_retries):
            self._rate_limit_wait()
            try:
                raw_data, meta = self._try_fetch(url, query, query_hash, meta)
                return raw_data, meta
            except httpx.HTTPError as exc:
                if attempt < self._config.max_retries - 1:
                    wait = self._config.retry_delay_seconds * (2**attempt)
                    logger.warning(
                        "Agenda fetch %04d-%02d attempt %d/%d failed: %s — retry in %.1fs",
                        year,
                        month,
                        attempt + 1,
                        self._config.max_retries,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        "Agenda fetch %04d-%02d failed after %d attempts: %s",
                        year,
                        month,
                        self._config.max_retries,
                        exc,
                    )
                    raise

        return {}, meta  # unreachable

    def _try_fetch(
        self,
        url: str,
        query: str,
        query_hash: str,
        meta: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        client = self._get_client()
        resp = client.get(url, params={"query": query})

        if resp.status_code == 403:
            logger.debug("GET returned 403 — falling back to POST")
            self._rate_limit_wait()
            resp = client.post(
                url,
                json={"query": query},
                headers={"Content-Type": "application/json"},
            )
            meta["fetch_method"] = "POST"
        else:
            meta["fetch_method"] = "GET"

        resp.raise_for_status()

        body_text = resp.text
        raw_data: dict[str, Any] = resp.json()

        meta["fetched_at"] = datetime.now(timezone.utc).isoformat()
        meta["http_status"] = resp.status_code
        meta["response_sha256"] = _sha256(body_text)
        meta["response_size_bytes"] = len(body_text.encode("utf-8"))
        meta["response_headers_subset"] = {k: resp.headers.get(k, "") for k in _HEADER_KEYS if resp.headers.get(k)}
        meta["query_hash"] = query_hash

        data_payload = raw_data.get("data", {})
        meta["contract_version_detected"] = _CONTRACT_KEY in data_payload

        if not meta["contract_version_detected"]:
            logger.warning("Contract mismatch: missing 'data.%s'", _CONTRACT_KEY)

        return raw_data, meta
