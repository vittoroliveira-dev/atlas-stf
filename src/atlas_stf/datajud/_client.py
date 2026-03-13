"""HTTP client for DataJud Elasticsearch API."""

from __future__ import annotations

import logging
import re
import time
from types import TracebackType

import httpx

from ._config import DATAJUD_BASE_URL

logger = logging.getLogger(__name__)
_INDEX_PATTERN = re.compile(r"^api_publica_[a-z0-9_]+$")


class DatajudClient:
    """Context-manager wrapper around httpx for DataJud API calls."""

    def __init__(
        self,
        api_key: str,
        *,
        timeout: int = 30,
        rate_limit: float = 1.0,
        max_retries: int = 3,
    ) -> None:
        self._api_key = api_key
        self._timeout = timeout
        self._rate_limit = rate_limit
        self._max_retries = max_retries
        self._last_request_time: float = 0.0
        self._client: httpx.Client | None = None

    def __enter__(self) -> DatajudClient:
        self._client = httpx.Client(
            base_url=DATAJUD_BASE_URL,
            headers={
                "Authorization": f"APIKey {self._api_key}",
                "Content-Type": "application/json",
            },
            timeout=self._timeout,
            follow_redirects=True,
        )
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _enforce_rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._rate_limit:
            time.sleep(self._rate_limit - elapsed)
        self._last_request_time = time.monotonic()

    def search(self, index: str, body: dict) -> dict:
        """POST /{index}/_search with retry and rate limiting."""
        if self._client is None:
            raise RuntimeError("DatajudClient must be used as a context manager")
        if not _INDEX_PATTERN.fullmatch(index):
            raise ValueError(f"Invalid DataJud index: {index!r}")

        url = f"/{index}/_search"
        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            self._enforce_rate_limit()
            try:
                response = self._client.post(url, json=body)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {429, 500, 502, 503, 504}:
                    last_error = exc
                    wait = 2**attempt
                    logger.warning(
                        "DataJud request failed (attempt %d/%d): %s — retrying in %ds",
                        attempt + 1,
                        self._max_retries,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    raise
            except httpx.RequestError as exc:
                last_error = exc
                wait = 2**attempt
                logger.warning(
                    "DataJud request failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1,
                    self._max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)

        raise RuntimeError(f"DataJud request failed after {self._max_retries} attempts") from last_error
