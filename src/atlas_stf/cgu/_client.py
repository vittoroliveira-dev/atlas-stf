"""HTTP client for CGU Portal da Transparencia CEIS/CNEP API."""

from __future__ import annotations

import logging
import time
from types import TracebackType

import httpx

from ._config import CGU_BASE_URL

logger = logging.getLogger(__name__)


class CguClient:
    """Context-manager wrapper around httpx for CGU API calls."""

    def __init__(
        self,
        api_key: str,
        *,
        timeout: int = 30,
        rate_limit: float = 0.7,
        max_retries: int = 3,
    ) -> None:
        self._api_key = api_key
        self._timeout = timeout
        self._rate_limit = rate_limit
        self._max_retries = max_retries
        self._last_request_time: float = 0.0
        self._client: httpx.Client | None = None

    def __enter__(self) -> CguClient:
        self._client = httpx.Client(
            base_url=CGU_BASE_URL,
            headers={
                "chave-api-dados": self._api_key,
                "Accept": "application/json",
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

    def _get(self, path: str, params: dict) -> list[dict]:
        """GET request with retry and rate limiting. Returns parsed JSON list."""
        if self._client is None:
            raise RuntimeError("CguClient must be used as a context manager")

        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            self._enforce_rate_limit()
            try:
                response = self._client.get(path, params=params)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, list):
                    return data
                return []
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {429, 500, 502, 503, 504}:
                    last_error = exc
                    wait = 2**attempt
                    logger.warning(
                        "CGU request failed (attempt %d/%d): %s — retrying in %ds",
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
                    "CGU request failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1,
                    self._max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)

        raise RuntimeError(f"CGU request failed after {self._max_retries} attempts") from last_error

    def search_ceis(self, params: dict) -> list[dict]:
        """Search CEIS (Cadastro de Empresas Inidoneas e Suspensas)."""
        return self._get("/ceis", params)

    def search_cnep(self, params: dict) -> list[dict]:
        """Search CNEP (Cadastro Nacional de Empresas Punidas)."""
        return self._get("/cnep", params)

    def search_leniencia(self, params: dict) -> list[dict]:
        """Search Acordos de Leniência."""
        return self._get("/acordos-leniencia", params)
