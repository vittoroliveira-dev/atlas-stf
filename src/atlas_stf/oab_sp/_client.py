"""HTTP client for OAB/SP society registry."""

from __future__ import annotations

import logging
import time
from types import TracebackType
from typing import Any

import httpx

from ._config import OABSP_DETAIL_URL, OABSP_INSCRITOS_URL, OABSP_SEARCH_URL

logger = logging.getLogger(__name__)


class OabSpClient:
    """Context-manager wrapper around httpx for OAB/SP lookups."""

    def __init__(
        self,
        *,
        timeout: int = 30,
        rate_limit: float = 1.5,
        max_retries: int = 3,
        retry_delay: float = 5.0,
    ) -> None:
        self._timeout = timeout
        self._rate_limit = rate_limit
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._last_request_time: float = 0.0
        self._client: httpx.Client | None = None

    def __enter__(self) -> OabSpClient:
        self._client = httpx.Client(
            headers={
                "User-Agent": "AtlasSTF/1.0 (academic research; https://github.com/vittoroliveira-dev/atlas-stf)",
                "Accept": "text/html,application/xhtml+xml",
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

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Execute request with retry on transient errors."""
        if self._client is None:
            raise RuntimeError("OabSpClient must be used as a context manager")

        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            self._enforce_rate_limit()
            try:
                response = self._client.request(method, url, **kwargs)
                if response.status_code in {429, 500, 502, 503, 504}:
                    wait = self._retry_delay * (2**attempt)
                    logger.warning(
                        "OAB/SP %s %s returned %d (attempt %d/%d) — retrying in %.1fs",
                        method,
                        url,
                        response.status_code,
                        attempt + 1,
                        self._max_retries,
                        wait,
                    )
                    last_error = httpx.HTTPStatusError(
                        f"HTTP {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response
            except httpx.RequestError as exc:
                last_error = exc
                wait = self._retry_delay * (2**attempt)
                logger.warning(
                    "OAB/SP request failed (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    self._max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)

        raise RuntimeError(f"OAB/SP request failed after {self._max_retries} attempts") from last_error

    def search_by_registration(self, registration_number: str) -> str:
        """POST search to find society by registration number. Returns HTML."""
        response = self._request_with_retry(
            "POST",
            OABSP_SEARCH_URL,
            data={
                "tipoConsulta": "1",
                "nr_RegistroSociedade": registration_number,
                "nm_RazaoSocial": "",
                "tipoSociedade": "1",
                "id_Municipio": "0",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://www2.oabsp.org.br/asp/consultaSociedades/consultaSociedades01.asp",
            },
        )
        return response.text

    def fetch_detail(self, param_id: str) -> str:
        """GET detail page for a society. Returns HTML."""
        url = f"{OABSP_DETAIL_URL}?param={param_id}"
        response = self._request_with_retry("GET", url)
        return response.text

    def search_inscrito(
        self,
        *,
        registration_number: str | None = None,
        name: str | None = None,
        city_id: str = "0",
    ) -> str:
        """Search inscritos (lawyers) by registration number or name. Returns HTML."""
        if registration_number:
            form_data = {
                "tipo_consulta": "1",
                "nr_inscricao": registration_number,
                "cbxadv": "1",
                "idCidade": "0",
                "nome_advogado": "",
                "nr_cpf": "",
            }
        elif name:
            form_data = {
                "tipo_consulta": "2",
                "nome_advogado": name,
                "parte_nome": "1",  # "começo do nome"
                "cbxadv": "1",
                "idCidade": city_id,
                "nr_inscricao": "",
                "nr_cpf": "",
            }
        else:
            raise ValueError("Either registration_number or name must be provided")

        response = self._request_with_retry(
            "POST",
            OABSP_INSCRITOS_URL,
            data=form_data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": OABSP_INSCRITOS_URL,
            },
        )
        return response.text
