"""Navigate and extract timeline data from the STF portal.

Uses httpx for HTTP requests. Falls back to Playwright if needed
(e.g., for JavaScript-rendered content).

The extraction strategy will be refined after Phase 2B probing.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from ._parser import (
    build_process_document,
    parse_andamentos_html,
    parse_deslocamentos_html,
    parse_informacoes_html,
    parse_oral_argument_html,
    parse_partes_representantes_html,
    parse_peticoes_detailed_html,
    parse_peticoes_html,
    parse_sessao_virtual_html,
)

logger = logging.getLogger(__name__)

PORTAL_BASE = "https://portal.stf.jus.br"
PROCESS_DETAIL_URL = f"{PORTAL_BASE}/processos/detalhe.asp"

# Common user agent for requests
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


class PortalExtractor:
    """Extracts process timeline data from the STF portal."""

    def __init__(
        self,
        *,
        rate_limit_seconds: float = 2.0,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        retry_delay_seconds: float = 5.0,
    ) -> None:
        self._rate_limit = rate_limit_seconds
        self._timeout = timeout_seconds
        self._max_retries = max_retries
        self._retry_delay = retry_delay_seconds
        self._client: httpx.Client | None = None
        self._last_request_time: float = 0

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=self._timeout,
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
        return self._client

    def _rate_limit_wait(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._rate_limit:
            time.sleep(self._rate_limit - elapsed)
        self._last_request_time = time.monotonic()

    def _fetch_with_retry(self, url: str, params: dict[str, str] | None = None) -> str:
        """Fetch a URL with retry logic. Returns HTML content."""
        client = self._get_client()
        for attempt in range(self._max_retries):
            self._rate_limit_wait()
            try:
                resp = client.get(url, params=params)
                resp.raise_for_status()
                return resp.text
            except httpx.HTTPError as exc:
                if attempt < self._max_retries - 1:
                    wait = self._retry_delay * (2**attempt)
                    logger.warning(
                        "Request failed (attempt %d/%d): %s — retrying in %.1fs",
                        attempt + 1, self._max_retries, exc, wait,
                    )
                    time.sleep(wait)
                else:
                    raise
        return ""  # unreachable, but satisfies type checker

    def extract_process(self, process_number: str, incidente: str | None = None) -> dict[str, Any] | None:
        """Extract full timeline data for a single process.

        Args:
            process_number: Process identifier (e.g., "ADI 1234")
            incidente: Optional incidente ID for the portal URL

        Returns:
            Structured process document, or None if extraction fails.
        """
        # Build the portal URL
        # The portal uses incidente IDs in the URL; if not provided,
        # we need to search for the process first
        if incidente:
            source_url = f"{PROCESS_DETAIL_URL}?incidente={incidente}"
        else:
            # Search by process number
            source_url = f"{PORTAL_BASE}/processos/listarProcessos.asp?classe=&processo={process_number}"

        logger.debug("Extracting process %s from %s", process_number, source_url)

        try:
            # Fetch the main page
            main_html = self._fetch_with_retry(source_url)

            # Parse each section
            # NOTE: The exact extraction will depend on the portal structure
            # discovered during probing (Phase 2B). Currently uses basic
            # HTML table parsing as a placeholder.
            andamentos = parse_andamentos_html(main_html)
            deslocamentos = parse_deslocamentos_html(main_html)
            peticoes = parse_peticoes_html(main_html)
            sessao_virtual = parse_sessao_virtual_html(main_html)
            informacoes = parse_informacoes_html(main_html)

            # Phase 2B: Representation-network parsers
            representantes = parse_partes_representantes_html(main_html)
            peticoes_detailed = parse_peticoes_detailed_html(main_html)
            oral_arguments = parse_oral_argument_html(main_html)

            doc = build_process_document(
                process_number=process_number,
                source_url=source_url,
                raw_html=main_html,
                andamentos=andamentos,
                deslocamentos=deslocamentos,
                peticoes=peticoes,
                sessao_virtual=sessao_virtual,
                informacoes=informacoes,
                representantes=representantes,
                peticoes_detailed=peticoes_detailed,
                oral_arguments=oral_arguments,
            )

            logger.info(
                "Extracted %s: %d andamentos, %d deslocamentos, %d peticoes, %d representantes",
                process_number,
                len(andamentos),
                len(deslocamentos),
                len(peticoes),
                len(representantes),
            )
            return doc

        except Exception:
            logger.exception("Failed to extract process %s", process_number)
            return None

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> PortalExtractor:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
