"""Navigate and extract timeline data from the STF portal.

Uses httpx for HTTP requests with adaptive anti-blocking techniques.
Client lifecycle managed by ``ClientPool`` (thread-safe, deferred close).
Per-proxy rate limiting and circuit breaking via ``ProxyManager``.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx

from ._client_pool import ClientPool
from ._http import (
    INCIDENTE_RE,
    LIST_URL,
    PORTAL_BASE,
    PROCESS_DETAIL_URL,
    TAB_BASE,
    TABS,
    TabFetchResult,
    TabsBatchResult,
)
from ._parser import (
    build_process_document,
    parse_andamentos_html,
    parse_deslocamentos_html,
    parse_informacoes_html,
    parse_partes_representantes_html,
    parse_peticoes_detailed_html,
    parse_peticoes_html,
)
from ._proxy import ProxyManager

logger = logging.getLogger(__name__)

# Re-export for backward compat
__all__ = ["PortalExtractor", "TabFetchResult", "TabsBatchResult"]


class PortalExtractor:
    """Extracts process timeline data from the STF portal."""

    def __init__(
        self,
        *,
        rate_limit_seconds: float = 2.0,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        retry_delay_seconds: float = 5.0,
        ignore_tls: bool = False,
        request_semaphore: threading.Semaphore | None = None,
        proxy_manager: ProxyManager | None = None,
        tab_concurrency: int = 2,
    ) -> None:
        self._rate_limit = rate_limit_seconds
        self._max_retries = max_retries
        self._retry_delay = retry_delay_seconds
        self._request_semaphore = request_semaphore
        self._proxy_manager = proxy_manager
        self._tab_concurrency = tab_concurrency
        self._last_request_time: float = 0
        self._pool = ClientPool(timeout=timeout_seconds, ignore_tls=ignore_tls)

    # --- Facade: expose pool internals for test compatibility ---

    @property
    def _client_lock(self) -> threading.Lock:
        return self._pool._lock

    @property
    def _retired_clients(self) -> list[httpx.Client]:
        return self._pool._retired

    @property
    def _client(self) -> httpx.Client | None:
        return self._pool._client

    @_client.setter
    def _client(self, value: httpx.Client | None) -> None:
        self._pool._client = value

    def _get_client(self) -> httpx.Client:
        return self._pool.get_client()

    def _rotate_client_for_proxy(self, proxy: str | None) -> None:
        self._pool.rotate_for_proxy(proxy)

    # --- Rate limiting ---

    def _local_rate_limit_wait(self) -> None:
        """Fallback rate limiting when no ProxyManager is set."""
        elapsed = time.monotonic() - self._last_request_time
        jitter = random.uniform(0.3, 1.5)  # noqa: S311
        wait_target = self._rate_limit * jitter
        if elapsed < wait_target:
            time.sleep(wait_target - elapsed)
        self._last_request_time = time.monotonic()

    # --- HTTP ---

    def _acquire_and_get(
        self,
        url: str,
        params: dict[str, str] | None,
        headers: dict[str, str],
        *,
        follow_redirects: bool = True,
    ) -> tuple[httpx.Response, str | None]:
        """Acquire a proxy slot, execute HTTP GET, return ``(response, proxy_url)``."""
        if self._proxy_manager:
            proxy, _ = self._proxy_manager.acquire()
        else:
            self._local_rate_limit_wait()
            proxy = None

        with self._pool._lock:
            client = self._pool.resolve(proxy, follow_redirects=follow_redirects)

        if self._request_semaphore:
            self._request_semaphore.acquire()
        try:
            return client.get(url, params=params, headers=headers), proxy
        except httpx.HTTPStatusError:
            raise
        except Exception:
            with self._pool._lock:
                client = self._pool.resolve(proxy, follow_redirects=follow_redirects, force_new=True)
            return client.get(url, params=params, headers=headers), proxy
        finally:
            if self._request_semaphore:
                self._request_semaphore.release()

    # --- Fetch logic ---

    def _fetch_with_retry(self, url: str, params: dict[str, str] | None = None) -> TabFetchResult:
        """Fetch a URL with retry logic and adaptive backoff on 403."""
        tab_name = ""
        if params and "incidente" in params:
            for t in TABS:
                if t in url:
                    tab_name = t
                    break

        proxy: str | None = None
        for attempt in range(self._max_retries):
            try:
                headers = {"Referer": PORTAL_BASE + "/", "User-Agent": self._pool.pick_user_agent()}
                resp, proxy = self._acquire_and_get(url, params, headers)
                resp.raise_for_status()
                if self._proxy_manager:
                    self._proxy_manager.record_success(proxy)
                return TabFetchResult(tab=tab_name, html=resp.text, success=True, blocked=False, retryable=False)
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 403:
                    if self._proxy_manager:
                        self._proxy_manager.record_403(proxy)
                    self._pool.rotate_for_proxy(proxy)
                    wait = self._retry_delay * (3**attempt) + random.uniform(2.0, 8.0)  # noqa: S311
                    logger.warning(
                        "403 Forbidden (attempt %d/%d) — adaptive backoff %.1fs",
                        attempt + 1,
                        self._max_retries,
                        wait,
                    )
                    time.sleep(wait)
                elif attempt < self._max_retries - 1:
                    wait = self._retry_delay * (2**attempt)
                    logger.warning(
                        "HTTP %d (attempt %d/%d): %s — retrying in %.1fs",
                        status,
                        attempt + 1,
                        self._max_retries,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    return TabFetchResult(
                        tab=tab_name,
                        html="",
                        success=False,
                        blocked=status == 403,
                        retryable=status in (408, 429, 500, 502, 503, 504),
                    )
            except Exception as exc:
                if attempt < self._max_retries - 1:
                    wait = self._retry_delay * (2**attempt)
                    logger.warning(
                        "Request failed (attempt %d/%d): %s — retrying in %.1fs",
                        attempt + 1,
                        self._max_retries,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    return TabFetchResult(tab=tab_name, html="", success=False, blocked=False, retryable=True)
        return TabFetchResult(tab=tab_name, html="", success=False, blocked=True, retryable=False)

    def _split_process_number(self, process_number: str) -> tuple[str, str]:
        """Split 'ADI 1234' into ('ADI', '1234')."""
        parts = process_number.strip().split(maxsplit=1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return "", process_number

    def _resolve_incidente(self, process_number: str) -> tuple[str | None, bool]:
        """Resolve incidente ID from the 302 Location header."""
        classe, numero = self._split_process_number(process_number)
        params = {"classe": classe, "numeroProcesso": numero}

        for attempt in range(self._max_retries):
            try:
                headers = {"Referer": PORTAL_BASE + "/", "User-Agent": self._pool.pick_user_agent()}
                resp, proxy = self._acquire_and_get(LIST_URL, params, headers, follow_redirects=False)

                if resp.status_code in (301, 302, 303, 307, 308):
                    location = resp.headers.get("location", "")
                    match = INCIDENTE_RE.search(location)
                    if match:
                        if self._proxy_manager:
                            self._proxy_manager.record_success(proxy)
                        return match.group(1), False

                if resp.status_code == 200 and resp.text:
                    match = INCIDENTE_RE.search(resp.text)
                    if match:
                        if self._proxy_manager:
                            self._proxy_manager.record_success(proxy)
                        return match.group(1), False

                if resp.status_code == 403:
                    if self._proxy_manager:
                        self._proxy_manager.record_403(proxy)
                    self._pool.rotate_for_proxy(proxy)
                    wait = self._retry_delay * (3**attempt) + random.uniform(2.0, 8.0)  # noqa: S311
                    logger.warning("403 on resolve %s (attempt %d/%d)", process_number, attempt + 1, self._max_retries)
                    time.sleep(wait)
                    continue

                logger.warning(
                    "No incidente found for %s (status=%d, location=%s)",
                    process_number,
                    resp.status_code,
                    resp.headers.get("location", ""),
                )
                return None, False
            except Exception as exc:
                if attempt < self._max_retries - 1:
                    wait = self._retry_delay * (2**attempt)
                    logger.warning(
                        "Resolve %s failed (attempt %d/%d): %s",
                        process_number,
                        attempt + 1,
                        self._max_retries,
                        exc,
                    )
                    time.sleep(wait)
                else:
                    logger.warning("Resolve %s failed after %d attempts", process_number, self._max_retries)
                    return None, False
        return None, True

    def _fetch_tabs_concurrent(self, incidente: str) -> TabsBatchResult:
        """Fetch all tabs concurrently (mirrors real browser behavior)."""
        results: dict[str, str] = {}
        got_403 = threading.Event()
        tabs_failed: set[str] = set()
        any_retryable = threading.Event()

        def _fetch_one(tab: str) -> TabFetchResult:
            if got_403.is_set():
                return TabFetchResult(tab=tab, html="", success=False, blocked=True, retryable=False)
            url = f"{TAB_BASE}/{tab}.asp"
            referer = f"{PROCESS_DETAIL_URL}?incidente={incidente}"
            headers = {"Referer": referer, "User-Agent": self._pool.pick_user_agent()}
            proxy: str | None = None
            for attempt in range(self._max_retries):
                if got_403.is_set():
                    return TabFetchResult(tab=tab, html="", success=False, blocked=True, retryable=False)
                try:
                    resp, proxy = self._acquire_and_get(url, {"incidente": incidente}, headers)
                    resp.raise_for_status()
                    if self._proxy_manager:
                        self._proxy_manager.record_success(proxy)
                    return TabFetchResult(tab=tab, html=resp.text, success=True, blocked=False, retryable=False)
                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code
                    if status == 403:
                        if self._proxy_manager:
                            self._proxy_manager.record_403(proxy)
                        self._pool.rotate_for_proxy(proxy)
                        got_403.set()
                        return TabFetchResult(tab=tab, html="", success=False, blocked=True, retryable=False)
                    if attempt < self._max_retries - 1:
                        time.sleep(self._retry_delay * (2**attempt))
                    else:
                        logger.warning("Tab %s failed for incidente %s (HTTP %d)", tab, incidente, status)
                        is_retryable = status in (408, 429, 500, 502, 503, 504)
                        return TabFetchResult(tab=tab, html="", success=False, blocked=False, retryable=is_retryable)
                except Exception:
                    if attempt < self._max_retries - 1:
                        time.sleep(self._retry_delay * (2**attempt))
                    else:
                        logger.warning("Tab %s failed for incidente %s", tab, incidente)
                        return TabFetchResult(tab=tab, html="", success=False, blocked=False, retryable=True)
            return TabFetchResult(tab=tab, html="", success=False, blocked=True, retryable=False)

        with ThreadPoolExecutor(max_workers=self._tab_concurrency) as pool:
            for result in pool.map(_fetch_one, TABS):
                if result.success:
                    results[result.tab] = result.html
                else:
                    tabs_failed.add(result.tab)
                    if result.retryable:
                        any_retryable.set()

        return TabsBatchResult(
            tabs=results,
            blocked=got_403.is_set(),
            retryable=any_retryable.is_set(),
            tabs_failed=tabs_failed,
        )

    # --- Orchestration ---

    def extract_process(self, process_number: str, incidente: str | None = None) -> dict[str, Any] | None:
        """Extract full timeline data for a single process."""
        try:
            if not incidente:
                incidente, blocked = self._resolve_incidente(process_number)
                if not incidente:
                    if blocked:
                        logger.warning("403 block on resolve for %s — will retry later", process_number)
                    else:
                        logger.warning("Could not resolve incidente for %s — skipping", process_number)
                    return None

            source_url = f"{PROCESS_DETAIL_URL}?incidente={incidente}"
            batch = self._fetch_tabs_concurrent(incidente)

            if batch.blocked:
                logger.warning("403 block on tabs for %s — will retry later", process_number)
                return None
            if batch.tabs_failed:
                failed_names = ", ".join(sorted(batch.tabs_failed))
                if batch.retryable:
                    logger.warning("Retryable failure on %s for %s — will retry later", failed_names, process_number)
                else:
                    logger.warning("Non-retryable failure on %s for %s — skipping", failed_names, process_number)
                return None

            tabs = batch.tabs
            andamentos = parse_andamentos_html(tabs.get("abaAndamentos", ""))
            deslocamentos = parse_deslocamentos_html(tabs.get("abaDeslocamentos", ""))
            peticoes = parse_peticoes_html(tabs.get("abaPeticoes", ""))
            informacoes = parse_informacoes_html(tabs.get("abaInformacoes", ""))
            representantes = parse_partes_representantes_html(tabs.get("abaPartes", ""))
            peticoes_detailed = parse_peticoes_detailed_html(tabs.get("abaPeticoes", ""))

            tab_order = ("abaAndamentos", "abaPartes", "abaPeticoes", "abaDeslocamentos", "abaInformacoes")
            combined_html = "\n".join(tabs.get(t, "") for t in tab_order)

            doc = build_process_document(
                process_number=process_number,
                source_url=source_url,
                raw_html=combined_html,
                andamentos=andamentos,
                deslocamentos=deslocamentos,
                peticoes=peticoes,
                sessao_virtual=[],
                informacoes=informacoes,
                representantes=representantes,
                peticoes_detailed=peticoes_detailed,
            )
            doc["incidente"] = incidente

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
        self._pool.close()

    def __enter__(self) -> PortalExtractor:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
