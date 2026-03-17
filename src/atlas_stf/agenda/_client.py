"""Playwright-based client for the STF ministerial agenda GraphQL API.

Uses a real browser to solve the AWS WAF challenge on noticias.stf.jus.br,
then executes GraphQL queries via the authenticated browser context.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any

from playwright.sync_api import Playwright, sync_playwright

from ._config import GRAPHQL_BASE_URL, USER_AGENT, AgendaFetchConfig

logger = logging.getLogger(__name__)

_GRAPHQL_QUERY_TPL = (
    "{{agendaMinistrosPorDiaCategoria(where:{{dateQuery:{{year:{year},month:{month}}}}},first:300)"
    "{{data descricaoData ministro{{nomeMinistro eventos{{titulo hora}}}}}}}}"
)

_CONTRACT_KEY = "agendaMinistrosPorDiaCategoria"
_HEADER_KEYS = ("content-type", "date", "server", "x-request-id", "cache-control")


class AgendaWafChallengeError(RuntimeError):
    """Raised when the STF WAF blocks the request with a challenge that requires a browser."""


def _sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class AgendaClient:
    """Playwright-based GraphQL client for the STF ministerial agenda API.

    Opens a headless Chromium browser, navigates to the STF noticias site
    to solve the AWS WAF challenge, then reuses that authenticated context
    for all subsequent GraphQL requests.
    """

    def __init__(self, config: AgendaFetchConfig) -> None:
        self._config = config
        self._pw: Playwright | None = None
        self._last_request_time: float = 0

    def __enter__(self) -> AgendaClient:
        self._pw = sync_playwright().start()
        browser = self._pw.chromium.launch(headless=True)
        self._browser = browser
        self._context = browser.new_context(
            user_agent=USER_AGENT,
            ignore_https_errors=True,
        )
        self._page = self._context.new_page()
        self._page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        logger.info("Navigating to %s to solve WAF challenge ...", GRAPHQL_BASE_URL)
        self._page.goto(GRAPHQL_BASE_URL, wait_until="networkidle", timeout=30_000)
        logger.info("WAF challenge resolved, browser session ready")
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        for obj in ("_page", "_context", "_browser"):
            try:
                getattr(self, obj).close()
            except Exception:
                pass
        if self._pw is not None:
            try:
                self._pw.stop()
            except Exception:
                pass
            self._pw = None

    def _rate_limit_wait(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._config.rate_limit_seconds:
            time.sleep(self._config.rate_limit_seconds - elapsed)
        self._last_request_time = time.monotonic()

    def fetch_month(self, year: int, month: int) -> tuple[dict[str, Any], dict[str, Any]]:
        """Fetch agenda data for a single month.

        Returns ``(raw_response_dict, fetch_metadata_dict)``.
        Raises ``AgendaWafChallengeError`` if the WAF still blocks after browser session.
        """
        query = _GRAPHQL_QUERY_TPL.format(year=year, month=month)
        query_hash = _sha256(query)
        url = GRAPHQL_BASE_URL + "graphql?" + urllib.parse.urlencode({"query": query})

        meta: dict[str, Any] = {
            "year": year,
            "month": month,
            "query_hash": query_hash,
            "user_agent": USER_AGENT,
            "fetched_at": "",
            "fetch_method": "GET",
            "http_status": 0,
            "response_sha256": "",
            "response_size_bytes": 0,
            "response_headers_subset": {},
            "contract_version_detected": False,
        }

        last_exc: Exception | None = None
        for attempt in range(self._config.max_retries):
            self._rate_limit_wait()
            try:
                return self._try_fetch(url, query_hash, meta)
            except AgendaWafChallengeError:
                raise
            except Exception as exc:
                last_exc = exc
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

        if last_exc is not None:
            raise last_exc
        return {}, meta  # unreachable

    def _try_fetch(
        self,
        url: str,
        query_hash: str,
        meta: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        # Execute fetch() inside the browser context so the request inherits
        # the WAF challenge token cookies.  page.request.get() does raw HTTP
        # and does NOT carry the solved challenge — that's why it still gets 202.
        result: dict[str, Any] = self._page.evaluate(
            """async (url) => {
                const r = await fetch(url, {credentials: 'include'});
                const text = await r.text();
                return {
                    status: r.status,
                    headers: Object.fromEntries(r.headers.entries()),
                    body: text,
                };
            }""",
            url,
        )

        status: int = result["status"]
        body_text: str = result["body"]
        headers: dict[str, str] = result["headers"]

        meta["http_status"] = status
        meta["response_headers_subset"] = {k: headers.get(k, "") for k in _HEADER_KEYS if headers.get(k)}

        waf_action = headers.get("x-amzn-waf-action", "")
        if status == 202 and waf_action == "challenge":
            raise AgendaWafChallengeError(
                f"STF WAF challenge: HTTP 202, action={waf_action!r}, "
                f"content_type={headers.get('content-type', '')!r}, "
                f"content_length={headers.get('content-length', '')!r}"
            )

        if status >= 400:
            raise RuntimeError(f"HTTP {status} from {url[:80]}: {body_text[:200]}")

        if not body_text.strip():
            raise RuntimeError(f"Empty response body (HTTP {status})")

        raw_data: dict[str, Any] = json.loads(body_text)

        meta["fetched_at"] = datetime.now(timezone.utc).isoformat()
        meta["response_sha256"] = _sha256(body_text)
        meta["response_size_bytes"] = len(body_text.encode("utf-8"))
        meta["query_hash"] = query_hash

        data_payload = raw_data.get("data", {})
        meta["contract_version_detected"] = _CONTRACT_KEY in data_payload

        if not meta["contract_version_detected"]:
            logger.warning("Contract mismatch: missing 'data.%s'", _CONTRACT_KEY)

        return raw_data, meta
