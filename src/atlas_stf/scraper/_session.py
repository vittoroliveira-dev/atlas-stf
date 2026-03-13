"""Playwright-based session with WAF bypass for jurisprudencia.stf.jus.br."""

from __future__ import annotations

import logging
import os
from typing import Any

from playwright.sync_api import Browser, BrowserContext, Page, Playwright, sync_playwright

logger = logging.getLogger("atlas_stf.scraper")

BASE_URL = "https://jurisprudencia.stf.jus.br"
DEFAULT_IGNORE_HTTPS_ERRORS_ENV = "ATLAS_STF_SCRAPER_IGNORE_HTTPS_ERRORS"


class ApiError(Exception):
    """Raised when an API request fails."""

    def __init__(self, status: int, url: str, body: str = "") -> None:
        self.status = status
        self.url = url
        self.body = body
        super().__init__(f"HTTP {status} for {url}")


class ApiSession:
    """Playwright browser session that inherits WAF cookies for API calls."""

    def __init__(
        self,
        playwright: Playwright,
        browser: Browser,
        context: BrowserContext,
        page: Page,
    ) -> None:
        self._playwright = playwright
        self._browser = browser
        self._context = context
        self._page = page

    @classmethod
    def create(cls, *, headless: bool = True, timeout_ms: int = 30_000) -> ApiSession:
        """Launch browser, navigate to site to solve WAF challenge, return ready session."""
        ignore_https_errors = (
            os.getenv(DEFAULT_IGNORE_HTTPS_ERRORS_ENV, "false").strip().lower() in {"1", "true", "yes", "on"}
        )
        if ignore_https_errors:
            logger.warning(
                "TLS verification disabled for jurisprudencia session via %s",
                DEFAULT_IGNORE_HTTPS_ERRORS_ENV,
            )
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            ignore_https_errors=ignore_https_errors,
        )
        page = context.new_page()

        # Remove webdriver flag
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        logger.info("Navigating to %s to solve WAF challenge ...", BASE_URL)
        page.goto(BASE_URL, wait_until="networkidle", timeout=timeout_ms)
        logger.info("WAF challenge resolved, session ready")

        return cls(pw, browser, context, page)

    def post_json(self, url: str, payload: Any) -> dict:
        """POST JSON using Playwright's request context (inherits WAF cookies)."""
        full_url = f"{BASE_URL}{url}" if url.startswith("/") else url
        response = self._page.request.post(
            full_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        if response.status != 200:
            raise ApiError(response.status, full_url, response.text())
        return response.json()

    def get_json(self, url: str) -> dict:
        """GET JSON using Playwright's request context."""
        full_url = f"{BASE_URL}{url}" if url.startswith("/") else url
        response = self._page.request.get(full_url)
        if response.status != 200:
            raise ApiError(response.status, full_url, response.text())
        return response.json()

    def close(self) -> None:
        """Clean shutdown: page → context → browser → playwright."""
        try:
            self._page.close()
        except Exception:
            pass
        try:
            self._context.close()
        except Exception:
            pass
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._playwright.stop()
        except Exception:
            pass

    def __enter__(self) -> ApiSession:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
