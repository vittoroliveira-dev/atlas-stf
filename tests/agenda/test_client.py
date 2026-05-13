from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

import atlas_stf.agenda._client as client_module
from atlas_stf.agenda._client import AgendaClient, AgendaWafChallengeError
from atlas_stf.agenda._config import AgendaFetchConfig


def _cfg(**kw):
    d = {"rate_limit_seconds": 0.0, "max_retries": 3, "retry_delay_seconds": 0.0, "timeout_seconds": 5.0}
    d.update(kw)
    return AgendaFetchConfig(**d)


def _browser_response(status: int, body: str, headers: dict[str, str] | None = None) -> dict:
    """Simulate the dict returned by page.evaluate(fetch(...))."""
    return {
        "status": status,
        "headers": headers or {"content-type": "application/json"},
        "body": body,
    }


class _FakePage:
    def __init__(self) -> None:
        self.init_scripts: list[str] = []
        self.goto_calls: list[tuple[str, str, int]] = []
        self.closed = False

    def add_init_script(self, script: str) -> None:
        self.init_scripts.append(script)

    def goto(self, url: str, *, wait_until: str, timeout: int) -> None:
        self.goto_calls.append((url, wait_until, timeout))

    def close(self) -> None:
        self.closed = True


class _FakeContext:
    def __init__(self) -> None:
        self.page = _FakePage()
        self.kwargs: dict[str, object] | None = None
        self.default_timeout: int | None = None
        self.navigation_timeout: int | None = None
        self.closed = False

    def new_page(self) -> _FakePage:
        return self.page

    def set_default_timeout(self, timeout: int) -> None:
        self.default_timeout = timeout

    def set_default_navigation_timeout(self, timeout: int) -> None:
        self.navigation_timeout = timeout

    def close(self) -> None:
        self.closed = True


class _FakeBrowser:
    def __init__(self) -> None:
        self.context = _FakeContext()
        self.launch_headless: bool | None = None
        self.closed = False

    def new_context(self, **kwargs: object) -> _FakeContext:
        self.context.kwargs = kwargs
        return self.context

    def close(self) -> None:
        self.closed = True


class _FakeChromium:
    def __init__(self) -> None:
        self.browser = _FakeBrowser()

    def launch(self, *, headless: bool) -> _FakeBrowser:
        self.browser.launch_headless = headless
        return self.browser


class _FakePlaywright:
    def __init__(self) -> None:
        self.chromium = _FakeChromium()
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class _FakeSyncPlaywrightFactory:
    def __init__(self) -> None:
        self.playwright = _FakePlaywright()

    def start(self) -> _FakePlaywright:
        return self.playwright


class TestAgendaClient:
    def test_browser_context_uses_configured_timeout_without_tls_bypass(self, monkeypatch: pytest.MonkeyPatch):
        factory = _FakeSyncPlaywrightFactory()
        monkeypatch.setattr(client_module, "sync_playwright", lambda: factory)

        client = AgendaClient(_cfg(timeout_seconds=12.5))
        with client:
            pass

        browser = factory.playwright.chromium.browser
        context = browser.context
        page = context.page

        assert browser.launch_headless is True
        kwargs = context.kwargs
        if kwargs is None:
            raise AssertionError("Expected browser context kwargs")
        assert kwargs == {"user_agent": client_module.USER_AGENT}
        assert "ignore_https_errors" not in kwargs
        assert context.default_timeout == 12_500
        assert context.navigation_timeout == 12_500
        assert page.goto_calls == [(client_module.GRAPHQL_BASE_URL, "networkidle", 12_500)]
        assert factory.playwright.stopped is True

    def test_get_success(self):
        data = {"data": {"agendaMinistrosPorDiaCategoria": [{"data": "02/03/2024"}]}}

        client = AgendaClient(_cfg())
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(200, json.dumps(data))

        d, meta = client.fetch_month(2024, 3)
        assert meta["fetch_method"] == "GET"
        assert meta["contract_version_detected"] is True
        assert d["data"]["agendaMinistrosPorDiaCategoria"][0]["data"] == "02/03/2024"

    def test_contract_unknown(self):
        data = {"data": {"other": []}}

        client = AgendaClient(_cfg())
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(200, json.dumps(data))

        _, meta = client.fetch_month(2024, 3)
        assert meta["contract_version_detected"] is False

    def test_waf_challenge_raises(self):
        client = AgendaClient(_cfg(max_retries=1))
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(
            202,
            "",
            headers={"x-amzn-waf-action": "challenge", "content-type": "text/html"},
        )

        with pytest.raises(AgendaWafChallengeError, match="WAF challenge"):
            client.fetch_month(2024, 3)

    def test_waf_challenge_no_retry(self):
        """WAF challenge must fail fast — no retries."""
        client = AgendaClient(_cfg(max_retries=3))
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(
            202,
            "",
            headers={"x-amzn-waf-action": "challenge", "content-type": "text/html"},
        )

        with pytest.raises(AgendaWafChallengeError):
            client.fetch_month(2024, 3)

        # Should have been called exactly once (no retries)
        assert client._page.evaluate.call_count == 1

    def test_fetch_timeout_is_propagated_to_browser_fetch(self):
        data = {"data": {"agendaMinistrosPorDiaCategoria": []}}
        client = AgendaClient(_cfg(timeout_seconds=7.25))
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(200, json.dumps(data))

        _, meta = client.fetch_month(2024, 3)

        script, payload = client._page.evaluate.call_args.args
        assert meta["contract_version_detected"] is True
        assert "AbortController" in script
        assert payload["timeoutMs"] == 7_250
        assert payload["url"].startswith(client_module.GRAPHQL_BASE_URL + "graphql?")

    def test_http_error_retries(self):
        """Network errors should be retried."""
        data = {"data": {"agendaMinistrosPorDiaCategoria": []}}

        client = AgendaClient(_cfg(max_retries=3))
        client._page = MagicMock()
        client._page.evaluate.side_effect = [
            _browser_response(500, "Internal Server Error"),
            _browser_response(200, json.dumps(data)),
        ]

        _, meta = client.fetch_month(2024, 3)
        assert meta["contract_version_detected"] is True
        assert client._page.evaluate.call_count == 2

    def test_fetch_timeout_error_retries(self):
        data = {"data": {"agendaMinistrosPorDiaCategoria": []}}

        client = AgendaClient(_cfg(max_retries=3, timeout_seconds=5.0))
        client._page = MagicMock()
        client._page.evaluate.side_effect = [
            RuntimeError("fetch timeout after 5000ms"),
            _browser_response(200, json.dumps(data)),
        ]

        _, meta = client.fetch_month(2024, 3)
        assert meta["contract_version_detected"] is True
        assert client._page.evaluate.call_count == 2

    def test_empty_body_retries(self):
        """Empty body (non-WAF) should be retried."""
        data = {"data": {"agendaMinistrosPorDiaCategoria": []}}

        client = AgendaClient(_cfg(max_retries=3))
        client._page = MagicMock()
        client._page.evaluate.side_effect = [
            _browser_response(200, ""),
            _browser_response(200, json.dumps(data)),
        ]

        _, meta = client.fetch_month(2024, 3)
        assert client._page.evaluate.call_count == 2
