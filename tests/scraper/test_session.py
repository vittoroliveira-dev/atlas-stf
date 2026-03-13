from __future__ import annotations

from types import SimpleNamespace

from atlas_stf.scraper import _session


class _FakePage:
    def __init__(self) -> None:
        self.init_scripts: list[str] = []
        self.goto_calls: list[tuple[str, str, int]] = []

    def add_init_script(self, script: str) -> None:
        self.init_scripts.append(script)

    def goto(self, url: str, *, wait_until: str, timeout: int) -> None:
        self.goto_calls.append((url, wait_until, timeout))


class _FakeContext:
    def __init__(self) -> None:
        self.page = _FakePage()

    def new_page(self) -> _FakePage:
        return self.page


class _FakeBrowser:
    def __init__(self) -> None:
        self.context_args: dict | None = None
        self.context = _FakeContext()

    def new_context(self, **kwargs):
        self.context_args = kwargs
        return self.context


class _FakePlaywright:
    def __init__(self) -> None:
        self.browser = _FakeBrowser()
        self.chromium = SimpleNamespace(launch=self._launch)

    def _launch(self, *, headless: bool):
        self.headless = headless
        return self.browser

    def stop(self) -> None:
        return None


class _FakeSyncPlaywright:
    def __init__(self, playwright: _FakePlaywright) -> None:
        self.playwright = playwright

    def start(self) -> _FakePlaywright:
        return self.playwright


def test_create_defaults_to_tls_verification(monkeypatch):
    fake = _FakePlaywright()
    monkeypatch.delenv("ATLAS_STF_SCRAPER_IGNORE_HTTPS_ERRORS", raising=False)
    monkeypatch.setattr(_session, "sync_playwright", lambda: _FakeSyncPlaywright(fake))

    session = _session.ApiSession.create()

    assert fake.browser.context_args is not None
    assert fake.browser.context_args["ignore_https_errors"] is False
    session.close()


def test_create_allows_explicit_tls_bypass(monkeypatch):
    fake = _FakePlaywright()
    monkeypatch.setenv("ATLAS_STF_SCRAPER_IGNORE_HTTPS_ERRORS", "true")
    monkeypatch.setattr(_session, "sync_playwright", lambda: _FakeSyncPlaywright(fake))

    session = _session.ApiSession.create()

    assert fake.browser.context_args is not None
    assert fake.browser.context_args["ignore_https_errors"] is True
    session.close()
