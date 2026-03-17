"""Download CSVs from STF transparency portal (Qlik Sense dashboards)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path

from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeout

from ._config import PAINEIS, TransparenciaFetchConfig, painel_url

logger = logging.getLogger(__name__)

_EXPORT_BUTTON_SELECTORS = (
    "#EXPORT-BUTTON-PADRAO",
    "#EXPORT-BUTTON-TOP",
    "button[data-qcmd='exportar_padrao']",
    "a[data-qcmd='exportar_padrao']",
    "button[data-qcmd='exportar_selecionado']",
    "a[data-qcmd='exportar_selecionado']",
)


def _wait_qlik_load(page: Page, timeout_ms: int) -> bool:
    """Wait for Qlik Sense to finish rendering dashboard objects."""
    try:
        page.wait_for_selector("#loader", state="hidden", timeout=timeout_ms)
        time.sleep(3)
        return True
    except Exception:
        try:
            page.wait_for_selector(".qvobject", timeout=timeout_ms)
            time.sleep(5)
            return True
        except Exception:
            return False


def _clear_qlik_filters(page: Page) -> None:
    """Remove active Qlik selections (e.g. default year filter) via RequireJS."""
    result = page.evaluate("""
        (() => {
            try {
                if (typeof require !== 'undefined') {
                    return new Promise((resolve) => {
                        require(['js/qlik'], function(qlik) {
                            try {
                                const app = qlik.currApp();
                                if (app) { app.clearAll(); resolve('ok_require'); }
                                else { resolve('no_app'); }
                            } catch(e) { resolve('error_require: ' + e.message); }
                        });
                        setTimeout(() => resolve('timeout'), 10000);
                    });
                }
                return 'no_require';
            } catch(e) { return 'error: ' + e.message; }
        })()
    """)
    logger.debug("clearAll result: %s", result)

    if result and str(result).startswith("ok"):
        time.sleep(3)
        return

    try:
        clear_btn = page.locator(".clear-all, [title*='Limpar'], [title*='Clear']").first
        if clear_btn.is_visible(timeout=3000):
            clear_btn.click()
            logger.debug("Cleared filters via UI button")
            time.sleep(3)
    except Exception:
        pass


def _download_csv(page: Page, download_dir: Path, slug: str, timeout_ms: int) -> Path | None:
    """Click the export button and wait for the CSV download."""
    button = None
    for sel in _EXPORT_BUTTON_SELECTORS:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible(timeout=2000):
                button = loc.first
                logger.debug("Export button found: %s", sel)
                break
        except Exception:
            continue

    if button is None:
        logger.warning("No export button found for %s", slug)
        return None

    try:
        with page.expect_download(timeout=timeout_ms) as download_info:
            button.click()

        download = download_info.value
        ext = Path(download.suggested_filename or "export.csv").suffix or ".csv"
        dest = download_dir / f"{slug}{ext}"
        download.save_as(str(dest))
        size_kb = dest.stat().st_size / 1024
        logger.info("%s: downloaded (%.0f KB)", slug, size_kb)
        return dest

    except PlaywrightTimeout:
        logger.warning("%s: download timed out", slug)
        return None


def _process_painel(
    page: Page,
    slug: str,
    config: TransparenciaFetchConfig,
) -> Path | None:
    """Navigate to a panel, clear filters, and export the CSV."""
    url = painel_url(slug)
    logger.info("%s — %s", slug, PAINEIS[slug])

    page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    if not _wait_qlik_load(page, config.timeout_qlik_load_ms):
        logger.warning("%s: Qlik failed to load, skipping", slug)
        return None

    _clear_qlik_filters(page)
    time.sleep(2)

    return _download_csv(page, config.output_dir, slug, config.timeout_download_ms)


def fetch_transparencia_data(
    config: TransparenciaFetchConfig,
    *,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Download CSVs from STF transparency portal (Qlik Sense)."""
    invalid = [s for s in config.paineis if s not in PAINEIS]
    if invalid:
        msg = f"Unknown panel slugs: {', '.join(invalid)}"
        raise ValueError(msg)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    total = len(config.paineis)

    logger.info("Transparência STF: %d painéis selecionados", total)

    if config.dry_run:
        for slug in config.paineis:
            logger.info("  [dry-run] %s — %s", slug, PAINEIS[slug])
        return config.output_dir

    launch_args: list[str] = []
    if config.ignore_tls:
        logger.warning("TLS verification DISABLED for transparency portal")
        launch_args.extend(["--ignore-certificate-errors", "--disable-web-security"])

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=config.headless, args=launch_args or None)
        context = browser.new_context(
            accept_downloads=True,
            ignore_https_errors=config.ignore_tls,
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        page.on("dialog", lambda d: d.accept())

        downloaded = 0
        for i, slug in enumerate(config.paineis):
            if on_progress:
                on_progress(i, total, f"Transparência: {slug}")

            try:
                result = _process_painel(page, slug, config)
                if result:
                    downloaded += 1
            except Exception:
                logger.exception("Failed to process panel %s", slug)

        browser.close()

    if on_progress:
        on_progress(total, total, "Transparência: Concluído")

    logger.info("Transparency fetch complete: %d/%d panels downloaded", downloaded, total)
    return config.output_dir
