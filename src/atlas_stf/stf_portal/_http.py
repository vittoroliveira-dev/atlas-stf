"""HTTP constants, browser fingerprint, and result types for STF portal extraction."""

from __future__ import annotations

import re
from dataclasses import dataclass

PORTAL_BASE = "https://portal.stf.jus.br"
PROCESS_DETAIL_URL = f"{PORTAL_BASE}/processos/detalhe.asp"
LIST_URL = f"{PORTAL_BASE}/processos/listarProcessos.asp"
TAB_BASE = f"{PORTAL_BASE}/processos"

INCIDENTE_RE = re.compile(r"incidente=(\d+)")

# Tabs to fetch (abaSessao excluded — JS-rendered, parser returns empty)
TABS = ("abaAndamentos", "abaPartes", "abaPeticoes", "abaDeslocamentos", "abaInformacoes")

USER_AGENTS = [
    # Chrome (Linux / Windows / macOS)
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    " (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Firefox (Linux / Windows)
    "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Safari (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"
    " (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
]

BROWSER_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}


@dataclass
class TabFetchResult:
    """Result of fetching a single tab with failure classification."""

    tab: str
    html: str
    success: bool
    blocked: bool  # 403/WAF
    retryable: bool  # 502/timeout (worth retry)


@dataclass
class TabsBatchResult:
    """Aggregate result of fetching all tabs for a process."""

    tabs: dict[str, str]
    blocked: bool
    retryable: bool
    tabs_failed: set[str]
