#!/usr/bin/env python3
"""Probe STF portal HTML structure for representation data extraction.

One-shot script (NOT part of the pipeline) — run manually:
    python scripts/probe_stf_portal.py
Outputs results to scripts/probe_results/ (gitignored).
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

PORTAL_BASE = "https://portal.stf.jus.br"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"

SAMPLE_PROCESSES = [  # (class, incidente_id) — representative sample
    ("ADI", "2130253"),
    ("ADPF", "4922027"),
    ("RE", "5355091"),
    ("HC", "2130710"),
    ("MS", "2130254"),
]

TABS = {
    "informacoes": "",
    "partes": "aba_partes",
    "andamentos": "aba_andamentos",
    "peticoes": "aba_peticoes",
}

OUTPUT_DIR = Path("scripts/probe_results")


def fetch_tab(client: httpx.Client, incidente: str, tab_param: str) -> str | None:
    """Fetch a specific tab page for a process."""
    url = f"{PORTAL_BASE}/processos/detalhe.asp"
    params: dict[str, str] = {"incidente": incidente}
    if tab_param:
        params["aba"] = tab_param

    try:
        time.sleep(2.0)  # Rate limit
        resp = client.get(url, params=params)
        resp.raise_for_status()
        return resp.text
    except httpx.HTTPError as exc:
        logger.warning("Failed to fetch %s tab=%s: %s", incidente, tab_param, exc)
        return None


def analyze_partes_structure(html: str) -> dict[str, object]:
    """Analyze the Partes tab HTML structure for representation data."""
    oab_patterns = re.findall(r"OAB[/\s]*[A-Z]{2}\s*\d+", html)
    return {
        "has_table": bool(re.search(r"<table", html, re.IGNORECASE)),
        "has_advogado": bool(re.search(r"advogad[oa]", html, re.IGNORECASE)),
        "has_oab": bool(re.search(r"OAB", html, re.IGNORECASE)),
        "has_procurador": bool(re.search(r"procurador", html, re.IGNORECASE)),
        "has_escritorio": bool(re.search(r"escrit[oó]rio", html, re.IGNORECASE)),
        "party_divs": len(re.findall(r"<div[^>]*class=['\"][^'\"]*parte[^'\"]*['\"]", html, re.IGNORECASE)),
        "lawyer_mentions": len(re.findall(r"advogad[oa]", html, re.IGNORECASE)),
        "oab_mentions": len(re.findall(r"OAB[/\s]", html)),
        "sample_oab_patterns": oab_patterns[:5],
    }


def analyze_peticoes_structure(html: str) -> dict[str, object]:
    """Analyze Peticoes tab structure."""
    return {
        "has_table": bool(re.search(r"<table", html, re.IGNORECASE)),
        "has_peticionario": bool(re.search(r"peticion[aá]rio", html, re.IGNORECASE)),
        "row_count": len(re.findall(r"<tr", html, re.IGNORECASE)),
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, object]] = []

    with httpx.Client(
        timeout=30.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        for process_class, incidente in SAMPLE_PROCESSES:
            logger.info("Probing %s (incidente=%s)...", process_class, incidente)

            process_result: dict[str, object] = {
                "class": process_class,
                "incidente": incidente,
                "tabs": {},
            }
            tabs_dict: dict[str, dict[str, object]] = {}

            for tab_name, tab_param in TABS.items():
                html = fetch_tab(client, incidente, tab_param)
                if html is None:
                    tabs_dict[tab_name] = {"status": "failed"}
                    continue

                html_path = OUTPUT_DIR / f"{process_class}_{incidente}_{tab_name}.html"
                html_path.write_text(html, encoding="utf-8")

                if tab_name == "partes":
                    analysis = analyze_partes_structure(html)
                elif tab_name == "peticoes":
                    analysis = analyze_peticoes_structure(html)
                else:
                    analysis = {
                        "html_length": len(html),
                        "has_table": bool(re.search(r"<table", html, re.IGNORECASE)),
                    }

                tabs_dict[tab_name] = {"status": "ok", "html_length": len(html), **analysis}

            process_result["tabs"] = tabs_dict
            results.append(process_result)

    report_path = OUTPUT_DIR / "probe_report.json"
    report_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Probe complete. Report at %s | HTML files in %s", report_path, OUTPUT_DIR)

    for r in results:
        tabs = r.get("tabs")
        p = tabs.get("partes", {}) if isinstance(tabs, dict) else {}
        logger.info(
            "%s: partes=%s advogado=%s oab=%s",
            r["class"],
            p.get("status", "?"),
            p.get("has_advogado", "?"),
            p.get("has_oab", "?"),
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
