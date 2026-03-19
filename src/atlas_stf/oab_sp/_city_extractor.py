"""Extract city dropdown options from OAB/SP inscritos page."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import httpx

from ._config import OABSP_INSCRITOS_URL

logger = logging.getLogger(__name__)

_OPTION_RE = re.compile(
    r'<option\s+value=["\x27]?(\d+)["\x27]?[^>]*>([^<]+)</option>',
    re.IGNORECASE,
)


def extract_city_options(html: str) -> dict[str, str]:
    """Extract city name → id mapping from the select dropdown HTML.

    Returns dict like {"SÃO PAULO": "617", "CAMPINAS": "48", ...}.
    Excludes the "Todas" option (value=0).
    """
    options = _OPTION_RE.findall(html)
    result: dict[str, str] = {}
    for value, name in options:
        if value == "0":  # Skip "Todas"
            continue
        result[name.strip()] = value
    return result


def fetch_and_save_cities(output_dir: Path) -> dict[str, str]:
    """Fetch consulta01.asp and save city mapping to JSON."""
    logger.info("Fetching city dropdown from OAB/SP...")
    with httpx.Client(
        timeout=30,
        headers={"User-Agent": "AtlasSTF/1.0 (academic research)"},
        follow_redirects=True,
    ) as client:
        resp = client.get(OABSP_INSCRITOS_URL)
        resp.raise_for_status()

    cities = extract_city_options(resp.text)
    logger.info("Extracted %d cities from OAB/SP dropdown", len(cities))

    output_path = output_dir / "cidades_oabsp.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(cities, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    logger.info("Saved city mapping to %s", output_path)
    return cities


def load_cities(output_dir: Path) -> dict[str, str]:
    """Load previously saved city mapping."""
    path = output_dir / "cidades_oabsp.json"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return json.load(f)
