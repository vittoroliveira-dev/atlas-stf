"""Extract lawyer names from individual firm names (Sociedade Individual de Advocacia)."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Suffixes to strip from firm name to get lawyer name
_FIRM_SUFFIXES = re.compile(
    r"\s*-?\s*(?:SOCIEDADE\s+INDIVIDUAL\s+DE\s+ADVOCACIA|"
    r"SOCIEDADE\s+DE\s+ADVOGADOS?|"
    r"ADVOGADOS?\s+ASSOCIADOS?|"
    r"ADVOGADOS?|"
    r"S/?S|"
    r"LTDA\.?|"
    r"ME|"
    r"EPP|"
    r"EIRELI)"
    r"\s*$",
    re.IGNORECASE,
)

# Minimum name length to consider valid
_MIN_NAME_LENGTH = 5


def extract_lawyer_name_from_firm(firm_name: str) -> str | None:
    """Extract lawyer name from individual firm name.

    Example: "JOÃO DA SILVA SOCIEDADE INDIVIDUAL DE ADVOCACIA" → "JOÃO DA SILVA"
    Returns None if the result doesn't look like a person name.
    """
    cleaned = _FIRM_SUFFIXES.sub("", firm_name).strip()
    cleaned = cleaned.rstrip(" -").strip()

    if len(cleaned) < _MIN_NAME_LENGTH:
        return None
    # Must have at least two words (first + last name)
    if " " not in cleaned:
        return None
    return cleaned


def build_lookup_candidates(
    oab_sp_dir: Path,
    city_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Build lookup candidates from individual firms in sociedade_detalhe.jsonl.

    Only includes firms with society_type='individual'.
    Returns list of {name, city_id, cnsa_number, firm_name_original}.
    """
    detalhe_path = oab_sp_dir / "sociedade_detalhe.jsonl"
    if not detalhe_path.exists():
        logger.warning("sociedade_detalhe.jsonl not found at %s", detalhe_path)
        return []

    candidates: list[dict[str, Any]] = []
    skipped = 0

    with detalhe_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("society_type") != "individual":
                continue

            firm_name = record.get("firm_name", "")
            lawyer_name = extract_lawyer_name_from_firm(firm_name)
            if not lawyer_name:
                skipped += 1
                continue

            city = record.get("city", "")
            city_id = "0"  # Default: all cities
            if city:
                # Try exact match first (uppercase)
                city_upper = city.upper().strip()
                city_id = city_map.get(city_upper, "0")

            candidates.append(
                {
                    "lawyer_name": lawyer_name,
                    "city_id": city_id,
                    "city_name": city,
                    "registration_number": record.get("registration_number", ""),
                    "firm_name_original": firm_name,
                }
            )

    logger.info(
        "Built %d lookup candidates from individual firms (%d skipped)",
        len(candidates),
        skipped,
    )
    return candidates


def save_candidates(candidates: list[dict[str, Any]], output_dir: Path) -> Path:
    """Save candidates to JSONL."""
    path = output_dir / "candidatos_nome.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for c in candidates:
            fh.write(json.dumps(c, ensure_ascii=False))
            fh.write("\n")
    logger.info("Saved %d candidates to %s", len(candidates), path)
    return path
