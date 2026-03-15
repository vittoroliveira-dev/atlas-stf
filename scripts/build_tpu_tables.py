#!/usr/bin/env python3
"""Download TPU tables from PDPJ/CNJ API and generate reference JSON artifacts.

Usage:
    uv run python scripts/build_tpu_tables.py [--output-dir data/reference]

Downloads the official Tabelas Processuais Unificadas (classes, movements,
subjects) from the PDPJ REST API (gateway.cloud.pje.jus.br/tpu) and generates
versioned JSON files in the output directory.

Output files:
    tpu_classes.json    — {code: name} for process classes
    tpu_movements.json  — {code: name} for movements
    tpu_subjects.json   — {code: name} for subjects (hierarchical)
    tpu_version.json    — metadata: API version, generation date, source URLs,
                          SHA256 checksums of source responses and generated artifacts
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PDPJ TPU REST API — public endpoints, no authentication required
# Swagger: https://gateway.cloud.pje.jus.br/tpu/swagger-ui.html
# ---------------------------------------------------------------------------

PDPJ_BASE = "https://gateway.cloud.pje.jus.br/tpu/api/v1/publico"

DOWNLOAD_ENDPOINTS: dict[str, str] = {
    "classes": f"{PDPJ_BASE}/download/classes",
    "movimentos": f"{PDPJ_BASE}/download/movimentos",
    "assuntos": f"{PDPJ_BASE}/download/assuntos",
}


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _fetch_json(url: str, timeout: float = 60.0) -> tuple[list[dict[str, Any]], bytes]:
    """Fetch JSON array from PDPJ endpoint. Returns (parsed data, raw bytes)."""
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        resp = client.get(url, headers={"User-Agent": "Atlas-STF/1.0 (research)"})
        resp.raise_for_status()
        raw = resp.content
        data = resp.json()
        if not isinstance(data, list):
            msg = f"Expected JSON array from {url}, got {type(data).__name__}"
            raise ValueError(msg)
        return data, raw


def _extract_classes(records: list[dict[str, Any]]) -> dict[str, str]:
    """Extract {cod_item: nome} from ClasseDTO records."""
    result: dict[str, str] = {}
    for rec in records:
        code = rec.get("cod_item")
        name = rec.get("nome", "").strip()
        if code is not None and name:
            result[str(code)] = name
    return result


def _extract_movements(records: list[dict[str, Any]]) -> dict[str, str]:
    """Extract {id: nome} from MovimentoDTO records.

    Movements use 'id' as the unique identifier (not cod_item).
    """
    result: dict[str, str] = {}
    for rec in records:
        code = rec.get("id") or rec.get("cod_item")
        name = rec.get("nome", "").strip()
        if code is not None and name:
            result[str(code)] = name
    return result


def _extract_subjects(records: list[dict[str, Any]]) -> dict[str, str]:
    """Extract {cod_item: nome} from AssuntoDTO records."""
    result: dict[str, str] = {}
    for rec in records:
        code = rec.get("cod_item")
        name = rec.get("nome", "").strip()
        if code is not None and name:
            result[str(code)] = name
    return result


_EXTRACTORS: dict[str, Any] = {
    "classes": _extract_classes,
    "movimentos": _extract_movements,
    "assuntos": _extract_subjects,
}


def fetch_and_build(output_dir: Path, dry_run: bool = False) -> None:
    """Main entry point: fetch TPU tables from PDPJ API and generate JSON artifacts."""
    output_dir.mkdir(parents=True, exist_ok=True)

    source_urls: dict[str, str] = {}
    source_checksums: dict[str, str] = {}
    artifact_checksums: dict[str, str] = {}
    tables: dict[str, dict[str, str]] = {}

    for table_name, url in DOWNLOAD_ENDPOINTS.items():
        logger.info("Fetching %s from %s", table_name, url)
        if dry_run:
            logger.info("  [dry-run] Would fetch %s", url)
            tables[table_name] = {}
            continue

        try:
            records, raw_bytes = _fetch_json(url)
            source_checksums[table_name] = _sha256(raw_bytes)
            source_urls[table_name] = url

            extractor = _EXTRACTORS[table_name]
            extracted = extractor(records)
            logger.info("  Fetched %d records, extracted %d entries for %s",
                        len(records), len(extracted), table_name)
            tables[table_name] = extracted
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("  Failed to fetch %s: %s — using existing file", table_name, exc)
            file_mapping = {"classes": "tpu_classes.json", "movimentos": "tpu_movements.json",
                            "assuntos": "tpu_subjects.json"}
            existing_path = output_dir / file_mapping[table_name]
            if existing_path.exists():
                tables[table_name] = json.loads(existing_path.read_text(encoding="utf-8"))
            else:
                tables[table_name] = {}

    # Write output files
    file_mapping = {
        "classes": "tpu_classes.json",
        "movimentos": "tpu_movements.json",
        "assuntos": "tpu_subjects.json",
    }

    for table_name, filename in file_mapping.items():
        data = tables.get(table_name, {})
        content = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
        out_path = output_dir / filename
        if not dry_run:
            out_path.write_text(content + "\n", encoding="utf-8")
            artifact_checksums[filename] = _sha256(content.encode("utf-8"))
        logger.info("  Wrote %s (%d entries)", filename, len(data))

    # Write version metadata
    version_info = {
        "sgt_version": "pdpj-tpu-v1.4",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_urls": source_urls,
        "source_checksums": source_checksums,
        "artifact_checksums": artifact_checksums,
    }
    version_path = output_dir / "tpu_version.json"
    if not dry_run:
        version_path.write_text(
            json.dumps(version_info, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    logger.info("Version metadata written to %s", version_path)

    total = sum(len(t) for t in tables.values())
    logger.info("Done. Total entries: %d (classes=%d, movimentos=%d, assuntos=%d)",
                total,
                len(tables.get("classes", {})),
                len(tables.get("movimentos", {})),
                len(tables.get("assuntos", {})))


def main() -> None:
    parser = argparse.ArgumentParser(description="Download TPU tables from PDPJ/CNJ API")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/reference"),
        help="Output directory for JSON artifacts (default: data/reference)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making changes",
    )
    args = parser.parse_args()
    fetch_and_build(args.output_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
