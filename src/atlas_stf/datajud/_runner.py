"""DataJud fetch runner: reads curated processes, queries API, writes raw JSON."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ..core.origin_mapping import index_to_tribunal_label, map_origin_to_datajud_indices
from ._client import DatajudClient
from ._config import DatajudFetchConfig
from ._queries import (
    build_assunto_aggregation,
    build_class_aggregation,
    build_orgao_julgador_aggregation,
    build_total_query,
    extract_aggregation_buckets,
    extract_total,
)

logger = logging.getLogger(__name__)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _discover_indices(process_path: Path) -> list[str]:
    """Read process.jsonl and discover unique DataJud indices."""
    seen_pairs: set[tuple[str, str]] = set()
    for record in _read_jsonl(process_path):
        court = record.get("origin_court_or_body") or ""
        state = record.get("origin_description") or ""
        if court or state:
            seen_pairs.add((court, state))

    all_indices: set[str] = set()
    for court, state in seen_pairs:
        indices = map_origin_to_datajud_indices(court or None, state or None)
        all_indices.update(indices)

    return sorted(all_indices)


def _load_checkpoint(output_dir: Path) -> set[str]:
    checkpoint_path = output_dir / "_checkpoint.json"
    if checkpoint_path.exists():
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return set(data.get("completed", []))
    return set()


def _save_checkpoint(output_dir: Path, completed: set[str]) -> None:
    checkpoint_path = output_dir / "_checkpoint.json"
    checkpoint_path.write_text(
        json.dumps({"completed": sorted(completed)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def fetch_origin_data(config: DatajudFetchConfig) -> Path:
    """Fetch aggregated data from DataJud for each discovered origin index.

    Returns the output directory path.
    """
    config.output_dir.mkdir(parents=True, exist_ok=True)

    indices = _discover_indices(config.process_path)
    logger.info("Discovered %d DataJud indices from %s", len(indices), config.process_path)

    if config.dry_run:
        for index in indices:
            logger.info("[dry-run] Would query index: %s", index)
        return config.output_dir

    completed = _load_checkpoint(config.output_dir)

    with DatajudClient(
        config.api_key,
        timeout=config.timeout_seconds,
        rate_limit=config.rate_limit_seconds,
        max_retries=config.max_retries,
    ) as client:
        for index in indices:
            if index in completed:
                logger.info("Skipping already completed index: %s", index)
                continue

            logger.info("Querying index: %s", index)

            total_resp = client.search(index, build_total_query())
            total = extract_total(total_resp)

            assunto_resp = client.search(index, build_assunto_aggregation())
            top_assuntos = extract_aggregation_buckets(assunto_resp, "top_assuntos")

            orgao_resp = client.search(index, build_orgao_julgador_aggregation())
            top_orgaos = extract_aggregation_buckets(orgao_resp, "top_orgaos")

            class_resp = client.search(index, build_class_aggregation())
            classes = extract_aggregation_buckets(class_resp, "classes")

            result = {
                "index": index,
                "tribunal_label": index_to_tribunal_label(index),
                "total_processes": total,
                "top_assuntos": top_assuntos,
                "top_orgaos_julgadores": top_orgaos,
                "class_distribution": classes,
            }

            out_path = config.output_dir / f"{index}.json"
            out_path.write_text(
                json.dumps(result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            completed.add(index)
            _save_checkpoint(config.output_dir, completed)
            logger.info("Completed index %s: %d processes", index, total)

    logger.info("DataJud fetch complete: %d indices", len(completed))
    return config.output_dir
