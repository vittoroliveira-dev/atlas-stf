"""DataJud fetch runner: reads curated processes, queries API, writes raw JSON.

Public interface (used by fetch adapter):
- ``discover_indices(process_path)`` — discover DataJud index names
- ``fetch_single_index(client, index, output_dir)`` — query one index, write JSON

No checkpoint logic — manifest store is the single source of truth.
"""

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


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def discover_indices(process_path: Path) -> list[str]:
    """Read process.jsonl and discover unique DataJud API index names.

    Returns a sorted, deduplicated list of index names like
    ``["api_publica_tjsp", "api_publica_trf3", ...]``.
    """
    seen_pairs: set[tuple[str, str]] = set()
    with process_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            court = record.get("origin_court_or_body") or ""
            state = record.get("origin_description") or ""
            if court or state:
                seen_pairs.add((court, state))

    all_indices: set[str] = set()
    for court, state in seen_pairs:
        indices = map_origin_to_datajud_indices(court or None, state or None)
        all_indices.update(indices)

    return sorted(all_indices)


def fetch_single_index(client: DatajudClient, index: str, output_dir: Path) -> dict[str, Any]:
    """Query aggregated stats for one DataJud index and write result JSON.

    Returns the result dict (also written to ``output_dir/{index}.json``).
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    total_resp = client.search(index, build_total_query())
    total = extract_total(total_resp)

    assunto_resp = client.search(index, build_assunto_aggregation())
    top_assuntos = extract_aggregation_buckets(assunto_resp, "top_assuntos")

    orgao_resp = client.search(index, build_orgao_julgador_aggregation())
    top_orgaos = extract_aggregation_buckets(orgao_resp, "top_orgaos")

    class_resp = client.search(index, build_class_aggregation())
    classes = extract_aggregation_buckets(class_resp, "classes")

    result: dict[str, Any] = {
        "index": index,
        "tribunal_label": index_to_tribunal_label(index),
        "total_processes": total,
        "top_assuntos": top_assuntos,
        "top_orgaos_julgadores": top_orgaos,
        "class_distribution": classes,
    }

    out_path = output_dir / f"{index}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Completed index %s: %d processes", index, total)
    return result


# ---------------------------------------------------------------------------
# Legacy CLI entry point (preserved for `atlas-stf datajud fetch`)
# ---------------------------------------------------------------------------


def fetch_origin_data(config: DatajudFetchConfig) -> Path:
    """Fetch aggregated data from DataJud for each discovered origin index.

    Uses the manifest store for tracking. Falls back to full run
    (no checkpoint) — each re-run re-queries all indices unless the
    caller uses the fetch engine plan/execute cycle.
    """
    from ..core.fetch_lock import FetchLock

    config.output_dir.mkdir(parents=True, exist_ok=True)

    indices = discover_indices(config.process_path)
    logger.info("Discovered %d DataJud indices from %s", len(indices), config.process_path)

    if config.dry_run:
        for index in indices:
            logger.info("[dry-run] Would query index: %s", index)
        return config.output_dir

    with FetchLock(config.output_dir, "datajud"):
        _fetch_datajud_locked(config, indices)

    return config.output_dir


def _fetch_datajud_locked(config: DatajudFetchConfig, indices: list[str]) -> None:
    """Inner implementation guarded by FetchLock."""
    from ..fetch._manifest_model import FetchUnit, RemoteState, SourceManifest, build_unit_id
    from ..fetch._manifest_store import load_manifest, write_manifest_unlocked

    manifest = load_manifest("datajud", config.output_dir) or SourceManifest(source="datajud")

    with DatajudClient(
        config.api_key,
        timeout=config.timeout_seconds,
        rate_limit=config.rate_limit_seconds,
        max_retries=config.max_retries,
    ) as client:
        for index in indices:
            uid = build_unit_id("datajud", index.lower().replace(".", "_"))
            existing = manifest.units.get(uid)
            if existing and existing.status == "committed":
                logger.info("Skipping already committed index: %s", index)
                continue

            logger.info("Querying index: %s", index)
            result = fetch_single_index(client, index, config.output_dir)

            manifest.units[uid] = FetchUnit(
                unit_id=uid,
                source="datajud",
                label=f"DataJud {index}",
                remote_url="",
                remote_state=RemoteState(url=""),
                local_path=str(config.output_dir / f"{index}.json"),
                status="committed",
                metadata={"index": index},
                published_record_count=result.get("total_processes", 0),
            )

    write_manifest_unlocked(manifest, config.output_dir)
    logger.info("DataJud fetch complete: %d indices committed", len(manifest.units))
