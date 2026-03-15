"""Runner for selective document extraction to enrich representation edges."""

from __future__ import annotations

import logging
from typing import Any

from ..curated.common import read_jsonl_records
from ._config import DocExtractorConfig

logger = logging.getLogger(__name__)


def _filter_low_confidence_edges(
    edges: list[dict[str, Any]],
    threshold: float,
) -> list[dict[str, Any]]:
    """Return edges with confidence below the given threshold."""
    return [e for e in edges if (e.get("confidence") or 0) < threshold]


def run_doc_extraction(config: DocExtractorConfig) -> int:
    """Run selective document extraction to enrich representation edges.

    1. Read representation_edge.jsonl
    2. Filter edges with confidence < min_confidence_gap
    3. For each edge, check if PDF is available locally
    4. Extract fields -> create source_evidence records
    5. Update representation_edge with improved confidence
    6. Return count of enriched edges
    """
    edge_path = config.curated_dir / "representation_edge.jsonl"
    if not edge_path.exists():
        logger.warning("No representation_edge.jsonl found at %s", edge_path)
        return 0

    edges = read_jsonl_records(edge_path)
    low_confidence = _filter_low_confidence_edges(edges, config.min_confidence_gap)

    if not low_confidence:
        logger.info("No low-confidence edges to enrich")
        return 0

    if config.max_documents is not None:
        low_confidence = low_confidence[: config.max_documents]

    logger.info(
        "Found %d edges below confidence threshold %.2f",
        len(low_confidence),
        config.min_confidence_gap,
    )

    # For now, this is a placeholder -- actual PDF download would need
    # access to STF document URLs from representation_event records
    enriched_count = 0

    logger.info("Document extraction complete: %d edges enriched", enriched_count)
    return enriched_count
