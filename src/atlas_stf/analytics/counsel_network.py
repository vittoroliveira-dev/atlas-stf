"""Build counsel network clustering: detect groups of associated counsel."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..core.rules import classify_outcome_raw
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_io import read_jsonl

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/counsel_network_cluster.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/counsel_network_cluster_summary.schema.json")
DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
MIN_SHARED_CLIENTS = 2
MIN_CLUSTER_CASES_FOR_FLAG = 5
RED_FLAG_DELTA_THRESHOLD = 0.15


def _build_counsel_client_graph(
    process_counsel_link_path: Path,
    process_party_link_path: Path,
) -> dict[str, set[str]]:
    """Map counsel_id -> set of party_ids (clients)."""
    # First, map process_id -> set of party_ids
    process_parties: dict[str, set[str]] = defaultdict(set)
    for record in read_jsonl(process_party_link_path):
        pid = record.get("process_id")
        party_id = record.get("party_id")
        if pid and party_id:
            process_parties[str(pid)].add(str(party_id))

    # Then, map counsel_id -> set of party_ids via shared processes
    counsel_clients: dict[str, set[str]] = defaultdict(set)
    for record in read_jsonl(process_counsel_link_path):
        cid = record.get("counsel_id")
        pid = record.get("process_id")
        if cid and pid:
            counsel_clients[str(cid)].update(process_parties.get(str(pid), set()))

    return dict(counsel_clients)


MAX_COUNSEL_PER_PARTY = 50  # Skip "hub" parties (UNIÃO, large orgs) to avoid O(n²) blowup


def _find_connected_components(
    counsel_clients: dict[str, set[str]],
    min_shared_clients: int = MIN_SHARED_CLIENTS,
) -> list[set[str]]:
    """Find clusters of counsel who share clients (connected components).

    Uses an inverted index (party→counsel) to count shared clients between
    counsel pairs. Parties with too many counsel (> MAX_COUNSEL_PER_PARTY)
    are skipped to avoid combinatorial explosion on hub entities.
    """
    adjacency: dict[str, set[str]] = defaultdict(set)
    counsel_ids = list(counsel_clients.keys())

    # Build inverted index: party_id -> set of counsel_ids
    party_counsels: dict[str, set[str]] = defaultdict(set)
    for cid, clients in counsel_clients.items():
        for party_id in clients:
            party_counsels[party_id].add(cid)

    # Build adjacency directly — skip hub parties that would create huge pair counts
    shared_count: dict[tuple[str, str], int] = Counter()
    for counsels in party_counsels.values():
        if len(counsels) > MAX_COUNSEL_PER_PARTY:
            continue
        counsel_list = sorted(counsels)
        for i, c1 in enumerate(counsel_list):
            for c2 in counsel_list[i + 1 :]:
                pair = (c1, c2)
                shared_count[pair] += 1
                if shared_count[pair] == min_shared_clients:
                    adjacency[c1].add(c2)
                    adjacency[c2].add(c1)

    # BFS to find connected components
    visited: set[str] = set()
    components: list[set[str]] = []
    for cid in counsel_ids:
        if cid in visited or cid not in adjacency:
            continue
        component: set[str] = set()
        queue = [cid]
        while queue:
            current = queue.pop()
            if current in visited:
                continue
            visited.add(current)
            component.add(current)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited:
                    queue.append(neighbor)
        if len(component) > 1:
            components.append(component)

    return components


def _compute_cluster_favorable_rate(
    cluster_counsel_ids: set[str],
    counsel_process_map: dict[str, list[str]],
    process_outcomes: dict[str, list[str]],
) -> tuple[float | None, int]:
    """Compute favorable rate for all processes handled by the cluster."""
    all_outcomes: list[str] = []
    all_pids: set[str] = set()
    for cid in cluster_counsel_ids:
        for pid in counsel_process_map.get(cid, []):
            if pid not in all_pids:
                all_pids.add(pid)
                all_outcomes.extend(process_outcomes.get(pid, []))

    favorable = 0
    classifiable = 0
    for outcome in all_outcomes:
        cls = classify_outcome_raw(outcome)
        if cls == "favorable":
            favorable += 1
            classifiable += 1
        elif cls == "unfavorable":
            classifiable += 1

    if classifiable == 0:
        return None, len(all_pids)
    return favorable / classifiable, len(all_pids)


def build_counsel_network(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    min_shared_clients: int = MIN_SHARED_CLIENTS,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build counsel network clusters: detect associated counsel groups."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if on_progress:
        on_progress(0, 4, "Counsel Network: Carregando dados...")

    pcl_path = curated_dir / "process_counsel_link.jsonl"
    ppl_path = curated_dir / "process_party_link.jsonl"
    de_path = curated_dir / "decision_event.jsonl"
    counsel_path = curated_dir / "counsel.jsonl"

    if not pcl_path.exists() or not ppl_path.exists():
        logger.warning("Counsel network skipped: required curated inputs missing")
        output_path = output_dir / "counsel_network_cluster.jsonl"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("", encoding="utf-8")
        return output_path

    counsel_names: dict[str, str] = {}
    for record in read_jsonl(counsel_path):
        cid = record.get("counsel_id")
        name = record.get("counsel_name_normalized") or record.get("counsel_name_raw")
        if cid and name:
            counsel_names[str(cid)] = str(name)

    counsel_clients = _build_counsel_client_graph(pcl_path, ppl_path)

    # Build counsel -> process and process -> outcomes maps
    counsel_process_map: dict[str, list[str]] = defaultdict(list)
    for record in read_jsonl(pcl_path):
        cid = record.get("counsel_id")
        pid = record.get("process_id")
        if cid and pid:
            counsel_process_map[str(cid)].append(str(pid))

    process_outcomes: dict[str, list[str]] = defaultdict(list)
    # Also build rapporteur map for per-minister analysis
    process_rapporteurs: dict[str, set[str]] = defaultdict(set)
    for record in read_jsonl(de_path):
        pid = record.get("process_id")
        progress = record.get("decision_progress")
        rapporteur = record.get("current_rapporteur")
        if pid and progress:
            process_outcomes[str(pid)].append(str(progress))
        if pid and rapporteur:
            process_rapporteurs[str(pid)].add(str(rapporteur))

    if on_progress:
        on_progress(1, 4, "Counsel Network: Clusterizando...")

    components = _find_connected_components(counsel_clients, min_shared_clients)

    if on_progress:
        on_progress(2, 4, "Counsel Network: Analisando clusters...")

    now_iso = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for cluster in components:
        # Compute cluster-level stats
        cluster_rate, cluster_case_count = _compute_cluster_favorable_rate(
            cluster, dict(counsel_process_map), dict(process_outcomes)
        )

        # Find ministers involved with this cluster
        cluster_ministers: set[str] = set()
        cluster_processes: set[str] = set()
        for cid in cluster:
            for pid in counsel_process_map.get(cid, []):
                cluster_processes.add(pid)
                cluster_ministers.update(process_rapporteurs.get(pid, set()))

        # Compute shared clients across the cluster
        all_clients: set[str] = set()
        for cid in cluster:
            all_clients.update(counsel_clients.get(cid, set()))

        red_flag = (
            cluster_rate is not None
            and cluster_rate > 0.5 + RED_FLAG_DELTA_THRESHOLD
            and cluster_case_count >= MIN_CLUSTER_CASES_FOR_FLAG
        )

        sorted_cluster = sorted(cluster)
        cluster_id = stable_id("cnet-", ":".join(sorted_cluster))
        records.append(
            {
                "cluster_id": cluster_id,
                "counsel_ids": sorted_cluster,
                "counsel_names": [counsel_names.get(cid, "") for cid in sorted_cluster],
                "cluster_size": len(cluster),
                "shared_client_count": len(all_clients),
                "shared_process_count": len(cluster_processes),
                "minister_names": sorted(cluster_ministers),
                "cluster_favorable_rate": (round(cluster_rate, 6) if cluster_rate is not None else None),
                "cluster_case_count": cluster_case_count,
                "red_flag": red_flag,
                "generated_at": now_iso,
            }
        )

    if on_progress:
        on_progress(3, 4, "Counsel Network: Gravando resultados...")

    validate_records(records, SCHEMA_PATH)

    output_path = output_dir / "counsel_network_cluster.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    red_flag_count = sum(1 for r in records if r["red_flag"])
    summary = {
        "generated_at": now_iso,
        "total_clusters": len(records),
        "red_flag_count": red_flag_count,
        "total_counsel_in_clusters": sum(r["cluster_size"] for r in records),
        "min_shared_clients": min_shared_clients,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "counsel_network_cluster_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Built counsel network clusters: %d clusters (%d red flags)",
        len(records),
        red_flag_count,
    )
    if on_progress:
        on_progress(4, 4, "Counsel Network: Concluído")
    return output_path
