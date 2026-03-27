"""Build law firm cluster analytics from curated representation data."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..curated.common import read_jsonl_records, write_jsonl
from ..schema_validate import validate_records

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_OUTPUT_DIR = Path("data/analytics")
SCHEMA_PATH = Path("schemas/firm_cluster.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/firm_cluster_summary.schema.json")

# Minimum shared parties to form a cluster
MIN_SHARED_PARTIES = 2


def _union_find_root(parent: dict[str, str], node: str) -> str:
    """Find root with path compression."""
    while parent[node] != node:
        parent[node] = parent.get(parent[node], parent[node])
        node = parent[node]
    return node


def _union_find_merge(parent: dict[str, str], rank: dict[str, int], a: str, b: str) -> None:
    """Merge two sets by rank."""
    ra = _union_find_root(parent, a)
    rb = _union_find_root(parent, b)
    if ra == rb:
        return
    if rank[ra] < rank[rb]:
        ra, rb = rb, ra
    parent[rb] = ra
    if rank[ra] == rank[rb]:
        rank[ra] += 1


def build_firm_cluster(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build law firm cluster analytics.

    Identifies firms that orbit same economic groups/parties.
    Uses Union-Find to cluster firms that share parties above threshold.

    Output fields per record:
    - cluster_id, firm_ids, firm_names, shared_party_count
    - shared_process_count, process_classes, cluster_size
    """
    total = 5
    step = 0

    def tick(desc: str) -> None:
        nonlocal step
        if on_progress:
            on_progress(step, total, desc)
        step += 1

    tick("Cluster: Carregando entidades...")

    firm_path = curated_dir / "law_firm_entity.jsonl"
    edge_path = curated_dir / "representation_edge.jsonl"
    party_path = curated_dir / "party.jsonl"
    process_path = curated_dir / "process.jsonl"

    firms = read_jsonl_records(firm_path) if firm_path.exists() else []
    edges = read_jsonl_records(edge_path) if edge_path.exists() else []
    parties = read_jsonl_records(party_path) if party_path.exists() else []
    processes = read_jsonl_records(process_path) if process_path.exists() else []

    tick("Cluster: Indexando dados...")

    firm_lookup: dict[str, str] = {}
    for rec in firms:
        fid = rec.get("firm_id")
        name = rec.get("firm_name_normalized") or rec.get("firm_name_raw", "")
        if fid:
            firm_lookup[fid] = name

    party_lookup: dict[str, str] = {}
    for rec in parties:
        pid = rec.get("party_id")
        name = rec.get("party_name_normalized") or rec.get("party_name_raw", "")
        if pid:
            party_lookup[pid] = name

    process_class_map: dict[str, str] = {}
    for rec in processes:
        pid = rec.get("process_id")
        pc = rec.get("process_class")
        if pid and pc:
            process_class_map[pid] = pc

    # Build firm -> lawyer mapping from firm entities
    firm_lawyers: dict[str, set[str]] = defaultdict(set)
    for rec in firms:
        fid = rec.get("firm_id")
        if not fid:
            continue
        for lid in rec.get("member_lawyer_ids", []):
            firm_lawyers[fid].add(lid)

    tick("Cluster: Mapeando escritorios a partes...")

    # Map firm_id -> set of party_ids (via edges for firm directly or via member lawyers)
    firm_parties: dict[str, set[str]] = defaultdict(set)
    firm_processes: dict[str, set[str]] = defaultdict(set)

    # Direct firm edges
    for edge in edges:
        fid = edge.get("firm_id")
        party_id = edge.get("party_id")
        process_id = edge.get("process_id", "")
        if fid and fid in firm_lookup:
            if party_id:
                firm_parties[fid].add(party_id)
            if process_id:
                firm_processes[fid].add(process_id)

    # Also attribute via member lawyers
    lawyer_to_firms: dict[str, set[str]] = defaultdict(set)
    for fid, lawyer_ids in firm_lawyers.items():
        for lid in lawyer_ids:
            lawyer_to_firms[lid].add(fid)

    for edge in edges:
        lid = edge.get("lawyer_id")
        if not lid:
            continue
        for fid in lawyer_to_firms.get(lid, set()):
            party_id = edge.get("party_id")
            process_id = edge.get("process_id", "")
            if party_id:
                firm_parties[fid].add(party_id)
            if process_id:
                firm_processes[fid].add(process_id)

    tick("Cluster: Agrupando escritorios por Union-Find...")

    # Build Union-Find: merge firms sharing >= MIN_SHARED_PARTIES parties
    all_firm_ids = list(firm_parties.keys())
    parent: dict[str, str] = {fid: fid for fid in all_firm_ids}
    rank: dict[str, int] = {fid: 0 for fid in all_firm_ids}

    # Invert: party_id -> set of firm_ids
    party_firms: dict[str, set[str]] = defaultdict(set)
    for fid, pids in firm_parties.items():
        for pid in pids:
            party_firms[pid].add(fid)

    # For each pair of firms sharing a party, count shared parties
    firm_pair_shared: dict[tuple[str, str], int] = defaultdict(int)
    for pid, fids in party_firms.items():
        fid_list = sorted(fids)
        for i in range(len(fid_list)):
            for j in range(i + 1, len(fid_list)):
                pair = (fid_list[i], fid_list[j])
                firm_pair_shared[pair] += 1

    # Merge pairs meeting threshold
    for (fid_a, fid_b), shared_count in firm_pair_shared.items():
        if shared_count >= MIN_SHARED_PARTIES:
            _union_find_merge(parent, rank, fid_a, fid_b)

    # Collect clusters
    cluster_members: dict[str, list[str]] = defaultdict(list)
    for fid in all_firm_ids:
        root = _union_find_root(parent, fid)
        cluster_members[root].append(fid)

    tick("Cluster: Montando registros...")

    timestamp = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for root, member_ids in cluster_members.items():
        if len(member_ids) < 2:
            continue

        # Aggregate stats across cluster
        cluster_party_ids: set[str] = set()
        cluster_process_ids: set[str] = set()
        class_counts: dict[str, int] = defaultdict(int)

        for fid in member_ids:
            cluster_party_ids.update(firm_parties.get(fid, set()))
            cluster_process_ids.update(firm_processes.get(fid, set()))

        for pid in cluster_process_ids:
            pc = process_class_map.get(pid)
            if pc:
                class_counts[pc] += 1

        cluster_id = stable_id("fcl_", ":".join(sorted(member_ids)))
        records.append(
            {
                "cluster_id": cluster_id,
                "firm_ids": sorted(member_ids),
                "firm_names": [firm_lookup.get(fid, "") for fid in sorted(member_ids)],
                "shared_party_count": len(cluster_party_ids),
                "shared_process_count": len(cluster_process_ids),
                "process_classes": dict(class_counts),
                "cluster_size": len(member_ids),
                "generated_at": timestamp,
            }
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    validate_records(records, SCHEMA_PATH)
    output_path = write_jsonl(records, output_dir / "firm_cluster.jsonl")

    summary: dict[str, Any] = {
        "total_clusters": len(records),
        "total_firms_in_clusters": sum(r["cluster_size"] for r in records),
        "total_firms_analyzed": len(all_firm_ids),
        "max_cluster_size": max((r["cluster_size"] for r in records), default=0),
        "generated_at": timestamp,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "firm_cluster_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    tick("Cluster: Concluido")
    logger.info(
        "Firm cluster: %d clusters written to %s",
        len(records),
        output_path,
    )

    return output_path
