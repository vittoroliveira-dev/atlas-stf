"""Setup, orchestration, dedup/sort, and output writing for sanction corporate links."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_helpers import (
    build_counsel_process_map,
    build_entity_match_index,
    build_party_process_map,
)
from ._run_context import RunContext
from ._scl_bridge import (
    _build_stf_entity_index,
    _collect_sanction_cnpjs,
    _collect_sanction_tax_ids,
    _expand_cnpjs_via_groups,
    _stream_companies_for_cnpjs,
    _stream_establishments_for_cnpjs,
    _stream_partners_for_cnpjs,
)
from ._scl_loaders import load_decision_event_data, load_process_class_map
from ._scl_traversal import RED_FLAG_DELTA_THRESHOLD, traverse_sanctions

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/sanction_corporate_link.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/sanction_corporate_link_summary.schema.json")

__all__ = ["RED_FLAG_DELTA_THRESHOLD", "resolve_and_write"]


def resolve_and_write(
    *,
    sanctions: list[dict[str, Any]],
    rfb_dir: Path,
    curated_dir: Path,
    analytics_dir: Path,
    output_dir: Path,
    ctx: RunContext,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Resolve corporate routes for each sanction and write output JSONL + summary."""
    # Step 2: Collect seed CNPJs
    ctx.start_step(2, "SCL: Coletando CNPJs-seed das sanções...")
    seed_cnpjs = _collect_sanction_cnpjs(sanctions)
    seed_tax_ids = _collect_sanction_tax_ids(sanctions)
    ctx.save_checkpoint(
        "seeds_collected",
        counts={"seed_cnpjs": len(seed_cnpjs), "seed_tax_ids": len(seed_tax_ids)},
    )

    # Step 3: Stream partners (1st pass — seed CNPJs + sanction tax IDs)
    ctx.start_step(3, "SCL: Streaming partners (seed)...")
    partners_path = rfb_dir / "partners_raw.jsonl"
    partner_by_cnpj_basico, partner_by_doc = _stream_partners_for_cnpjs(
        partners_path, seed_cnpjs, target_tax_ids=seed_tax_ids
    )
    partner_count = sum(len(v) for v in partner_by_cnpj_basico.values())
    ctx.log_memory(f"partners (seed pass): {partner_count}")
    ctx.save_checkpoint("partners_seed_loaded", counts={"partners": partner_count})

    # Step 4: Expand via economic groups
    ctx.start_step(4, "SCL: Expandindo via grupos econômicos...")
    eg_path = analytics_dir / "economic_group.jsonl"
    expanded_cnpjs, eg_index = _expand_cnpjs_via_groups(eg_path, seed_cnpjs, ctx=ctx)
    # CNPJs discovered via path B/C (sanction appears as partner at some company): need full partner scan.
    cnpjs_from_doc_paths: set[str] = {
        rec.get("cnpj_basico", "") for recs in partner_by_doc.values() for rec in recs if rec.get("cnpj_basico", "")
    }
    # New CNPJs: from economic group expansion OR from doc-bridge paths (not already fully seeded).
    new_cnpjs = (expanded_cnpjs - seed_cnpjs) | (cnpjs_from_doc_paths - seed_cnpjs)
    ctx.log_memory(f"expansion: {len(expanded_cnpjs)} CNPJs total, {len(new_cnpjs)} new")
    ctx.save_checkpoint("expansion_done", counts={"expanded_cnpjs": len(expanded_cnpjs)})

    # Step 5: Stream partners (2nd pass — expanded CNPJs + doc-bridge CNPJs)
    ctx.start_step(5, "SCL: Streaming partners (expandido)...")
    if new_cnpjs:
        expanded_by_cnpj, expanded_by_doc = _stream_partners_for_cnpjs(partners_path, new_cnpjs)
        for cnpj, partners in expanded_by_cnpj.items():
            # Use update (replace) for doc-bridge CNPJs — first pass was partial (only one direction).
            if cnpj in cnpjs_from_doc_paths:
                partner_by_cnpj_basico[cnpj] = partners
            else:
                partner_by_cnpj_basico.setdefault(cnpj, []).extend(partners)
        for doc, partners in expanded_by_doc.items():
            partner_by_doc.setdefault(doc, []).extend(partners)
        ctx.log_memory(f"partners (expanded pass): {sum(len(v) for v in expanded_by_cnpj.values())}")
    ctx.save_checkpoint("partners_expanded_loaded")

    # Step 6: Stream companies/establishments
    ctx.start_step(6, "SCL: Streaming companies/establishments...")
    all_target_cnpjs = expanded_cnpjs | set(partner_by_cnpj_basico.keys())
    companies_path = rfb_dir / "companies_raw.jsonl"
    establishments_path = rfb_dir / "establishments_raw.jsonl"
    company_index = _stream_companies_for_cnpjs(companies_path, all_target_cnpjs)
    establishment_index = _stream_establishments_for_cnpjs(establishments_path, all_target_cnpjs)
    ctx.log_memory(f"companies: {len(company_index)}, establishments: {len(establishment_index)}")
    ctx.save_checkpoint(
        "companies_loaded",
        counts={"companies": len(company_index), "establishments": len(establishment_index)},
    )

    if not partner_by_doc and not company_index:
        logger.warning("No RFB data found in %s — skipping sanction corporate links", rfb_dir)
        ctx.finish(outputs=[])
        output_path = output_dir / "sanction_corporate_link.jsonl"
        with AtomicJsonlWriter(output_path) as _fh:
            pass
        return output_path

    # Step 7: Build STF entity index
    ctx.start_step(7, "SCL: Construindo índice STF...")
    combined_records, party_id_to_name, counsel_id_to_name = _build_stf_entity_index(curated_dir)
    stf_index = build_entity_match_index(combined_records, name_field="entity_name_normalized")
    ctx.log_memory(f"STF entities: {len(combined_records)}")

    # Step 8: Load process data
    ctx.start_step(8, "SCL: Carregando dados de processos...")
    process_path = curated_dir / "process.jsonl"
    decision_event_path = curated_dir / "decision_event.jsonl"
    process_party_link_path = curated_dir / "process_party_link.jsonl"
    process_counsel_link_path = curated_dir / "process_counsel_link.jsonl"

    party_process_map = build_party_process_map(process_party_link_path, party_id_to_name)
    counsel_process_map = build_counsel_process_map(process_counsel_link_path, counsel_id_to_name)

    # Single-pass reads: process.jsonl once (replaces build_process_class_map +
    # the process.jsonl read inside build_baseline_rates_stratified); then
    # decision_event.jsonl once (replaces build_process_outcomes,
    # build_baseline_rates_stratified decision pass, and build_process_jb_category_map).
    process_class_map = load_process_class_map(process_path)
    process_outcomes, stratified_rates, fallback_rates, process_jb_map = (
        load_decision_event_data(decision_event_path, process_class_map)
    )

    now_iso = datetime.now(timezone.utc).isoformat()

    # Step 9: Resolve corporate routes
    ctx.start_step(9, "SCL: Resolvendo rotas corporativas...", total_items=len(sanctions), unit="sanções")

    traversal = traverse_sanctions(
        sanctions=sanctions,
        partner_by_cnpj_basico=partner_by_cnpj_basico,
        partner_by_doc=partner_by_doc,
        company_index=company_index,
        establishment_index=establishment_index,
        eg_index=eg_index,
        stf_index=stf_index,
        party_process_map=party_process_map,
        counsel_process_map=counsel_process_map,
        process_outcomes=process_outcomes,
        stratified_rates=stratified_rates,
        fallback_rates=fallback_rates,
        process_class_map=process_class_map,
        process_jb_map=process_jb_map,
        now_iso=now_iso,
        ctx=ctx,
    )

    # Step 10: Dedup and sort
    ctx.start_step(10, "SCL: Deduplicando e ordenando...")
    # Post-dedup: for same (sanction_id, bridge_cnpj, stf_entity_type, stf_entity_id) but different
    # link_basis, keep shortest degree then highest stf_match_score; distinct routes are both kept.
    final_map: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for rec in traversal.output_records:
        key = (
            rec["sanction_id"],
            rec["bridge_company_cnpj_basico"],
            rec["stf_entity_type"],
            rec["stf_entity_id"],
        )
        existing = final_map.get(key)
        if existing is None:
            final_map[key] = rec
        elif rec["bridge_link_basis"] != existing["bridge_link_basis"]:
            # Different routes — keep both under composite key
            composite = (*key, rec["bridge_link_basis"])
            final_map[composite] = rec  # type: ignore[assignment]
        elif rec["link_degree"] < existing["link_degree"]:
            final_map[key] = rec
        elif rec["link_degree"] == existing["link_degree"]:
            rec_score = rec.get("stf_match_score") or 0.0
            ex_score = existing.get("stf_match_score") or 0.0
            if rec_score > ex_score:
                final_map[key] = rec

    final_records = sorted(
        final_map.values(),
        key=lambda r: (not r["red_flag"], -(r["risk_score"] or 0), r["link_id"]),
    )

    # Step 11: Write results
    ctx.start_step(11, "SCL: Gravando resultados...")
    validate_records(final_records, SCHEMA_PATH)
    output_path = output_dir / "sanction_corporate_link.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for rec in final_records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    red_flag_count = sum(1 for r in final_records if r["red_flag"])
    degree_counts: dict[int, int] = defaultdict(int)
    for r in final_records:
        degree_counts[int(r["link_degree"])] += 1

    summary: dict[str, Any] = {
        "total_links": len(final_records),
        "red_flag_count": red_flag_count,
        "sanctions_scanned": len(sanctions),
        "degree_counts": {str(k): v for k, v in sorted(degree_counts.items())},
        "source_counts": dict(Counter(r["sanction_source"] for r in final_records)),
        "truncated_sanctions": traversal.truncated_sanctions,
        "truncated_sanctions_count": len(traversal.truncated_sanctions),
        "cache_stats": {
            "hits": traversal.match_cache_hits,
            "misses": traversal.match_cache_misses,
            "hit_rate_pct": round(
                traversal.match_cache_hits * 100.0
                / max(traversal.match_cache_hits + traversal.match_cache_misses, 1),
                2,
            ),
        },
        "total_match_calls": traversal.match_cache_hits + traversal.match_cache_misses,
        "unique_groups_touched": len(traversal.all_groups_touched),
        "total_group_touches": traversal.total_group_touches,
        "total_partners_scanned": traversal.total_partners_scanned,
        "total_cnpjs_scanned": traversal.total_cnpjs_scanned,
        "generated_at": now_iso,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    summary_path = output_dir / "sanction_corporate_link_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    manifest = ctx.finish(outputs=[str(output_path)])
    logger.info(
        "Built %d sanction corporate links (%d red flags) from %d sanctions",
        len(final_records),
        red_flag_count,
        len(sanctions),
    )
    _ = manifest
    return output_path
