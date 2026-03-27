"""Per-sanction corporate route traversal for sanction corporate links (Step 9)."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from ..core.identity import normalize_tax_id, stable_id
from ..core.stats import red_flag_confidence_label, red_flag_power
from ._match_helpers import (
    compute_favorable_rate_role_aware,
    lookup_baseline_rate,
    match_entity_record,
)
from ._match_helpers import degree_decay as _degree_decay
from ._run_context import RunContext
from ._scl_bridge import _compute_modal_class_jb, _record_hash
from ._scl_record_builder import _build_evidence_chain, _build_link_record
from .donor_corporate_link import _classify_document

logger = logging.getLogger(__name__)

RED_FLAG_DELTA_THRESHOLD = 0.15
MIN_CASES_FOR_RED_FLAG = 3
_MAX_CNPJS_PER_SANCTION = 5000


@dataclass
class TraversalResult:
    """Aggregated output of a full sanctions traversal pass."""

    output_records: list[dict[str, Any]] = field(default_factory=list)
    truncated_sanctions: list[dict[str, str]] = field(default_factory=list)
    match_cache_hits: int = 0
    match_cache_misses: int = 0
    all_groups_touched: set[str] = field(default_factory=set)
    total_group_touches: int = 0
    total_partners_scanned: int = 0
    total_cnpjs_scanned: int = 0


def traverse_sanctions(
    *,
    sanctions: list[dict[str, Any]],
    partner_by_cnpj_basico: dict[str, list[dict[str, Any]]],
    partner_by_doc: dict[str, list[dict[str, Any]]],
    company_index: dict[str, dict[str, Any]],
    establishment_index: dict[str, dict[str, Any]],
    eg_index: dict[str, dict[str, Any]],
    stf_index: Any,
    party_process_map: dict[str, list[tuple[str, str | None]]],
    counsel_process_map: dict[str, list[tuple[str, str | None]]],
    process_outcomes: dict[str, list[str]],
    stratified_rates: dict[tuple[str, str], float],
    fallback_rates: dict[str, float],
    process_class_map: dict[str, str],
    process_jb_map: dict[str, str],
    now_iso: str,
    ctx: RunContext,
) -> TraversalResult:
    """Iterate all sanctions, resolve corporate routes, and return raw output records."""
    result = TraversalResult()
    seen_keys: set[tuple[str, str, str, str, str]] = set()

    _match_cache: dict[tuple[str, str], Any] = {}
    sanctions_processed: int = 0
    _last_log_mono = time.monotonic()

    def _maybe_log_by_time() -> None:
        nonlocal _last_log_mono
        now = time.monotonic()
        if (now - _last_log_mono) < 30.0:
            return
        _last_log_mono = now
        logger.info(
            "build_sanction_corporate_links: sanctions=%d/%d records=%d "
            "cache_hits=%d cache_misses=%d groups_unique=%d truncated=%d",
            sanctions_processed,
            len(sanctions),
            len(result.output_records),
            result.match_cache_hits,
            result.match_cache_misses,
            len(result.all_groups_touched),
            len(result.truncated_sanctions),
        )

    for sanction in sanctions:
        sanction_start = time.monotonic()
        sanction_id = str(sanction.get("sanction_id", ""))
        raw_doc = str(sanction.get("entity_cnpj_cpf", "") or sanction.get("cpf_cnpj_sancionado", "") or "")
        sanction_entity_name = str(sanction.get("entity_name", "") or sanction.get("razao_social", "") or "")
        sanction_source = str(
            sanction.get("sanction_source", "") or sanction.get("sanction_source_origin", "") or ""
        )
        sanction_type = str(sanction.get("sanction_type", "") or sanction.get("tipo_sancao", "") or "")

        doc_type, tax_id_normalized, tax_id_valid, cnpj_basico = _classify_document(raw_doc)
        if not tax_id_valid or not tax_id_normalized:
            ctx.advance(1, current_item=sanction_entity_name)
            continue

        # Collect bridge companies: (cnpj_basico, link_basis, partner_record_or_none)
        bridge_companies: list[tuple[str, str, dict[str, Any] | None]] = []

        # Path A: CNPJ → direct company
        if doc_type == "cnpj" and cnpj_basico:
            if cnpj_basico in company_index or cnpj_basico in partner_by_cnpj_basico:
                bridge_companies.append((cnpj_basico, "exact_cnpj_basico", None))

        # Path B: CNPJ appears as partner_cpf_cnpj (PJ partner)
        if doc_type == "cnpj" and tax_id_normalized:
            for partner in partner_by_doc.get(tax_id_normalized, []):
                cb = partner.get("cnpj_basico", "")
                if cb:
                    bridge_companies.append((cb, "exact_partner_cnpj", partner))

        # Path C: CPF appears as partner_cpf_cnpj (PF partner)
        if doc_type == "cpf" and tax_id_normalized:
            for partner in partner_by_doc.get(tax_id_normalized, []):
                cb = partner.get("cnpj_basico", "")
                if cb:
                    bridge_companies.append((cb, "exact_partner_cpf", partner))

        # --- Deduplicate scan universe across all bridges for this sanction ---
        #
        # Structure:
        #   Phase 1: collect bridge metas + degree-2 CNPJs.
        #   Phase 2: estimate degree-3 universe size. If > budget, truncate BEFORE expanding.
        #   Phase 3: expand degree-3 only if not truncated.
        #   Phase 4: build cnpj_to_bridges map (deduplicated by bridge identity).

        # Each bridge meta: (link_basis, bridge_cnpj, bridge_partner_record, bridge_company_record)
        all_bridge_metas: list[tuple[str, str, dict[str, Any] | None, dict[str, Any]]] = []
        unified_cnpjs: dict[str, int] = {}  # cnpj -> min degree
        seen_group_ids: set[str] = set()
        group_to_bridge_indices: dict[str, list[int]] = {}

        # Phase 1: degree-2 bridges
        for bridge_cnpj, link_basis, bridge_partner in bridge_companies:
            company = company_index.get(bridge_cnpj, {})
            meta_idx = len(all_bridge_metas)
            all_bridge_metas.append((link_basis, bridge_cnpj, bridge_partner, company))

            if bridge_cnpj not in unified_cnpjs or unified_cnpjs[bridge_cnpj] > 2:
                unified_cnpjs[bridge_cnpj] = 2

            eg = eg_index.get(bridge_cnpj)
            if eg:
                gid = eg.get("group_id", "")
                group_to_bridge_indices.setdefault(gid, []).append(meta_idx)
                seen_group_ids.add(gid)

        result.all_groups_touched.update(seen_group_ids)
        result.total_group_touches += len(seen_group_ids)
        ctx.pulse(f"{sanction_entity_name}: bridges coletadas", extra={
            "current_phase": "bridges",
            "current_sanction": sanction_entity_name,
            "bridge_companies_found": len(bridge_companies),
            "unique_groups_touched": len(seen_group_ids),
        })

        # TECH DEBT: Componente conectado (RFB QSA) ≠ grupo econômico para inferência.
        # O circuit breaker é guardrail operacional, não threshold semântico.
        # Trabalho futuro: travessia que pesa tipos de aresta (controle vs. minoritário).

        # Phase 2: estimate degree-3 universe size BEFORE expanding
        estimated_degree3 = 0
        for gid in seen_group_ids:
            sample_idx = group_to_bridge_indices[gid][0]
            sample_bridge_cnpj = all_bridge_metas[sample_idx][1]
            eg_rec = eg_index.get(sample_bridge_cnpj)
            if eg_rec:
                estimated_degree3 += len(eg_rec.get("member_cnpjs", []))

        # Upper bound: may overcount due to overlap between degree-2 bridges and
        # group members, but safe for circuit breaker (truncates conservatively).
        pre_truncation_count = len(unified_cnpjs) + estimated_degree3
        truncated = False
        if pre_truncation_count > _MAX_CNPJS_PER_SANCTION:
            result.truncated_sanctions.append({
                "sanction_id": sanction_id or f"{sanction_entity_name}:{raw_doc}",
                "entity_name": sanction_entity_name,
            })
            logger.warning(
                "SCL: Truncating scan for %s (estimated %d CNPJs > %d limit, skipping group expansion)",
                sanction_entity_name,
                pre_truncation_count,
                _MAX_CNPJS_PER_SANCTION,
            )
            truncated = True
            # Do NOT expand degree-3 — keep only degree-2 bridges

        ctx.pulse(f"{sanction_entity_name}: estimativa degree-3", extra={
            "current_phase": "estimate",
            "current_sanction": sanction_entity_name,
            "estimated_degree3_count": estimated_degree3,
            "unique_cnpjs_to_scan": len(unified_cnpjs),
            "truncated": truncated,
        })

        # Phase 3: expand degree-3 only if not truncated
        if not truncated:
            for gid in seen_group_ids:
                sample_idx = group_to_bridge_indices[gid][0]
                sample_bridge_cnpj = all_bridge_metas[sample_idx][1]
                eg_rec = eg_index.get(sample_bridge_cnpj)
                if eg_rec:
                    for member_cnpj in eg_rec.get("member_cnpjs", []):
                        if member_cnpj not in unified_cnpjs:
                            unified_cnpjs[member_cnpj] = 3

        # Phase 4: build cnpj_to_bridges map
        # Degree-2: each bridge CNPJ maps to its own bridge metas.
        # Degree-3: each group member maps to ALL bridges whose group contains it.
        # Dedup by (link_basis, bridge_cnpj) to avoid duplicate bridge entries.
        cnpj_to_bridges: dict[str, list[tuple[str, str, dict[str, Any] | None, dict[str, Any]]]] = {}
        for meta in all_bridge_metas:
            _lb, bc, _bp, _co = meta
            cnpj_to_bridges.setdefault(bc, []).append(meta)

        if not truncated:
            for gid, bridge_indices in group_to_bridge_indices.items():
                group_metas = [all_bridge_metas[i] for i in bridge_indices]
                # Dedup group_metas by (link_basis, bridge_cnpj, bridge_partner_id).
                # Two metas with same link_basis and bridge_cnpj but different bridge_partner
                # represent distinct routes (e.g. different PJ partners at the same company).
                seen_bridge_keys: set[tuple[str, str, str]] = set()
                deduped_metas: list[tuple[str, str, dict[str, Any] | None, dict[str, Any]]] = []
                for m in group_metas:
                    bp_id = m[2].get("partner_cpf_cnpj", "") if m[2] else ""
                    bk = (m[0], m[1], bp_id)  # (link_basis, bridge_cnpj, bridge_partner_doc)
                    if bk not in seen_bridge_keys:
                        seen_bridge_keys.add(bk)
                        deduped_metas.append(m)

                sample_bridge_cnpj = all_bridge_metas[bridge_indices[0]][1]
                eg_rec = eg_index.get(sample_bridge_cnpj)
                if eg_rec:
                    for member_cnpj in eg_rec.get("member_cnpjs", []):
                        if member_cnpj in unified_cnpjs and unified_cnpjs[member_cnpj] >= 3:
                            cnpj_to_bridges.setdefault(member_cnpj, []).extend(deduped_metas)

        # Iterate unified universe once
        _last_scan_pulse = time.monotonic()
        scan_count = 0
        partners_scanned = 0
        for scan_cnpj, base_degree in unified_cnpjs.items():
            scan_count += 1
            result.total_cnpjs_scanned += 1
            scan_partners = partner_by_cnpj_basico.get(scan_cnpj, [])
            partners_scanned += len(scan_partners)
            result.total_partners_scanned += len(scan_partners)
            now_mono = time.monotonic()
            if scan_count % 500 == 0 or (now_mono - _last_scan_pulse) >= 5.0:
                _last_scan_pulse = now_mono
                ctx.pulse(f"{sanction_entity_name}: scanning", extra={
                    "current_phase": "scan",
                    "current_sanction": sanction_entity_name,
                    "scan_cnpjs_done": scan_count,
                    "unique_cnpjs_to_scan": len(unified_cnpjs),
                    "partners_scanned": partners_scanned,
                    "match_calls": result.match_cache_hits + result.match_cache_misses,
                    "cache_hits": result.match_cache_hits,
                    "cache_misses": result.match_cache_misses,
                })
                _maybe_log_by_time()
            for co_partner in scan_partners:
                partner_name = co_partner.get("partner_name_normalized", "")
                partner_doc = normalize_tax_id(co_partner.get("partner_cpf_cnpj", ""))

                if not partner_name:
                    continue
                # Skip if co-partner IS the sanctioned entity
                if partner_doc and partner_doc == tax_id_normalized:
                    continue

                # Cached fuzzy match
                cache_key = (partner_name, partner_doc or "")
                if cache_key in _match_cache:
                    match_result = _match_cache[cache_key]
                    result.match_cache_hits += 1
                else:
                    match_result = match_entity_record(
                        query_name=partner_name,
                        query_tax_id=partner_doc,
                        index=stf_index,
                        name_field="entity_name_normalized",
                    )
                    _match_cache[cache_key] = match_result
                    result.match_cache_misses += 1

                if match_result is None or match_result.strategy == "ambiguous":
                    continue

                matched_record = match_result.record
                stf_entity_type = str(matched_record.get("entity_type", ""))
                stf_entity_id = str(matched_record.get("entity_id", ""))
                stf_entity_name = str(matched_record.get("entity_name_normalized", ""))

                if stf_entity_type not in {"party", "counsel"}:
                    continue

                # Emit one record per bridge that led to this scan_cnpj.
                # Uses pre-computed cnpj_to_bridges (all bridges, no break).
                scan_bridges = cnpj_to_bridges.get(scan_cnpj, [])
                if not scan_bridges:
                    # No bridge mapped to this CNPJ — should not happen with correct
                    # cnpj_to_bridges construction.  Log and skip rather than
                    # silently attributing to an arbitrary bridge.
                    logger.debug(
                        "SCL: No bridge for scan_cnpj=%s (sanction=%s) — skipping",
                        scan_cnpj,
                        sanction_entity_name,
                    )
                    continue

                for link_basis, bridge_cnpj, bridge_partner, bridge_company in scan_bridges:
                    dedup_key = (sanction_id, bridge_cnpj, stf_entity_type, stf_entity_id, link_basis)
                    if dedup_key in seen_keys:
                        continue

                    link_degree = base_degree

                    # Compute outcome stats
                    if stf_entity_type == "party":
                        process_list = party_process_map.get(stf_entity_name, [])
                    else:
                        process_list = counsel_process_map.get(stf_entity_name, [])

                    process_ids = [pid for pid, _role in process_list]
                    stf_process_count = len(set(process_ids))

                    outcomes_with_roles: list[tuple[str, str | None]] = []
                    for pid, role in process_list:
                        for progress in process_outcomes.get(pid, []):
                            outcomes_with_roles.append((progress, role))

                    favorable_rate = compute_favorable_rate_role_aware(outcomes_with_roles)

                    modal_class, modal_jb = _compute_modal_class_jb(
                        list(set(process_ids)), process_class_map, process_jb_map
                    )
                    baseline_rate: float | None = None
                    if modal_class and modal_jb:
                        baseline_rate = lookup_baseline_rate(
                            stratified_rates, fallback_rates, modal_class, modal_jb
                        )

                    delta: float | None = None
                    risk_score: float | None = None
                    red_flag = False
                    if favorable_rate is not None and baseline_rate is not None:
                        delta = favorable_rate - baseline_rate
                        risk_score = delta * _degree_decay(link_degree)
                        red_flag = (
                            risk_score > RED_FLAG_DELTA_THRESHOLD
                            and stf_process_count >= MIN_CASES_FOR_RED_FLAG
                        )

                    power = (
                        red_flag_power(stf_process_count, baseline_rate) if baseline_rate is not None else None
                    )
                    confidence = red_flag_confidence_label(power)

                    bridge_company_name = (
                        (bridge_company.get("razao_social") if bridge_company else None)
                        or (establishment_index.get(bridge_cnpj, {}).get("nome_fantasia"))
                        or bridge_cnpj
                    )
                    eg = eg_index.get(bridge_cnpj)

                    evidence_chain = _build_evidence_chain(
                        sanction_source=sanction_source,
                        sanction_entity_name=sanction_entity_name,
                        raw_doc=raw_doc,
                        link_basis=link_basis,
                        bridge_company_name=bridge_company_name,
                        bridge_cnpj=bridge_cnpj,
                        scan_cnpj=scan_cnpj,
                        company_index=company_index,
                        establishment_index=establishment_index,
                        partner_name=partner_name,
                        stf_entity_type=stf_entity_type,
                        match_strategy=match_result.strategy,
                        match_score=match_result.score,
                    )

                    link_id = stable_id(
                        "scl-",
                        f"{sanction_id}:{bridge_cnpj}:{stf_entity_type}:{stf_entity_id}:{link_basis}",
                    )
                    record = _build_link_record(
                        link_id=link_id,
                        sanction_id=sanction_id,
                        sanction_source=sanction_source,
                        sanction_entity_name=sanction_entity_name,
                        raw_doc=raw_doc,
                        sanction_type=sanction_type,
                        bridge_cnpj=bridge_cnpj,
                        bridge_company_name=bridge_company_name,
                        link_basis=link_basis,
                        truncated=truncated,
                        bridge_partner=bridge_partner,
                        co_partner=co_partner,
                        eg=eg,
                        stf_entity_type=stf_entity_type,
                        stf_entity_id=stf_entity_id,
                        stf_entity_name=stf_entity_name,
                        match_result=match_result,
                        link_degree=link_degree,
                        stf_process_count=stf_process_count,
                        favorable_rate=favorable_rate,
                        baseline_rate=baseline_rate,
                        delta=delta,
                        risk_score=risk_score,
                        red_flag=red_flag,
                        power=power,
                        confidence=confidence,
                        evidence_chain=evidence_chain,
                        pre_truncation_count=pre_truncation_count,
                        unified_cnpj_count=len(unified_cnpjs),
                        estimated_degree3=estimated_degree3,
                        now_iso=now_iso,
                    )
                    record["record_hash"] = _record_hash(record)

                    seen_keys.add(dedup_key)
                    result.output_records.append(record)

        ctx.advance(1, current_item=sanction_entity_name)
        sanctions_processed += 1

        sanction_elapsed = time.monotonic() - sanction_start
        if sanction_elapsed > 60.0:
            logger.warning(
                "build_sanction_corporate_links: sanction %s took %.1fs (bridges=%d, cnpjs=%d)",
                sanction_entity_name, sanction_elapsed, len(bridge_companies), len(unified_cnpjs),
            )

        if sanctions_processed % 100 == 0:
            _last_log_mono = time.monotonic()
            logger.info(
                "build_sanction_corporate_links: sanctions=%d/%d records=%d "
                "cache_hits=%d cache_misses=%d groups_unique=%d truncated=%d",
                sanctions_processed,
                len(sanctions),
                len(result.output_records),
                result.match_cache_hits,
                result.match_cache_misses,
                len(result.all_groups_touched),
                len(result.truncated_sanctions),
            )
        _maybe_log_by_time()

    logger.info(
        "build_sanction_corporate_links: match_cache hits=%d misses=%d (%.1f%% hit rate), truncated=%d sanctions",
        result.match_cache_hits,
        result.match_cache_misses,
        result.match_cache_hits * 100.0 / max(result.match_cache_hits + result.match_cache_misses, 1),
        len(result.truncated_sanctions),
    )

    return result
