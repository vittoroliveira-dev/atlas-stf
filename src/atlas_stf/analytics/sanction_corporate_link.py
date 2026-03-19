"""Build sanction → corporate → STF indirect links via RFB bridge."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import normalize_tax_id, stable_id
from ..core.stats import red_flag_confidence_label, red_flag_power
from ._atomic_io import AtomicJsonlWriter
from ._match_helpers import (
    build_baseline_rates_stratified,
    build_counsel_process_map,
    build_entity_match_index,
    build_party_process_map,
    build_process_class_map,
    build_process_jb_category_map,
    build_process_outcomes,
    compute_favorable_rate_role_aware,
    lookup_baseline_rate,
    match_entity_record,
)
from ._match_helpers import (
    degree_decay as _degree_decay,
)
from ._run_context import RunContext
from ._scl_bridge import (
    _build_stf_entity_index,
    _collect_sanction_cnpjs,
    _collect_sanction_tax_ids,
    _compute_modal_class_jb,
    _expand_cnpjs_via_groups,
    _load_sanctions,
    _record_hash,
    _stream_companies_for_cnpjs,
    _stream_establishments_for_cnpjs,
    _stream_partners_for_cnpjs,
)
from .donor_corporate_link import _classify_document

logger = logging.getLogger(__name__)

DEFAULT_CGU_DIR = Path("data/raw/cgu")
DEFAULT_CVM_DIR = Path("data/raw/cvm")
DEFAULT_RFB_DIR = Path("data/raw/rfb")
DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")
DEFAULT_OUTPUT_DIR = Path("data/analytics")

RED_FLAG_DELTA_THRESHOLD = 0.15
MIN_CASES_FOR_RED_FLAG = 3


def build_sanction_corporate_links(
    *,
    cgu_dir: Path = DEFAULT_CGU_DIR,
    cvm_dir: Path = DEFAULT_CVM_DIR,
    rfb_dir: Path = DEFAULT_RFB_DIR,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    analytics_dir: Path = DEFAULT_ANALYTICS_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build sanction → corporate → STF indirect links."""
    output_dir.mkdir(parents=True, exist_ok=True)
    total_steps = 11
    ctx = RunContext("cgu-corporate-links", output_dir, total_steps, on_progress=on_progress)

    try:
        # Step 1: Load sanctions
        ctx.start_step(1, "SCL: Carregando sanções...")
        sanctions = _load_sanctions(cgu_dir, cvm_dir)
        if not sanctions:
            logger.warning("No sanctions_raw.jsonl found — skipping sanction corporate links")
            ctx.finish(outputs=[])
            return output_dir
        ctx.log_memory(f"sanctions loaded: {len(sanctions)}")

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
            return output_dir

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
        process_outcomes = build_process_outcomes(decision_event_path)
        stratified_rates, fallback_rates = build_baseline_rates_stratified(decision_event_path, process_path)
        process_class_map = build_process_class_map(process_path)
        process_jb_map = build_process_jb_category_map(decision_event_path)

        now_iso = datetime.now(timezone.utc).isoformat()
        output_records: list[dict[str, Any]] = []
        seen_keys: set[tuple[str, str, str, str, str]] = set()

        # Step 9: Resolve corporate routes
        ctx.start_step(9, "SCL: Resolvendo rotas corporativas...", total_items=len(sanctions), unit="sanções")

        for sanction in sanctions:
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

            for bridge_cnpj, link_basis, bridge_partner in bridge_companies:
                company = company_index.get(bridge_cnpj, {})
                establishment = establishment_index.get(bridge_cnpj)
                bridge_company_name = (
                    company.get("razao_social")
                    or (establishment.get("nome_fantasia") if establishment else None)
                    or bridge_cnpj
                )

                # Collect all CNPJs to scan (bridge + economic group members)
                cnpjs_to_scan: list[tuple[str, int]] = [(bridge_cnpj, 2)]
                eg = eg_index.get(bridge_cnpj)
                if eg:
                    for member_cnpj in eg.get("member_cnpjs", []):
                        if member_cnpj != bridge_cnpj:
                            cnpjs_to_scan.append((member_cnpj, 3))

                for scan_cnpj, base_degree in cnpjs_to_scan:
                    scan_partners = partner_by_cnpj_basico.get(scan_cnpj, [])
                    for co_partner in scan_partners:
                        partner_name = co_partner.get("partner_name_normalized", "")
                        partner_doc = normalize_tax_id(co_partner.get("partner_cpf_cnpj", ""))

                        if not partner_name:
                            continue
                        # Skip if co-partner IS the sanctioned entity
                        if partner_doc and partner_doc == tax_id_normalized:
                            continue

                        match_result = match_entity_record(
                            query_name=partner_name,
                            query_tax_id=partner_doc,
                            index=stf_index,
                            name_field="entity_name_normalized",
                        )
                        if match_result is None or match_result.strategy == "ambiguous":
                            continue

                        matched_record = match_result.record
                        stf_entity_type = str(matched_record.get("entity_type", ""))
                        stf_entity_id = str(matched_record.get("entity_id", ""))
                        stf_entity_name = str(matched_record.get("entity_name_normalized", ""))

                        if stf_entity_type not in {"party", "counsel"}:
                            continue

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
                                risk_score > RED_FLAG_DELTA_THRESHOLD and stf_process_count >= MIN_CASES_FOR_RED_FLAG
                            )

                        power = red_flag_power(stf_process_count, baseline_rate) if baseline_rate is not None else None
                        confidence = red_flag_confidence_label(power)

                        # Evidence chain
                        evidence_chain: list[str] = [
                            f"Sanção {sanction_source.upper()}: {sanction_entity_name} ({raw_doc})",
                        ]
                        if link_basis == "exact_cnpj_basico":
                            evidence_chain.append(f"→ Empresa {bridge_company_name} (CNPJ base {bridge_cnpj})")
                        elif link_basis == "exact_partner_cnpj":
                            evidence_chain.append(f"→ Sócio PJ em {bridge_company_name} (CNPJ base {bridge_cnpj})")
                        else:
                            evidence_chain.append(f"→ Sócio PF em {bridge_company_name} (CNPJ base {bridge_cnpj})")
                        if scan_cnpj != bridge_cnpj:
                            scan_company = company_index.get(scan_cnpj, {})
                            evidence_chain.append(f"→ Grupo econômico: {scan_company.get('razao_social', scan_cnpj)}")
                        match_desc = f"match: {match_result.strategy}"
                        if match_result.score is not None:
                            match_desc += f", score {match_result.score}"
                        evidence_chain.append(f"→ Co-sócio {partner_name} = {stf_entity_type} STF ({match_desc})")

                        hq_uf = establishment.get("uf") if establishment else None
                        hq_cnae = establishment.get("cnae_fiscal") if establishment else None
                        if hq_uf:
                            evidence_chain.append(f"  Sede: {hq_uf}")
                        if hq_cnae:
                            evidence_chain.append(f"  CNAE: {hq_cnae}")

                        source_datasets = ["rfb_socios"]
                        if sanction_source:
                            source_datasets.append(sanction_source)
                        if eg:
                            source_datasets.append("economic_group")

                        link_id = stable_id(
                            "scl-",
                            f"{sanction_id}:{bridge_cnpj}:{stf_entity_type}:{stf_entity_id}:{link_basis}",
                        )

                        record: dict[str, Any] = {
                            "link_id": link_id,
                            "sanction_id": sanction_id,
                            "sanction_source": sanction_source,
                            "sanction_entity_name": sanction_entity_name,
                            "sanction_entity_tax_id": raw_doc,
                            "sanction_type": sanction_type,
                            "bridge_company_cnpj_basico": bridge_cnpj,
                            "bridge_company_name": bridge_company_name,
                            "bridge_link_basis": link_basis,
                            "bridge_confidence": "deterministic",
                            "bridge_partner_role": (
                                co_partner.get("qualification_label")
                                if co_partner != bridge_partner
                                else (bridge_partner.get("qualification_label") if bridge_partner else None)
                            ),
                            "bridge_qualification_code": co_partner.get("qualification_code"),
                            "bridge_qualification_label": co_partner.get("qualification_label"),
                            "economic_group_id": eg.get("group_id") if eg else None,
                            "economic_group_member_count": eg.get("member_count") if eg else None,
                            "is_law_firm_group": eg.get("is_law_firm_group") if eg else None,
                            "stf_entity_type": stf_entity_type,
                            "stf_entity_id": stf_entity_id,
                            "stf_entity_name": stf_entity_name,
                            "stf_match_strategy": match_result.strategy,
                            "stf_match_score": match_result.score,
                            "stf_match_confidence": (
                                "deterministic"
                                if match_result.strategy == "tax_id"
                                else "exact_name"
                                if match_result.strategy in {"exact", "canonical_name", "alias"}
                                else "fuzzy"
                            ),
                            "matched_alias": match_result.matched_alias,
                            "matched_tax_id": match_result.matched_tax_id,
                            "uncertainty_note": match_result.uncertainty_note,
                            "link_degree": link_degree,
                            "stf_process_count": stf_process_count,
                            "favorable_rate": favorable_rate,
                            "baseline_favorable_rate": baseline_rate,
                            "favorable_rate_delta": delta,
                            "risk_score": risk_score,
                            "red_flag": red_flag,
                            "red_flag_power": power,
                            "red_flag_confidence": confidence,
                            "evidence_chain": evidence_chain,
                            "source_datasets": sorted(set(source_datasets)),
                            "generated_at": now_iso,
                        }
                        record["record_hash"] = _record_hash(record)

                        seen_keys.add(dedup_key)
                        output_records.append(record)

            ctx.advance(1, current_item=sanction_entity_name)

        # Step 10: Dedup and sort
        ctx.start_step(10, "SCL: Deduplicando e ordenando...")
        # Post-dedup: for same (sanction_id, bridge_cnpj, stf_entity_type, stf_entity_id) but different
        # link_basis, keep shortest degree then highest stf_match_score; distinct routes are both kept.
        final_map: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for rec in output_records:
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
            "generated_at": now_iso,
        }
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

    except BaseException:
        ctx.finish(outputs=[])
        raise
