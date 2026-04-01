"""Corporate network: minister-party/counsel corporate ties.

Discovery → Enrichment. Summary at every phase checkpoint.
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name
from ..core.stats import red_flag_confidence_label, red_flag_power
from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._corporate_network_context import (
    MIN_CASES_FOR_RED_FLAG,
    RED_FLAG_DELTA_THRESHOLD,
    CorporateNetworkContext,
    _build_counsel_index,
    _build_name_to_cnpjs,
    _build_partner_index,
    _build_pj_partner_index,
    _compute_conflict,
    _load_minister_names,
    _now,
    _RunStats,
    _write_summary,
)
from ._match_helpers import (
    build_baseline_rates_stratified,
    build_counsel_process_map,
    build_party_index,
    build_party_process_map,
    build_process_class_map,
    build_process_jb_category_map,
    degree_decay,
    lookup_baseline_rate,
    read_jsonl,
)
from ._outcome_helpers import (
    compute_favorable_rate_role_aware,
    compute_favorable_rate_substantive,
)

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/corporate_network.schema.json")
DEFAULT_MAX_LINK_DEGREE = 3


def build_corporate_network(
    *,
    rfb_dir: Path = Path("data/raw/rfb"),
    minister_bio_path: Path = Path("data/curated/minister_bio.json"),
    party_path: Path = Path("data/curated/party.jsonl"),
    counsel_path: Path = Path("data/curated/counsel.jsonl"),
    process_path: Path = Path("data/curated/process.jsonl"),
    decision_event_path: Path = Path("data/curated/decision_event.jsonl"),
    process_party_link_path: Path = Path("data/curated/process_party_link.jsonl"),
    process_counsel_link_path: Path = Path("data/curated/process_counsel_link.jsonl"),
    output_dir: Path = Path("data/analytics"),
    max_link_degree: int = DEFAULT_MAX_LINK_DEGREE,
) -> Path:
    """Build corporate network analytics from RFB data + curated entities."""
    output_dir.mkdir(parents=True, exist_ok=True)
    max_link_degree = min(max(1, int(max_link_degree)), 6)
    run = _RunStats(
        started_at=_now(),
        resolved_rfb_dir=str(rfb_dir.resolve()),
        resolved_output_dir=str(output_dir.resolve()),
    )
    output_path = output_dir / "corporate_network.jsonl"
    summary_path = output_dir / "corporate_network_summary.json"
    conflicts: list[dict[str, Any]] = []

    try:
        run.phase = "discovery"
        _write_summary(summary_path, run, conflicts, max_link_degree)
        _phase_discovery(
            rfb_dir=rfb_dir,
            minister_bio_path=minister_bio_path,
            party_path=party_path,
            counsel_path=counsel_path,
            max_link_degree=max_link_degree,
            run=run,
            conflicts=conflicts,
        )
        run.phase = "discovery_done"
        _write_summary(summary_path, run, conflicts, max_link_degree)
        logger.info("Discovery: %d candidates, %d conflicts", run.candidate_entities_total, run.conflicts_appended)

        if run.conflicts_appended > 0:
            run.phase = "enrichment"
            _write_summary(summary_path, run, conflicts, max_link_degree)
            _phase_enrichment(
                decision_event_path=decision_event_path,
                process_path=process_path,
                process_party_link_path=process_party_link_path,
                process_counsel_link_path=process_counsel_link_path,
                party_path=party_path,
                counsel_path=counsel_path,
                rfb_dir=rfb_dir,
                output_dir=output_dir,
                run=run,
                conflicts=conflicts,
            )
            run.phase = "enrichment_done"
            logger.info("Enrichment: %d with shared processes", run.conflicts_with_shared_processes)
        else:
            run.phase = "enrichment_skipped"

        run.phase = "write"
        validate_records(conflicts, SCHEMA_PATH)
        with AtomicJsonlWriter(output_path) as fh:
            for c in conflicts:
                fh.write(json.dumps(c, ensure_ascii=False) + "\n")
                run.jsonl_records_written += 1

        if run.compute_conflict_calls > 0 and run.jsonl_records_written == 0:
            raise RuntimeError(f"Invariant: compute_conflict_calls={run.compute_conflict_calls} but written=0")
        run.status = "ok"
        run.phase = "complete"
    except Exception as exc:
        run.status = "error"
        run.error_type = type(exc).__name__
        run.error_message = str(exc)[:500]
        logger.error("Build failed at phase=%s: %s", run.phase, exc)
        raise
    finally:
        run.finished_at = _now()
        _write_summary(summary_path, run, conflicts, max_link_degree)

    logger.info(
        "Built: %d conflicts (%d shared, %d red flags)",
        run.conflicts_appended,
        run.conflicts_with_shared_processes,
        sum(1 for c in conflicts if c.get("red_flag")),
    )
    return output_path


def _phase_discovery(
    *,
    rfb_dir: Path,
    minister_bio_path: Path,
    party_path: Path,
    counsel_path: Path,
    max_link_degree: int,
    run: _RunStats,
    conflicts: list[dict[str, Any]],
) -> None:
    """Find candidate conflicts. Lightweight — no baselines or process data."""
    partner_index = _build_partner_index(rfb_dir)
    run.partner_index_size = len(partner_index)
    logger.info("partner_index: %d CNPJs", run.partner_index_size)
    if not partner_index:
        return

    minister_names = _load_minister_names(minister_bio_path)
    run.minister_names_loaded = len(minister_names)
    name_to_cnpjs = _build_name_to_cnpjs(partner_index)
    pj_partner_index = _build_pj_partner_index(partner_index)
    party_idx = build_party_index(party_path)
    counsel_idx = _build_counsel_index(counsel_path)

    # Collect only CNPJs reachable from ministers within max_link_degree
    relevant_cnpjs: set[str] = set()
    for minister_norm in minister_names:
        cnpjs = name_to_cnpjs.get(minister_norm, set())
        if cnpjs:
            run.ministers_with_companies += 1
            run.companies_linked_to_ministers += len(cnpjs)
            relevant_cnpjs.update(cnpjs)
    if not relevant_cnpjs:
        return

    # BFS expansion to find all reachable CNPJs within max_link_degree
    bfs_cnpjs = set(relevant_cnpjs)
    frontier = set(relevant_cnpjs)
    for _depth in range(max_link_degree - 1):
        next_frontier: set[str] = set()
        for cnpj in frontier:
            for p in partner_index.get(cnpj, []):
                if p.get("partner_type") == "1":
                    pj = p.get("partner_cpf_cnpj", "").strip()
                    if pj:
                        for linked in pj_partner_index.get(pj, set()):
                            if linked not in bfs_cnpjs:
                                next_frontier.add(linked)
        bfs_cnpjs.update(next_frontier)
        frontier = next_frontier
        if not frontier:
            break

    # Company index: only BFS-reachable CNPJs
    company_index: dict[str, dict[str, Any]] = {}
    companies_path = rfb_dir / "companies_raw.jsonl"
    if companies_path.exists():
        for record in read_jsonl(companies_path):
            cnpj = record.get("cnpj_basico", "")
            if cnpj in bfs_cnpjs:
                company_index[cnpj] = record
    logger.info("company_index (lazy): %d of %d BFS-reachable", len(company_index), len(bfs_cnpjs))

    def _load_ref(name: str) -> dict[str, str]:
        p = rfb_dir / f"{name}.json"
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

    ctx = CorporateNetworkContext(
        company_index=company_index,
        partner_index=partner_index,
        process_party_map={},
        process_counsel_map={},
        process_outcomes={},
        stratified_rates=None,
        fallback_rates=None,
        process_jb_map={},
        process_class_map={},
        rapporteur_map={},
        qualificacoes=_load_ref("qualificacoes"),
        naturezas=_load_ref("naturezas"),
        estab_index={},
        eg_index={},
        now_iso=_now(),
    )

    seen_keys: set[str] = set()

    def company_label(cnpj: str) -> str:
        return company_index.get(cnpj, {}).get("razao_social") or cnpj

    for minister_norm, minister_name in minister_names.items():
        minister_cnpjs = sorted(name_to_cnpjs.get(minister_norm, set()))
        if not minister_cnpjs:
            continue
        best_degree: dict[str, int] = {c: 1 for c in minister_cnpjs}
        queue = deque((c, 1, [company_label(c)]) for c in minister_cnpjs)
        while queue:
            cnpj, degree, chain = queue.popleft()
            for co in partner_index.get(cnpj, []):
                rr = co.get("representative_name") or co.get("representative_name_normalized", "")
                for nk, dn, qual, evt in (
                    (
                        co.get("partner_name_normalized", ""),
                        co.get("partner_name") or co.get("partner_name_normalized", ""),
                        co.get("qualification_code"),
                        "partner_pf",
                    ),
                    (co.get("representative_name_normalized", ""), f"(repr.) {rr}", None, "representative"),
                ):
                    if not nk or nk == minister_norm:
                        continue
                    run.co_partners_seen += 1
                    er = party_idx.get(nk)
                    if er:
                        run.co_partners_in_party_index += 1
                    else:
                        er = counsel_idx.get(nk)
                        if er:
                            run.co_partners_in_counsel_index += 1
                    if er is None:
                        continue
                    run.candidate_entities_total += 1
                    ck = f"{minister_norm}:{cnpj}:{nk}:d{degree}"
                    if ck in seen_keys:
                        continue
                    seen_keys.add(ck)
                    et = "party" if nk in party_idx else "counsel"
                    run.compute_conflict_calls += 1
                    lc = f"{minister_name} -> {' -> '.join(chain)} -> {dn}"
                    conflict = _compute_conflict(
                        ctx, minister_name, minister_norm, cnpj, nk, dn, et, er, degree, lc, evt, qual
                    )
                    conflicts.append(conflict)
                    run.conflicts_appended += 1
            if degree >= max_link_degree:
                continue
            for co in partner_index.get(cnpj, []):
                if co.get("partner_type") != "1":
                    continue
                pj = co.get("partner_cpf_cnpj", "").strip()
                if not pj:
                    continue
                for nc in sorted(pj_partner_index.get(pj, set())):
                    nd = degree + 1
                    if nc == cnpj or nd > max_link_degree:
                        continue
                    if nd >= best_degree.get(nc, nd + 1):
                        continue
                    best_degree[nc] = nd
                    queue.append((nc, nd, [*chain, company_label(nc)]))


def _enrich_favorable_rates(
    conflicts: list[dict[str, Any]],
    process_path: Path,
    decision_event_path: Path,
    process_party_map: dict[str, list[tuple[str, str | None]]],
    process_counsel_map: dict[str, list[tuple[str, str | None]]],
    process_outcomes: dict[str, list[str]],
) -> None:
    """Compute favorable_rate, baseline, risk_score for conflicts with shared processes."""
    process_class_map = build_process_class_map(process_path)
    process_jb_map = build_process_jb_category_map(decision_event_path)
    stratified_rates, fallback_rates = build_baseline_rates_stratified(decision_event_path, process_path)

    for conflict in conflicts:
        shared = conflict.get("shared_process_ids", [])
        if not shared:
            continue
        et = conflict.get("linked_entity_type", "")
        en = normalize_entity_name(conflict.get("linked_entity_name", "")) or ""
        shared_set = set(shared)
        outcomes_with_roles: list[tuple[str, str | None]] = []
        class_jb_pairs: list[tuple[str, str]] = []
        seen: set[str] = set()

        pmap = process_party_map if et == "party" else process_counsel_map
        for pid, role_or_side in pmap.get(en, []):
            if pid in shared_set:
                for progress in process_outcomes.get(pid, []):
                    outcomes_with_roles.append((progress, role_or_side if et == "party" else None))
                if pid not in seen:
                    seen.add(pid)
                    pc = process_class_map.get(pid)
                    if pc:
                        class_jb_pairs.append((pc, process_jb_map.get(pid, "incerto")))

        fav = compute_favorable_rate_role_aware(outcomes_with_roles)
        fav_sub, n_sub = compute_favorable_rate_substantive(outcomes_with_roles)

        baseline_rate: float | None = None
        if class_jb_pairs:
            mc, mj = Counter(class_jb_pairs).most_common(1)[0][0]
            baseline_rate = lookup_baseline_rate(stratified_rates, fallback_rates, mc, mj)

        link_degree = conflict.get("link_degree", 1)
        df = degree_decay(link_degree)
        delta: float | None = None
        risk_score: float | None = None
        red_flag = False
        if fav is not None and baseline_rate is not None:
            delta = fav - baseline_rate
            risk_score = delta * df
            red_flag = risk_score > RED_FLAG_DELTA_THRESHOLD and len(seen) >= MIN_CASES_FOR_RED_FLAG

        red_flag_sub: bool | None = None
        if fav_sub is not None and baseline_rate is not None and n_sub >= MIN_CASES_FOR_RED_FLAG:
            sub_risk = (fav_sub - baseline_rate) * df
            red_flag_sub = sub_risk > RED_FLAG_DELTA_THRESHOLD

        power = red_flag_power(len(seen), baseline_rate) if baseline_rate is not None else None
        confidence = red_flag_confidence_label(power)

        conflict["favorable_rate"] = fav
        conflict["favorable_rate_substantive"] = fav_sub
        conflict["substantive_decision_count"] = n_sub
        conflict["baseline_favorable_rate"] = baseline_rate
        conflict["favorable_rate_delta"] = delta
        conflict["risk_score"] = risk_score
        conflict["decay_factor"] = df
        conflict["red_flag"] = red_flag
        conflict["red_flag_substantive"] = red_flag_sub
        conflict["red_flag_power"] = power
        conflict["red_flag_confidence"] = confidence


def _phase_enrichment(
    *,
    decision_event_path: Path,
    process_path: Path,
    process_party_link_path: Path,
    process_counsel_link_path: Path,
    party_path: Path,
    counsel_path: Path,
    rfb_dir: Path,
    output_dir: Path,
    run: _RunStats,
    conflicts: list[dict[str, Any]],
) -> None:
    """Enrich conflicts with rapporteur matching, baselines, and establishment data."""
    party_idx = build_party_index(party_path)
    counsel_idx = _build_counsel_index(counsel_path)
    pid2name = {r.get("party_id", ""): n for n, r in party_idx.items() if r.get("party_id")}
    process_party_map = build_party_process_map(process_party_link_path, pid2name)
    cid2name = {r.get("counsel_id", ""): n for n, r in counsel_idx.items() if r.get("counsel_id")}
    process_counsel_map = build_counsel_process_map(process_counsel_link_path, cid2name)

    # Single pass over decision_event: rapporteur_map + process_outcomes
    rapporteur_map: dict[str, str] = {}
    process_outcomes: dict[str, list[str]] = defaultdict(list)
    for record in read_jsonl(decision_event_path):
        pid = record.get("process_id")
        if not pid:
            continue
        rap = record.get("current_rapporteur")
        if rap:
            rapporteur_map[pid] = rap
        progress = record.get("decision_progress")
        if progress:
            process_outcomes[pid].append(progress)
    logger.info("Single pass: rapporteur=%d, outcomes=%d", len(rapporteur_map), len(process_outcomes))

    # Baselines + process classification (only needed if shared processes exist)
    has_shared = False
    conflict_cnpjs = {c["company_cnpj_basico"] for c in conflicts}

    # First pass: find shared processes
    for conflict in conflicts:
        en = normalize_entity_name(conflict.get("linked_entity_name", "")) or ""
        et = conflict.get("linked_entity_type", "")
        mn = conflict.get("minister_name", "")
        shared: list[str] = []
        if en and et == "party":
            shared = [p for p, _ in process_party_map.get(en, []) if rapporteur_map.get(p) == mn]
        elif en and et == "counsel":
            shared = [p for p, _ in process_counsel_map.get(en, []) if rapporteur_map.get(p) == mn]
        conflict["shared_process_ids"] = shared
        conflict["shared_process_count"] = len(shared)
        if shared:
            run.conflicts_with_shared_processes += 1
            has_shared = True

    if has_shared:
        _enrich_favorable_rates(
            conflicts, process_path, decision_event_path,
            process_party_map, process_counsel_map, process_outcomes,
        )
        logger.info("%d conflicts have shared processes (favorable_rate computed)", run.conflicts_with_shared_processes)
    else:
        logger.info("No shared processes — baselines not loaded")

    # Estab index — only for conflict CNPJs
    estab_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    estab_path = rfb_dir / "establishments_raw.jsonl"
    if estab_path.exists():
        for record in read_jsonl(estab_path):
            cnpj = record.get("cnpj_basico", "")
            if cnpj in conflict_cnpjs:
                estab_index[cnpj].append(record)

    eg_index: dict[str, dict[str, Any]] = {}
    eg_path = output_dir / "economic_group.jsonl"
    if eg_path.exists():
        for record in read_jsonl(eg_path):
            for cnpj in record.get("member_cnpjs", []):
                if cnpj in conflict_cnpjs:
                    eg_index[cnpj] = record

    for conflict in conflicts:
        cnpj = conflict.get("company_cnpj_basico", "")
        establishments = estab_index.get(cnpj, [])
        conflict["establishment_count"] = len(establishments)
        active = [e for e in establishments if e.get("situacao_cadastral") == "02"]
        conflict["active_establishment_count"] = len(active)
        eg = eg_index.get(cnpj, {})
        if eg:
            conflict["economic_group_id"] = eg.get("group_id")
            conflict["economic_group_member_count"] = eg.get("member_count")
