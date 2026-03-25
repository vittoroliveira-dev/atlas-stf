"""Build curated agenda events and coverage metadata."""

from __future__ import annotations

import calendar
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import date as date_type
from pathlib import Path
from typing import Any

from ..core.identity import normalize_process_code, stable_id
from .common import read_jsonl_records, utc_now_iso, write_jsonl

logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR = Path("data/raw/agenda")
DEFAULT_CURATED_DIR = Path("data/curated")

RECESS_MONTHS: frozenset[int] = frozenset({1, 7, 12})


def _business_days(year: int, month: int) -> int:
    days_in_month = calendar.monthrange(year, month)[1]
    return sum(1 for d in range(1, days_in_month + 1) if date_type(year, month, d).weekday() < 5)


def _build_process_index(processes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    idx: dict[str, dict[str, Any]] = {}
    for p in processes:
        pn = p.get("process_number")
        if pn:
            idx[normalize_process_code(str(pn))] = p
    return idx


def _match_ref(ref: dict[str, Any], idx: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    cls, num = ref.get("class", ""), ref.get("number", "")
    if cls and num:
        return idx.get(normalize_process_code(f"{cls} {num}"))
    return None


def _slugs_eq(a: str, b: str) -> bool:
    return a.strip().upper() == b.strip().upper()


def build_agenda_events(
    *,
    raw_dir: Path = DEFAULT_RAW_DIR,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    total, step = 6, 0

    def tick(desc: str) -> None:
        nonlocal step
        if on_progress:
            on_progress(step, total, desc)
        step += 1

    tick("Agenda: Carregando eventos brutos...")
    raw_events: list[dict[str, Any]] = []
    if raw_dir.exists():
        for f in sorted(raw_dir.glob("*.jsonl")):
            raw_events.extend(read_jsonl_records(f))
    logger.info("Loaded %d raw agenda events", len(raw_events))

    tick("Agenda: Carregando processos...")
    pp = curated_dir / "process.jsonl"
    processes = read_jsonl_records(pp) if pp.exists() else []
    pidx = _build_process_index(processes)

    tick("Agenda: Cruzando referencias...")
    ts = utc_now_iso()
    curated: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for ev in raw_events:
        slug = ev.get("minister_slug", "")
        refs = ev.get("process_refs") or []
        matched: list[dict[str, Any]] = []
        for ref in refs:
            proc = _match_ref(ref, pidx)
            if proc:
                rap = proc.get("rapporteur_slug") or proc.get("rapporteur", "")
                matched.append(
                    {
                        "process_id": proc.get("process_id"),
                        "process_class": proc.get("process_class"),
                        "process_rapporteur": rap,
                        "is_own_process": bool(rap and slug and _slugs_eq(rap, slug)),
                        "minister_case_role": "relator" if rap and slug and _slugs_eq(rap, slug) else None,
                    }
                )
            else:
                matched.append(
                    {
                        "process_id": None,
                        "process_class": ref.get("class"),
                        "process_rapporteur": None,
                        "is_own_process": False,
                        "minister_case_role": None,
                    }
                )

        aev_id = ev.get("event_id") or stable_id("aev_", f"{slug}:{ev.get('event_date', '')}")
        if aev_id in seen_ids:
            continue
        seen_ids.add(aev_id)

        curated.append(
            {
                "agenda_event_id": aev_id,
                "minister_slug": slug,
                "minister_name": ev.get("minister_name", ""),
                "event_date": ev.get("event_date"),
                "title": ev.get("event_title", ""),
                "event_category": ev.get("event_category", ""),
                "owner_scope": ev.get("owner_scope", ""),
                "meeting_nature": ev.get("meeting_nature"),
                "relevance_track": ev.get("relevance_track"),
                "process_refs_matched": matched,
                "process_refs_raw": refs,
                "institutional_role_bias_flag": bool(ev.get("institutional_role_bias_flag")),
                "generated_at": ts,
            }
        )

    tick("Agenda: Escrevendo eventos...")
    curated_dir.mkdir(parents=True, exist_ok=True)
    out = write_jsonl(curated, curated_dir / "agenda_event.jsonl")

    tick("Agenda: Calculando cobertura...")
    covs = _coverage(curated, ts)
    write_jsonl(covs, curated_dir / "agenda_coverage.jsonl")

    tick("Agenda: Concluido")
    return out


def _parse_date(ev: dict[str, Any]) -> date_type | None:
    raw = ev.get("event_date")
    if not raw:
        return None
    try:
        return date_type.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _coverage(events: list[dict[str, Any]], ts: str) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for ev in events:
        if ev.get("owner_scope") != "ministerial":
            continue
        slug = ev.get("minister_slug", "")
        d = _parse_date(ev)
        if slug and d:
            groups[(slug, d.year, d.month)].append(ev)

    hist: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
    for (slug, y, m), evts in groups.items():
        days = {d.day for e in evts if (d := _parse_date(e)) is not None}
        hist[slug].append((y, m, len(days)))
    for slug in hist:
        hist[slug].sort()

    records: list[dict[str, Any]] = []
    for (slug, y, m), evts in groups.items():
        cats: dict[str, int] = defaultdict(int)
        ta = tb = 0
        days: set[int] = set()
        for e in evts:
            cats[e.get("event_category", "unclear")] += 1
            t = e.get("relevance_track")
            if t == "A":
                ta += 1
            elif t == "B":
                tb += 1
            d = _parse_date(e)
            if d:
                days.add(d.day)

        biz = _business_days(y, m)
        ratio = len(days) / biz if biz else 0.0
        recess = m in RECESS_MONTHS
        tier = "high" if ratio >= 0.6 else ("medium" if ratio >= 0.3 else "low")

        # Vacation heuristic
        vac = False
        if not recess:
            prior = [d for yy, mm, d in hist.get(slug, []) if (yy, mm) < (y, m) and mm not in RECESS_MONTHS]
            if len(prior) >= 3 and sum(prior[-3:]) / 3 >= 15 and len(days) < 5:
                vac = True

        # Publication gap
        gap = False
        prior_r = []
        for yy, mm, dd in hist.get(slug, []):
            if (yy, mm) >= (y, m):
                break
            b = _business_days(yy, mm)
            if b:
                prior_r.append(dd / b)
        if len(prior_r) >= 3 and ratio < 0.3 and sum(prior_r) / len(prior_r) > 0.5:
            gap = True

        records.append(
            {
                "coverage_id": stable_id("acov_", f"{slug}:{y}-{m:02d}"),
                "minister_slug": slug,
                "minister_name": evts[0].get("minister_name", "") if evts else "",
                "owner_scope": "ministerial",
                "year": y,
                "month": m,
                "publication_observed": True,
                "event_count": len(evts),
                "days_with_events": len(days),
                "business_days_in_month": biz,
                "coverage_ratio": round(ratio, 4),
                "institutional_core_count": cats.get("institutional_core", 0),
                "institutional_external_actor_count": cats.get("institutional_external_actor", 0),
                "private_advocacy_count": cats.get("private_advocacy", 0),
                "unclear_count": cats.get("unclear", 0),
                "track_a_count": ta,
                "track_b_count": tb,
                "court_recess_flag": recess,
                "vacation_or_leave_flag": vac,
                "publication_gap_flag": gap,
                "comparability_tier": tier,
                "coverage_quality_note": "recess_from_heuristic" if recess else None,
                "generated_at": ts,
            }
        )
    return records
