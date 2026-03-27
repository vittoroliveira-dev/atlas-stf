"""Build agenda exposure analytics."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import date as date_type
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ..curated.common import read_jsonl_records, utc_now_iso, write_jsonl
from ..schema_validate import validate_records

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")
SCHEMA_PATH = Path("schemas/agenda_exposure.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/agenda_exposure_summary.schema.json")

_WINDOWS = [("7d", 7), ("14d", 14), ("30d", 30), ("60d", 60)]
_W_SCORE = {"7d": 0.3, "14d": 0.2, "30d": 0.1, "60d": 0.05}
_DT_WEIGHT = {"acordao": 0.1, "decisao_monocratica_merito": 0.1, "merito": 0.1}
_CAT_SCORE = {"private_advocacy": 0.1}
_MIN_N = 5

_NOTE = (
    "Agenda exposure analytics measure temporal proximity between public ministerial "
    "agenda events and subsequent judicial decisions. All comparisons are intra-minister. "
    "Records with fewer than 5 baseline observations have priority_score capped at 0.29."
)


def _pd(v: Any) -> date_type | None:
    if not v:
        return None
    try:
        return date_type.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def _win(days: int) -> str | None:
    for label, th in _WINDOWS:
        if days <= th:
            return label
    return None


def build_agenda_exposure(
    *,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    analytics_dir: Path = DEFAULT_ANALYTICS_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    total, step = 7, 0

    def tick(d: str) -> None:
        nonlocal step
        if on_progress:
            on_progress(step, total, d)
        step += 1

    tick("Exposicao: Carregando...")
    ap = curated_dir / "agenda_event.jsonl"
    cp = curated_dir / "agenda_coverage.jsonl"
    dp = curated_dir / "decision_event.jsonl"
    pp = curated_dir / "process.jsonl"

    aevs = read_jsonl_records(ap) if ap.exists() else []
    covs = read_jsonl_records(cp) if cp.exists() else []
    decs = read_jsonl_records(dp) if dp.exists() else []
    read_jsonl_records(pp) if pp.exists() else []

    tick("Exposicao: Indexando...")
    dbyp: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for d in decs:
        pid = d.get("process_id")
        if pid:
            dbyp[pid].append(d)

    cidx: dict[tuple[str, int, int], dict[str, Any]] = {}
    for c in covs:
        cidx[(c.get("minister_slug", ""), c.get("year", 0), c.get("month", 0))] = c

    tick("Exposicao: Filtrando Track A...")
    relevant = [e for e in aevs if e.get("owner_scope") == "ministerial" and e.get("relevance_track") == "A"]

    tick("Exposicao: Cruzando decisoes...")
    ts = utc_now_iso()
    records: list[dict[str, Any]] = []

    for ev in relevant:
        slug = ev.get("minister_slug", "")
        ad = _pd(ev.get("event_date"))
        refs = ev.get("process_refs_matched") or []
        cc = "low"
        if ad:
            cv = cidx.get((slug, ad.year, ad.month))
            if cv:
                cc = cv.get("comparability_tier", "low")

        common = {
            "aeid": ev.get("agenda_event_id", ""),
            "slug": slug,
            "name": ev.get("minister_name", ""),
            "ad": ev.get("event_date"),
            "title": ev.get("title", ""),
            "cat": ev.get("event_category", ""),
            "nat": ev.get("meeting_nature"),
            "bias": ev.get("institutional_role_bias_flag", False),
            "cc": cc,
            "ts": ts,
        }

        if not refs:
            records.append(_mk(common))
            continue

        for ref in refs:
            pid = ref.get("process_id")
            re = {
                "pid": pid,
                "pcls": ref.get("process_class"),
                "own": ref.get("is_own_process", False),
                "role": ref.get("minister_case_role"),
            }
            if not pid or not ad:
                records.append(_mk(common, re))
                continue
            found = False
            for dec in dbyp.get(pid, []):
                dd = _pd(dec.get("decision_date"))
                if not dd or dd <= ad:
                    continue
                diff = (dd - ad).days
                w = _win(diff)
                if w is None:
                    continue
                found = True
                records.append(
                    _mk(
                        common,
                        re,
                        {
                            "deid": dec.get("decision_event_id"),
                            "dd": dec.get("decision_date"),
                            "dt": dec.get("decision_type"),
                            "db": diff,
                            "w": w,
                        },
                    )
                )
            if not found:
                records.append(_mk(common, re))

    tick("Exposicao: Scoring...")
    bl = _baselines(records, cidx)
    for r in records:
        _score(r, bl)

    tick("Exposicao: Escrevendo...")
    analytics_dir.mkdir(parents=True, exist_ok=True)
    validate_records(records, SCHEMA_PATH)
    out = write_jsonl(records, analytics_dir / "agenda_exposure.jsonl")

    tb = sum(1 for e in aevs if e.get("owner_scope") == "ministerial" and e.get("relevance_track") == "B")
    bm: dict[str, int] = defaultdict(int)
    bw: dict[str, int] = defaultdict(int)
    bp: dict[str, int] = defaultdict(int)
    ms: set[str] = set()
    ds: list[str] = []
    for r in records:
        s = r.get("minister_slug", "")
        bm[s] += 1
        ms.add(s)
        bw[r.get("window", "none")] += 1
        bp[r.get("priority_tier", "low")] += 1
        if r.get("agenda_date"):
            ds.append(str(r["agenda_date"]))

    summary = {
        "total_relevant_events": len(relevant),
        "total_exposures": len(records),
        "track_a_events": len(relevant),
        "track_b_events_stored": tb,
        "exposures_by_minister": dict(bm),
        "exposures_by_window": dict(bw),
        "exposures_by_priority": dict(bp),
        "coverage_scope": "public_agenda_partial",
        "coverage_minister_set": sorted(ms),
        "coverage_start_date": sorted(ds)[0] if ds else None,
        "within_minister_only": True,
        "cross_minister_allowed": False,
        "historical_claims_pre_2024": False,
        "methodology_note": _NOTE,
        "generated_at": ts,
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)
    (analytics_dir / "agenda_exposure_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tick("Exposicao: Concluido")
    return out


def _mk(c: dict[str, Any], r: dict[str, Any] | None = None, d: dict[str, Any] | None = None) -> dict[str, Any]:
    pid = (r or {}).get("pid")
    deid = (d or {}).get("deid")
    return {
        "exposure_id": stable_id("aex_", f"{c['aeid']}:{pid or 'x'}:{deid or 'x'}"),
        "agenda_event_id": c["aeid"],
        "minister_slug": c["slug"],
        "minister_name": c["name"],
        "process_id": pid,
        "process_class": (r or {}).get("pcls"),
        "agenda_date": c["ad"],
        "agenda_event_title": c["title"],
        "agenda_event_category": c["cat"],
        "agenda_meeting_nature": c["nat"],
        "decision_event_id": deid,
        "decision_date": (d or {}).get("dd"),
        "decision_type": (d or {}).get("dt"),
        "days_between": (d or {}).get("db"),
        "window": (d or {}).get("w", "none"),
        "is_own_process": (r or {}).get("own", False),
        "minister_case_role": (r or {}).get("role"),
        "institutional_role_bias_flag": c.get("bias", False),
        "baseline_rate": None,
        "observed_rate": None,
        "rate_ratio": None,
        "priority_score": None,
        "priority_tier": None,
        "priority_tier_override_reason": None,
        "coverage_comparability": c["cc"],
        "relevance_track": "A",
        "within_minister_only": True,
        "generated_at": c["ts"],
    }


def _baselines(
    recs: list[dict[str, Any]],
    cidx: dict[tuple[str, int, int], dict[str, Any]],
) -> dict[tuple[str, str | None, str | None], dict[str, float | int]]:
    gc: dict[tuple[str, str | None, str | None], int] = defaultdict(int)
    for r in recs:
        if r.get("decision_event_id"):
            gc[(r.get("minister_slug", ""), r.get("process_class"), r.get("decision_type"))] += 1
    mw: dict[str, float] = defaultdict(float)
    for (s, _, _), cv in cidx.items():
        if cv.get("comparability_tier", "low") != "low":
            mw[s] += cv.get("business_days_in_month", 22) / 5.0
    bl: dict[tuple[str, str | None, str | None], dict[str, float | int]] = {}
    for k, n in gc.items():
        w = mw.get(k[0], 0.0)
        bl[k] = {"baseline_rate": round(n / w, 6) if w else 0.0, "count": n}
    return bl


def _score(
    r: dict[str, Any],
    bl: dict[tuple[str, str | None, str | None], dict[str, float | int]],
) -> None:
    k = (r.get("minister_slug", ""), r.get("process_class"), r.get("decision_type"))
    bi = bl.get(k)
    r["baseline_rate"] = bi["baseline_rate"] if bi else None
    bn = bi["count"] if bi else 0

    s = 0.0
    if r.get("process_id"):
        s += 0.3
    if r.get("is_own_process"):
        s += 0.2
    s += _W_SCORE.get(r.get("window", ""), 0.0)
    dt = r.get("decision_type")
    if dt:
        s += _DT_WEIGHT.get(dt, 0.0)
    s += _CAT_SCORE.get(r.get("agenda_event_category", ""), 0.0)
    if r.get("institutional_role_bias_flag"):
        s *= 0.7
    cc = r.get("coverage_comparability", "low")
    if cc == "low":
        s *= 0.5
    elif cc == "medium":
        s *= 0.8

    ov: str | None = None
    if bn < _MIN_N:
        s = min(s, 0.29)
        r["rate_ratio"] = None
        ov = "insufficient_baseline_n"
    else:
        br = r.get("baseline_rate") or 0.0
        if br > 0:
            r["observed_rate"] = 1.0
            r["rate_ratio"] = round(1.0 / br, 4)

    r["priority_score"] = round(min(s, 1.0), 4)
    r["priority_tier"] = "high" if s >= 0.6 else ("medium" if s >= 0.3 else "low")
    r["priority_tier_override_reason"] = ov
