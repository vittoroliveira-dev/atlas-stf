"""Cross-artifact referential integrity, field coverage and cardinality checks.

Reads JSONL files directly (streaming, line-by-line) — no atlas_stf imports.
Can be run as ``python -m atlas_stf.validation.pipeline_integrity``.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_CR, _HI, _ME = "CRITICAL", "HIGH", "MEDIUM"


@dataclass
class CheckResult:
    name: str
    description: str
    severity: str
    status: str
    artifacts: list[str]
    observed_value: Any
    threshold: Any
    details: str
    samples: list[str] = field(default_factory=list)
    suggestion: str = ""


@dataclass
class ValidationReport:
    timestamp: str
    scope: str
    status: str
    checks: list[CheckResult]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)  # pyright: ignore[reportReturnType]


# -- streaming helpers -------------------------------------------------------


def _ids(path: Path, fld: str) -> set[str]:
    out: set[str] = set()
    if not path.exists():
        return out
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            if not (ln := raw.strip()):
                continue
            v = json.loads(ln).get(fld)
            if v is not None:
                out.add(str(v))
    return out


def _multi_ids(path: Path, fields: list[str]) -> dict[str, set[str]]:
    s: dict[str, set[str]] = {f: set() for f in fields}
    if not path.exists():
        return s
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            if not (ln := raw.strip()):
                continue
            rec = json.loads(ln)
            for f in fields:
                v = rec.get(f)
                if v is not None:
                    s[f].add(str(v))
    return s


def _coverage(path: Path, fld: str) -> tuple[int, int]:
    total = nn = 0
    if not path.exists():
        return total, nn
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            if not (ln := raw.strip()):
                continue
            total += 1
            v = json.loads(ln).get(fld)
            if v is not None and v != "":
                nn += 1
    return total, nn


def _lines(path: Path) -> int:
    n = 0
    if not path.exists():
        return n
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            if raw.strip():
                n += 1
    return n


def _pct(num: int, den: int) -> float:
    return num / den if den else 0.0


def _c(
    nm: str,
    ds: str,
    sv: str,
    st: str,
    ar: list[str],
    ob: Any,
    th: Any,
    dt: str,
    sa: list[str] | None = None,
    sg: str = "",
) -> CheckResult:
    return CheckResult(nm, ds, sv, st, ar, ob, th, dt, sa or [], sg)


def _skip(nm: str, ds: str, sv: str, ar: list[str], dt: str) -> CheckResult:
    return _c(nm, ds, sv, "SKIP", ar, None, None, dt)


# -- checks ------------------------------------------------------------------


def check_alert_referential_integrity(cd: Path, ad: Path) -> CheckResult:
    nm, ds = "alert_referential_integrity", "Alertas referenciam artefatos existentes"
    ap = ad / "outlier_alert.jsonl"
    ar = ["outlier_alert.jsonl", "decision_event.jsonl", "process.jsonl", "comparison_group.jsonl"]
    if not ap.exists():
        return _skip(nm, ds, _CR, ar[:1], "outlier_alert.jsonl nao encontrado")

    refs = {
        "decision_event_id": _ids(cd / "decision_event.jsonl", "decision_event_id"),
        "process_id": _ids(cd / "process.jsonl", "process_id"),
        "comparison_group_id": _ids(ad / "comparison_group.jsonl", "comparison_group_id"),
    }
    orph: dict[str, list[str]] = {k: [] for k in refs}
    total = 0
    with open(ap, encoding="utf-8") as fh:
        for raw in fh:
            if not (ln := raw.strip()):
                continue
            rec = json.loads(ln)
            total += 1
            for fk, ref in refs.items():
                v = rec.get(fk)
                if v and v not in ref and len(orph[fk]) < 5:
                    orph[fk].append(v)
    oc = sum(len(v) for v in orph.values())
    sa = [f"{k}={v}" for k, vs in orph.items() for v in vs][:5]
    st = "PASS" if oc == 0 else "FAIL"
    sg = "Verificar se curate e analytics rodaram na mesma versão dos dados" if oc else ""
    return _c(
        nm,
        ds,
        _CR,
        st,
        ar,
        f"{total} alerts, {oc} orphan FKs ({_pct(oc, total) * 100:.2f}%)",
        "0 orphans",
        f"{total} alertas verificados, {oc} referências órfãs",
        sa,
        sg,
    )


def check_representation_edge_party_coverage(cd: Path, threshold: float = 0.01) -> CheckResult:
    nm, ds = "representation_edge_party_coverage", "Cobertura de party_id em arestas"
    p = cd / "representation_edge.jsonl"
    if not p.exists():
        return _skip(nm, ds, _HI, [p.name], f"{p.name} nao encontrado")
    t, nn = _coverage(p, "party_id")
    r = _pct(nn, t)
    st = "PASS" if r >= threshold else "FAIL"
    sg = "Verificar _resolve_party_id em _build_representation_edges.py" if st == "FAIL" else ""
    return _c(
        nm,
        ds,
        _HI,
        st,
        [p.name],
        f"{t} edges, {nn} com party_id ({r * 100:.2f}%)",
        f"{threshold * 100:.1f}%",
        f"{t} arestas, {nn} com party_id ({r * 100:.2f}%)",
        sg=sg,
    )


def check_session_event_rapporteur_coverage(cd: Path, threshold: float = 0.30) -> CheckResult:
    nm, ds = "session_event_rapporteur_coverage", "Cobertura de rapporteur_at_event em sessões"
    p = cd / "session_event.jsonl"
    if not p.exists():
        return _skip(nm, ds, _HI, [p.name], f"{p.name} nao encontrado")
    t, nn = _coverage(p, "rapporteur_at_event")
    r = _pct(nn, t)
    st = "PASS" if r >= threshold else "FAIL"
    sg = "Verificar enriquecimento de rapporteur em build_session_event.py" if st == "FAIL" else ""
    return _c(
        nm,
        ds,
        _HI,
        st,
        [p.name],
        f"{t} events, {nn} com rapporteur ({r * 100:.2f}%)",
        f"{threshold * 100:.0f}%",
        f"{t} sessões, {nn} com rapporteur_at_event ({r * 100:.2f}%)",
        sg=sg,
    )


def check_representation_event_sanity(cd: Path) -> CheckResult:
    nm, ds = "representation_event_sanity", "Proporção eventos/arestas de representação"
    ar = ["representation_event.jsonl", "representation_edge.jsonl"]
    ep = cd / "representation_edge.jsonl"
    if not ep.exists():
        return _skip(nm, ds, _ME, ar, "representation_edge.jsonl nao encontrado")
    ec = _lines(ep)
    vp = cd / "representation_event.jsonl"
    vc = _lines(vp) if vp.exists() else 0
    if vc == 0 and ec > 0:
        return _c(
            nm,
            ds,
            _CR,
            "FAIL",
            ar,
            f"0 events, {ec} edges",
            ">0 quando edges > 0",
            "Zero eventos com arestas presentes",
            sg="Verificar se curate representation foi executado corretamente",
        )
    ratio = _pct(vc, ec)
    if ratio < 0.001 and ec > 0:
        return _c(
            nm,
            ds,
            _ME,
            "WARN",
            ar,
            f"{vc} events, {ec} edges (ratio {ratio:.6f})",
            "ratio >= 0.001",
            f"Proporção eventos/arestas baixa: {ratio:.6f}",
            sg="Proporção baixa pode ser esperada; verificar fontes de eventos",
        )
    return _c(
        nm,
        ds,
        _ME,
        "PASS",
        ar,
        f"{vc} events, {ec} edges (ratio {ratio:.4f})",
        "ratio >= 0.001",
        f"{vc} eventos, {ec} arestas, proporção {ratio:.4f}",
    )


def check_recurrence_referential_integrity(cd: Path, ad: Path) -> CheckResult:
    nm, ds = "recurrence_referential_integrity", "Recorrências referenciam entidades existentes"
    rp = ad / "representation_recurrence.jsonl"
    ar = ["representation_recurrence.jsonl", "lawyer_entity.jsonl", "party.jsonl"]
    if not rp.exists():
        return _skip(nm, ds, _CR, ar[:1], "representation_recurrence.jsonl nao encontrado")
    law = _ids(cd / "lawyer_entity.jsonl", "lawyer_id")
    par = _ids(cd / "party.jsonl", "party_id")
    rs = _multi_ids(rp, ["lawyer_id", "party_id"])
    ol, op = rs["lawyer_id"] - law, rs["party_id"] - par
    nl, np_ = len(rs["lawyer_id"]), len(rs["party_id"])
    oc = len(ol) + len(op)
    sa = [f"lawyer_id={x}" for x in sorted(ol)[:3]] + [f"party_id={x}" for x in sorted(op)[:2]]
    st = "PASS" if oc == 0 else "FAIL"
    sg = "Verificar se curate counsel/party rodou antes de analytics recurrence" if oc else ""
    dt = f"Advogados: {nl} ref, {len(ol)} órfãos; Partes: {np_} ref, {len(op)} órfãos"
    ob = f"{nl} lawyer refs ({len(ol)} orphans), {np_} party refs ({len(op)} orphans)"
    return _c(nm, ds, _CR, st, ar, ob, "0 orphans", dt, sa[:5], sg)


def check_critical_field_coverage(cd: Path, ad: Path) -> list[CheckResult]:
    specs: list[tuple[Path, str, str, float]] = [
        (cd / "decision_event.jsonl", "current_rapporteur", "decision_event.current_rapporteur", 0.95),
        (cd / "process.jsonl", "process_class", "process.process_class", 0.95),
        (ad / "outlier_alert.jsonl", "alert_score", "outlier_alert.alert_score", 0.95),
    ]
    out: list[CheckResult] = []
    for path, fld, label, thr in specs:
        cn = f"field_coverage_{label}"
        if not path.exists():
            out.append(_skip(cn, f"Cobertura de {label}", _HI, [path.name], f"{path.name} nao encontrado"))
            continue
        t, nn = _coverage(path, fld)
        r = _pct(nn, t)
        st = "PASS" if r >= thr else "FAIL"
        sg = f"Campo {label} com cobertura abaixo de {thr * 100:.0f}%" if st == "FAIL" else ""
        out.append(
            _c(
                cn,
                f"Cobertura de {label}",
                _HI,
                st,
                [path.name],
                f"{t} records, {nn} non-null ({r * 100:.2f}%)",
                f"{thr * 100:.0f}%",
                f"{t} registros, {nn} preenchidos ({r * 100:.2f}%)",
                sg=sg,
            )
        )
    return out


def check_output_cardinality_sanity(cd: Path, ad: Path) -> list[CheckResult]:
    out: list[CheckResult] = []
    for tag, path, ds in [
        ("decision_event", cd / "decision_event.jsonl", "Eventos de decisão devem existir"),
        ("process", cd / "process.jsonl", "Processos devem existir"),
    ]:
        cn = f"cardinality_{tag}"
        if not path.exists():
            out.append(_skip(cn, ds, _ME, [path.name], f"{path.name} nao encontrado"))
            continue
        n = _lines(path)
        st = "PASS" if n > 0 else "FAIL"
        sg = f"{path.name} vazio — pipeline pode não ter sido executado" if n == 0 else ""
        out.append(_c(cn, ds, _ME, st, [path.name], n, "> 0", f"{n} registros", sg=sg))

    bp, alp = ad / "baseline.jsonl", ad / "outlier_alert.jsonl"
    if bp.exists():
        bn, an = _lines(bp), _lines(alp) if alp.exists() else 0
        st = "PASS" if an > 0 or bn == 0 else "FAIL"
        sg = "Pipeline build-alerts pode não ter executado" if st == "FAIL" else ""
        out.append(
            _c(
                "cardinality_alerts_vs_baseline",
                "Alertas quando baseline presente",
                _ME,
                st,
                ["baseline.jsonl", "outlier_alert.jsonl"],
                f"baseline={bn}, alerts={an}",
                "alerts > 0 quando baseline > 0",
                f"{bn} baselines, {an} alertas",
                sg=sg,
            )
        )

    pp, ep = cd / "process.jsonl", cd / "representation_edge.jsonl"
    if pp.exists():
        pn, en = _lines(pp), _lines(ep) if ep.exists() else 0
        st = "PASS" if en > 0 or pn == 0 else "FAIL"
        sg = "Pipeline curate representation pode não ter executado" if st == "FAIL" else ""
        out.append(
            _c(
                "cardinality_edges_vs_process",
                "Arestas quando processos presentes",
                _ME,
                st,
                ["process.jsonl", "representation_edge.jsonl"],
                f"processes={pn}, edges={en}",
                "edges > 0 quando processes > 0",
                f"{pn} processos, {en} arestas",
                sg=sg,
            )
        )
    return out


# -- runner ------------------------------------------------------------------


def run_validation(
    curated_dir: Path = Path("data/curated"),
    analytics_dir: Path = Path("data/analytics"),
    scope: str = "all",
    fail_on_medium: bool = False,
) -> ValidationReport:
    checks: list[CheckResult] = []
    rc = scope in ("all", "curated")
    ra = scope in ("all", "analytics")
    if rc or ra:
        checks.append(check_alert_referential_integrity(curated_dir, analytics_dir))
    if rc:
        checks.append(check_representation_edge_party_coverage(curated_dir))
        checks.append(check_session_event_rapporteur_coverage(curated_dir))
        checks.append(check_representation_event_sanity(curated_dir))
    if rc or ra:
        checks.append(check_recurrence_referential_integrity(curated_dir, analytics_dir))
        checks.extend(check_critical_field_coverage(curated_dir, analytics_dir))
        checks.extend(check_output_cardinality_sanity(curated_dir, analytics_dir))
    fsev = {"CRITICAL", "HIGH"} | ({"MEDIUM"} if fail_on_medium else set())
    fail = any(c.status == "FAIL" and c.severity in fsev for c in checks)
    return ValidationReport(datetime.now(timezone.utc).isoformat(), scope, "FAIL" if fail else "PASS", checks)


# -- CLI ---------------------------------------------------------------------

_SYM = {
    "PASS": "\033[32mPASS\033[0m",
    "FAIL": "\033[31mFAIL\033[0m",
    "WARN": "\033[33mWARN\033[0m",
    "SKIP": "\033[90mSKIP\033[0m",
}


def _print_report(rpt: ValidationReport) -> None:
    print(f"\nPipeline Integrity Validation\n{'=' * 50}")
    print(f"Scope: {rpt.scope} | Timestamp: {rpt.timestamp}\n")
    for c in rpt.checks:
        print(f"  [{_SYM.get(c.status, c.status)}] {c.name}\n    {c.details}")
        if c.threshold is not None:
            print(f"    Threshold: {c.threshold}")
        if c.samples:
            print(f"    Amostras: {', '.join(c.samples[:5])}")
        if c.suggestion:
            print(f"    Sugestao: {c.suggestion}")
        print()
    counts: dict[str, int] = {}
    cf = hf = 0
    for c in rpt.checks:
        counts[c.status] = counts.get(c.status, 0) + 1
        if c.status == "FAIL":
            cf += c.severity == "CRITICAL"
            hf += c.severity == "HIGH"
    print("=" * 50)
    parts = [f"{counts[s]} {s}" for s in ("PASS", "FAIL", "WARN", "SKIP") if counts.get(s)]
    print(f"{len(rpt.checks)} checks: {', '.join(parts)}")
    det = f"({cf} CRITICAL, {hf} HIGH failures)" if rpt.status == "FAIL" else ""
    print(f"Status: {_SYM.get(rpt.status, rpt.status)} {det}\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Valida integridade referencial e cobertura dos artefatos do pipeline")
    ap.add_argument("--scope", choices=["all", "curated", "analytics"], default="all")
    ap.add_argument("--json", action="store_true", dest="json_output")
    ap.add_argument("--output", type=Path, default=None)
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--fail-on-medium", action="store_true")
    ap.add_argument("--curated-dir", type=Path, default=Path("data/curated"))
    ap.add_argument("--analytics-dir", type=Path, default=Path("data/analytics"))
    args = ap.parse_args()

    rpt = run_validation(args.curated_dir, args.analytics_dir, args.scope, args.fail_on_medium)
    if args.json_output:
        print(json.dumps(rpt.to_dict(), indent=2, ensure_ascii=False))
    else:
        _print_report(rpt)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(rpt.to_dict(), fh, indent=2, ensure_ascii=False)
        if not args.json_output:
            print(f"Relatório salvo em {args.output}")
    if args.strict:
        sys.exit(1 if any(c.status == "FAIL" for c in rpt.checks) else 0)
    else:
        sys.exit(1 if rpt.status == "FAIL" else 0)


if __name__ == "__main__":
    main()
