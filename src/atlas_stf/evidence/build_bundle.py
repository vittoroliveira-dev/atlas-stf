"""Build evidence bundles for outlier alerts."""

from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..audit_gates import build_alert_gate_status_payload, build_score_details_payload
from ..schema_validate import validate_records

DEFAULT_ALERT_PATH = Path("data/analytics/outlier_alert.jsonl")
DEFAULT_BASELINE_PATH = Path("data/analytics/baseline.jsonl")
DEFAULT_GROUP_PATH = Path("data/analytics/comparison_group.jsonl")
DEFAULT_DECISION_EVENT_PATH = Path("data/curated/decision_event.jsonl")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_EVIDENCE_DIR = Path("data/evidence")
DEFAULT_REPORT_DIR = Path("reports/anomaly-reports")
DEFAULT_RAPPORTEUR_PROFILE_PATH = Path("data/analytics/rapporteur_profile.jsonl")
DEFAULT_SEQUENTIAL_PATH = Path("data/analytics/sequential_analysis.jsonl")
DEFAULT_ASSIGNMENT_AUDIT_PATH = Path("data/analytics/assignment_audit.jsonl")
SCHEMA_PATH = Path("schemas/evidence_bundle.schema.json")
EVIDENCE_VERSION = "evidence-bundle-v3"


def _read_jsonl_map(path: Path, key: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number} contains invalid JSON") from exc
            if key not in row:
                raise ValueError(f"{path}:{line_number} missing required key '{key}'")
            result[str(row[key])] = row
    return result


def _read_jsonl_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    result: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number} contains invalid JSON") from exc
            result.append(row)
    return result


def _nonempty_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _load_optional_analytics(
    rapporteur_profile_path: Path,
    sequential_path: Path,
    assignment_audit_path: Path,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    rp_rows = _read_jsonl_list(rapporteur_profile_path)
    rp_index: dict[str, list[dict[str, Any]]] = {}
    for row in rp_rows:
        key = _nonempty_string(row.get("rapporteur"))
        if key is None:
            continue
        rp_index.setdefault(key, []).append(row)

    seq_rows = _read_jsonl_list(sequential_path)
    seq_index: dict[str, list[dict[str, Any]]] = {}
    for row in seq_rows:
        key = _nonempty_string(row.get("rapporteur"))
        if key is None:
            continue
        seq_index.setdefault(key, []).append(row)

    aa_rows = _read_jsonl_list(assignment_audit_path)
    aa_index: dict[str, list[dict[str, Any]]] = {}
    for row in aa_rows:
        key = f"{row.get('process_class', '')}|{row.get('decision_year', '')}"
        aa_index.setdefault(key, []).append(row)

    return rp_index, seq_index, aa_index


def _match_analytics_context(
    event: dict[str, Any],
    process: dict[str, Any],
    rp_index: dict[str, list[dict[str, Any]]],
    seq_index: dict[str, list[dict[str, Any]]],
    aa_index: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    rapporteur = _nonempty_string(event.get("current_rapporteur"))
    process_class = str(process.get("process_class", ""))
    decision_year = event.get("decision_year")

    matched_rp = None
    if rapporteur is not None:
        for row in rp_index.get(rapporteur, []):
            if row.get("process_class") == process_class and row.get("decision_year") == decision_year:
                matched_rp = row
                break

    matched_seq = None
    if rapporteur is not None:
        for row in seq_index.get(rapporteur, []):
            if row.get("decision_year") == decision_year:
                matched_seq = row
                break

    aa_key = f"{process_class}|{decision_year}"
    matched_aa = aa_index.get(aa_key, [None])[0]  # type: ignore[list-item]

    return {
        "rapporteur_profile": matched_rp,
        "sequential_analysis": matched_seq,
        "assignment_audit": matched_aa,
    }


def _resolve_minister(event: dict[str, Any]) -> str:
    rapporteur = event.get("current_rapporteur")
    return str(rapporteur) if rapporteur else "INCERTO"


def _bundle_payload(
    alert: dict[str, Any],
    decision_event: dict[str, Any],
    process: dict[str, Any],
    baseline: dict[str, Any],
    comparison_group: dict[str, Any],
    analytics_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    score_details = build_score_details_payload(decision_event, baseline)
    comparison_group_id = alert.get("comparison_group_id")
    payload: dict[str, Any] = {
        "bundle_version": EVIDENCE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "alert": alert,
        "decision_event": decision_event,
        "process": process,
        "baseline": baseline,
        "comparison_group": comparison_group,
        "score_details": score_details,
        "gate_status": build_alert_gate_status_payload(
            alert,
            score_details,
            comparison_group_id=comparison_group_id if isinstance(comparison_group_id, str) else None,
            baseline=baseline,
        ),
        "analysis_context": {
            "process_number": process.get("process_number"),
            "minister": _resolve_minister(decision_event),
            "decision_date": decision_event.get("decision_date"),
            "decision_type": decision_event.get("decision_type"),
            "group_rule_version": comparison_group.get("rule_version"),
            "baseline_event_count": baseline.get("event_count"),
        },
        "analysis_prompts": [
            "Confirmar se o caso permanece comparável ao grupo utilizado.",
            "Confirmar se o baseline é suficientemente estável para interpretação.",
            "Produzir síntese descritiva sem extrapolar o que os dados suportam.",
        ],
    }
    if analytics_context and any(v is not None for v in analytics_context.values()):
        payload["advanced_analytics"] = analytics_context
    return payload


def _resolve_bundle_inputs(
    *,
    alert_path: Path,
    baseline_path: Path,
    comparison_group_path: Path,
    decision_event_path: Path,
    process_path: Path,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    return (
        _read_jsonl_map(alert_path, "alert_id"),
        _read_jsonl_map(baseline_path, "comparison_group_id"),
        _read_jsonl_map(comparison_group_path, "comparison_group_id"),
        _read_jsonl_map(decision_event_path, "decision_event_id"),
        _read_jsonl_map(process_path, "process_id"),
    )


def _build_evidence_bundle_from_maps(
    alert_id: str,
    *,
    alerts: dict[str, dict[str, Any]],
    baselines: dict[str, dict[str, Any]],
    groups: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    processes: dict[str, dict[str, Any]],
    evidence_dir: Path,
    report_dir: Path,
    rp_index: dict[str, list[dict[str, Any]]] | None = None,
    seq_index: dict[str, list[dict[str, Any]]] | None = None,
    aa_index: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[Path, Path]:
    if alert_id not in alerts:
        raise ValueError(f"alert_id not found: {alert_id}")

    alert = alerts[alert_id]
    comparison_group_id = alert.get("comparison_group_id")
    if comparison_group_id is None:
        raise ValueError(f"alert without comparison_group_id: {alert_id}")

    decision_event_id = alert["decision_event_id"]
    process_id = alert["process_id"]
    if decision_event_id not in events:
        raise ValueError(f"decision_event_id not found for alert {alert_id}: {decision_event_id}")
    if process_id not in processes:
        raise ValueError(f"process_id not found for alert {alert_id}: {process_id}")
    if comparison_group_id not in baselines:
        raise ValueError(f"baseline not found for alert {alert_id}: {comparison_group_id}")
    if comparison_group_id not in groups:
        raise ValueError(f"comparison_group not found for alert {alert_id}: {comparison_group_id}")

    decision_event = events[decision_event_id]
    process = processes[process_id]
    baseline = baselines[comparison_group_id]
    group = groups[comparison_group_id]

    analytics_context = None
    if rp_index is not None or seq_index is not None or aa_index is not None:
        analytics_context = _match_analytics_context(
            decision_event,
            process,
            rp_index or {},
            seq_index or {},
            aa_index or {},
        )
    bundle = _bundle_payload(alert, decision_event, process, baseline, group, analytics_context)
    validate_records([bundle], SCHEMA_PATH)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    json_path = evidence_dir / f"{alert_id}.json"
    md_path = report_dir / f"{alert_id}.md"
    json_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(bundle), encoding="utf-8")
    return json_path, md_path


def _render_markdown(bundle: dict[str, Any]) -> str:
    alert = bundle["alert"]
    event = bundle["decision_event"]
    process = bundle["process"]
    baseline = bundle["baseline"]
    group = bundle["comparison_group"]
    analysis = bundle["analysis_context"]
    score_details = bundle["score_details"]
    gate_status = bundle["gate_status"]

    lines = [
        f"# Evidência do alerta {alert['alert_id']}",
        "",
        "## 1. Identificação",
        f"- alerta: {alert['alert_id']}",
        f"- processo: {process.get('process_number') or process['process_id']}",
        f"- ministro: {analysis['minister']}",
        f"- data da decisão: {event.get('decision_date') or 'INCERTO'}",
        f"- tipo de alerta: {alert['alert_type']}",
        f"- score: {alert.get('alert_score')}",
        f"- status operacional: {alert['status']}",
        "",
        "## 2. Objetivo",
        "Registrar, em um único artefato, o contexto mínimo necessário para análise derivada do alerta.",
        "",
        "## 3. Recorte analítico",
        f"- comparison_group_id: {group['comparison_group_id']}",
        f"- regra de comparabilidade: {group['rule_version']}",
        f"- critérios: {json.dumps(group.get('selection_criteria') or {}, ensure_ascii=False, sort_keys=True)}",
        f"- janela temporal do grupo: {group.get('time_window') or 'INCERTO'}",
        f"- tamanho do grupo: {group.get('case_count')}",
        "",
        "## 4. Baseline explícito",
        f"- baseline_id: {baseline['baseline_id']}",
        f"- eventos no baseline: {baseline.get('event_count')}",
        f"- processos no baseline: {baseline.get('process_count')}",
        f"- período observado: {baseline.get('observed_period_start')} até {baseline.get('observed_period_end')}",
        f"- notas: {baseline.get('notes') or 'INCERTO'}",
        "",
        "## 5. Padrão esperado e observado",
        f"- esperado: {alert.get('expected_pattern') or score_details.get('expected_pattern') or 'INCERTO'}",
        f"- observado: {alert.get('observed_pattern') or score_details.get('observed_pattern') or 'INCERTO'}",
        f"- evidência resumida: {alert.get('evidence_summary') or score_details.get('evidence_summary') or 'INCERTO'}",
        "",
        "## 6. Componentes do score",
        (
            "- score recomputado: "
            + str(score_details.get("alert_score") if score_details.get("alert_score") is not None else "INCERTO")
        ),
        f"- tipo recomputado: {score_details.get('alert_type') or 'INCERTO'}",
    ]

    components = score_details.get("components") or []
    if components:
        for component in components:
            lines.append(
                "- "
                + (
                    f"{component['name']}: observado='{component['observed_value']}', "
                    f"esperado='{component.get('expected_value') or 'INCERTO'}', "
                    f"probabilidade esperada={component['expected_probability']:.3f}, "
                    f"raridade={component['rarity_score']:.3f}"
                )
            )
    else:
        lines.append("- Nenhum componente reprodutível foi calculado.")

    lines.extend(
        [
            "",
            "## 7. Gates de auditoria",
            f"- possui comparison_group_id: {gate_status['has_comparison_group_id']}",
            f"- possui baseline: {gate_status['has_baseline']}",
            f"- possui padrão esperado: {gate_status['has_expected_pattern']}",
            f"- possui padrão observado: {gate_status['has_observed_pattern']}",
            f"- possui resumo de evidência: {gate_status['has_evidence_summary']}",
            f"- possui score: {gate_status['has_alert_score']}",
            f"- nota de incerteza obrigatória: {gate_status['uncertainty_note_required']}",
            f"- nota de incerteza presente: {gate_status['has_uncertainty_note']}",
            f"- linguagem neutra: {gate_status['uses_neutral_language']}",
            f"- faz acusação automática: {gate_status['makes_automatic_accusation']}",
            f"- apto para análise derivada: {gate_status['passes_for_analysis']}",
        ]
    )

    uncertainty_note = alert.get("uncertainty_note") or score_details.get("uncertainty_note")
    if uncertainty_note:
        lines.extend(
            [
                "",
                "## 8. Incertezas e limites",
                f"- {uncertainty_note}",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "## 8. Incertezas e limites",
                "- Nenhuma incerteza adicional foi registrada nesta etapa além dos limites gerais do projeto.",
            ]
        )

    advanced = bundle.get("advanced_analytics")
    if advanced:
        analytics_lines: list[str] = []
        rp = advanced.get("rapporteur_profile")
        if rp:
            flag = "SIM" if rp.get("deviation_flag") else "NÃO"
            analytics_lines.append(f"- perfil do relator: chi2={rp.get('chi2_statistic')}, desvio={flag}")
        seq = advanced.get("sequential_analysis")
        if seq:
            flag = "SIM" if seq.get("sequential_bias_flag") else "NÃO"
            autocorr = seq.get("autocorrelation_lag1")
            analytics_lines.append(f"- análise sequencial: autocorrelação={autocorr}, bias={flag}")
        aa = advanced.get("assignment_audit")
        if aa:
            flag = "SIM" if aa.get("uniformity_flag") else "NÃO"
            analytics_lines.append(f"- auditoria de distribuição: chi2={aa.get('chi2_statistic')}, uniforme={flag}")
        if analytics_lines:
            lines.extend(["", "## 9. Contexto analítico adicional", *analytics_lines])

    lines.extend(
        [
            "",
            "## 10. Próximos passos sugeridos",
            "1. Confirmar comparabilidade do caso com o grupo utilizado.",
            "2. Avaliar documentação oficial relacionada ao processo, se necessário.",
            "3. Produzir análise derivada opcional por IA ou leitura externa, se necessário.",
            "",
            "## 11. Evidência técnica",
            f"- process_id: {process['process_id']}",
            f"- decision_event_id: {event['decision_event_id']}",
            f"- source_id: {event.get('source_id') or process.get('source_id') or 'INCERTO'}",
            f"- bundle_version: {bundle['bundle_version']}",
            f"- generated_at: {bundle['generated_at']}",
        ]
    )
    return "\n".join(lines) + "\n"


def build_evidence_bundle(
    alert_id: str,
    *,
    alert_path: Path = DEFAULT_ALERT_PATH,
    baseline_path: Path = DEFAULT_BASELINE_PATH,
    comparison_group_path: Path = DEFAULT_GROUP_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    evidence_dir: Path = DEFAULT_EVIDENCE_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    rapporteur_profile_path: Path = DEFAULT_RAPPORTEUR_PROFILE_PATH,
    sequential_path: Path = DEFAULT_SEQUENTIAL_PATH,
    assignment_audit_path: Path = DEFAULT_ASSIGNMENT_AUDIT_PATH,
) -> tuple[Path, Path]:
    alerts, baselines, groups, events, processes = _resolve_bundle_inputs(
        alert_path=alert_path,
        baseline_path=baseline_path,
        comparison_group_path=comparison_group_path,
        decision_event_path=decision_event_path,
        process_path=process_path,
    )
    rp_index, seq_index, aa_index = _load_optional_analytics(
        rapporteur_profile_path,
        sequential_path,
        assignment_audit_path,
    )
    return _build_evidence_bundle_from_maps(
        alert_id,
        alerts=alerts,
        baselines=baselines,
        groups=groups,
        events=events,
        processes=processes,
        evidence_dir=evidence_dir,
        report_dir=report_dir,
        rp_index=rp_index,
        seq_index=seq_index,
        aa_index=aa_index,
    )


def build_all_evidence_bundles(
    *,
    alert_path: Path = DEFAULT_ALERT_PATH,
    baseline_path: Path = DEFAULT_BASELINE_PATH,
    comparison_group_path: Path = DEFAULT_GROUP_PATH,
    decision_event_path: Path = DEFAULT_DECISION_EVENT_PATH,
    process_path: Path = DEFAULT_PROCESS_PATH,
    evidence_dir: Path = DEFAULT_EVIDENCE_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    rapporteur_profile_path: Path = DEFAULT_RAPPORTEUR_PROFILE_PATH,
    sequential_path: Path = DEFAULT_SEQUENTIAL_PATH,
    assignment_audit_path: Path = DEFAULT_ASSIGNMENT_AUDIT_PATH,
    on_progress: Callable[[int, int], None] | None = None,
    max_workers: int = 8,
) -> list[tuple[Path, Path]]:
    alerts, baselines, groups, events, processes = _resolve_bundle_inputs(
        alert_path=alert_path,
        baseline_path=baseline_path,
        comparison_group_path=comparison_group_path,
        decision_event_path=decision_event_path,
        process_path=process_path,
    )
    rp_index, seq_index, aa_index = _load_optional_analytics(
        rapporteur_profile_path,
        sequential_path,
        assignment_audit_path,
    )

    alert_ids = sorted(alerts)
    total = len(alert_ids)

    def _write_one(alert_id: str) -> tuple[Path, Path]:
        return _build_evidence_bundle_from_maps(
            alert_id,
            alerts=alerts,
            baselines=baselines,
            groups=groups,
            events=events,
            processes=processes,
            evidence_dir=evidence_dir,
            report_dir=report_dir,
            rp_index=rp_index,
            seq_index=seq_index,
            aa_index=aa_index,
        )

    # Pre-create output directories once, before threads start, to avoid
    # TOCTOU races on mkdir inside _build_evidence_bundle_from_maps.
    evidence_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, tuple[Path, Path]] = {}
    completed = 0

    # Bounded sliding window: submit at most `window_size` futures at a time
    # to avoid holding 239k+ pending tasks in memory simultaneously.
    window_size = max_workers * 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        pending: dict[Any, str] = {}  # future -> alert_id
        it = iter(alert_ids)

        # Seed the initial window
        for aid in _take(it, window_size):
            pending[executor.submit(_write_one, aid)] = aid

        while pending:
            done, _ = _wait_first(pending)
            for future in done:
                aid = pending.pop(future)
                results[aid] = future.result()
                completed += 1
                if on_progress is not None:
                    on_progress(completed, total)

            # Refill window with new items
            for aid in _take(it, len(done)):
                pending[executor.submit(_write_one, aid)] = aid

    # Return in deterministic sorted order, matching the original behaviour.
    return [results[aid] for aid in alert_ids]


def _take(it: Any, n: int) -> list[str]:
    """Take up to n items from an iterator."""
    import itertools

    return list(itertools.islice(it, n))


def _wait_first(pending: dict[Any, str]) -> tuple[set[Any], set[Any]]:
    """Wait for at least one future to complete."""
    from concurrent.futures import FIRST_COMPLETED, wait

    return wait(pending, return_when=FIRST_COMPLETED)
