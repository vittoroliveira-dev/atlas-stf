from __future__ import annotations

import json
from typing import Any


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
