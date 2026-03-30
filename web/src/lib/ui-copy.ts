export const ML_ANOMALY_THRESHOLD = 0.8;

export function formatDateSafe(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("pt-BR");
}

export function labelCollegiateFilterHuman(value: string | undefined): string {
  if (value === "colegiado") return "Decisões colegiadas";
  if (value === "monocratico") return "Decisões individuais";
  return "Todas as decisões";
}

export function relationLabelHuman(
  level: "process_level" | "decision_derived" | "incerto",
): string {
  if (level === "decision_derived") return "ligação com o mesmo caso";
  if (level === "incerto") return "ligação que pede contexto";
  return "ligação no mesmo processo";
}

export function relationHelperText(
  level: "process_level" | "decision_derived" | "incerto",
): string {
  if (level === "decision_derived") {
    return "Este nome aparece ligado ao caso analisado no mesmo contexto de decisão.";
  }
  if (level === "incerto") {
    return "Há relação observada, mas ela precisa de leitura adicional para ser entendida com segurança.";
  }
  return "Este nome aparece no mesmo processo analisado.";
}

export function alertTypeLabel(value: string): string {
  if (value === "atipicidade") return "desvio estatístico";
  if (value === "inconclusivo") return "sem classificação";
  return value.replace(/_/g, " ");
}

export function alertStatusLabel(value: string): string {
  if (value === "novo") return "novo no painel";
  if (value === "inconclusivo") return "sem classificação";
  return value.replace(/_/g, " ");
}

const FIELD_NAME_MAP: Record<string, string> = {
  decision_progress: "andamento da decisão",
  current_rapporteur: "ministro relator",
  judging_body: "órgão julgador",
  decision_type: "tipo de decisão",
  process_class: "tipo de ação",
  is_collegiate: "tipo de sessão",
  branch_of_law: "ramo do direito",
  thematic_key: "tema",
};

export function humanizePattern(raw: string): string {
  let result = raw;
  for (const [field, label] of Object.entries(FIELD_NAME_MAP)) {
    result = result.replaceAll(field, label);
  }
  result = result.replaceAll("tende a", "tende a ser");
  result = result.replaceAll(";", " ·");
  return result;
}

export function interpretationTitle(status: "comparativo" | "inconclusivo"): string {
  if (status === "inconclusivo") {
    return "Ainda não há contexto suficiente para uma leitura segura";
  }
  return "Há base suficiente para comparar este período";
}

export function interpretationSummary(status: "comparativo" | "inconclusivo"): string {
  if (status === "inconclusivo") {
    return "Os dados ajudam a orientar a leitura, mas ainda não sustentam comparação temática mais firme.";
  }
  return "Os dados do período já permitem uma leitura comparativa inicial sem esconder os limites da análise.";
}

export function interpretationReasonText(reason: string): string {
  switch (reason) {
    case "event_count_lt_5":
      return "há poucos registros no período";
    case "active_day_count_lt_3":
      return "o período teve poucos dias com atividade";
    case "historical_event_count_lt_20":
      return "o histórico ainda é curto para comparação";
    default:
      return "há pouco contexto para aprofundar esta leitura";
  }
}

export function sourceLabelHuman(label: string): string {
  switch (label) {
    case "process":
      return "Dados principais do processo";
    case "decision_event":
      return "Dados da decisão";
    case "party":
      return "Pessoas e organizações ligadas ao caso";
    case "process_party_link":
      return "Papel das partes no caso";
    case "counsel":
      return "Representantes identificados";
    case "process_counsel_link":
      return "Vínculo entre representantes e caso";
    case "outlier_alert":
      return "Pontos de atenção encontrados";
    case "outlier_alert_summary":
      return "Resumo dos pontos de atenção";
    case "comparison_group_summary":
      return "Resumo dos grupos de comparação";
    case "baseline_summary":
      return "Resumo da referência usada na comparação";
    default:
      return "Base usada para montar esta tela";
  }
}

export function sourceDescriptionHuman(label: string): string {
  switch (label) {
    case "process":
      return "Base que reúne o número do processo, o tipo da ação e o contexto principal.";
    case "decision_event":
      return "Base que informa data, tipo de decisão e andamento.";
    case "party":
      return "Base com pessoas e organizações ligadas ao caso.";
    case "process_party_link":
      return "Base que explica como cada parte aparece no processo.";
    case "counsel":
      return "Base com nomes de representantes identificados.";
    case "process_counsel_link":
      return "Base que mostra como representantes aparecem no caso.";
    case "outlier_alert":
      return "Base com ocorrências fora do padrão esperado para revisão humana.";
    case "outlier_alert_summary":
      return "Resumo geral dos sinais mostrados no painel.";
    case "comparison_group_summary":
      return "Resumo dos grupos usados como referência de comparação.";
    case "baseline_summary":
      return "Resumo da linha de base usada para dar contexto aos números.";
    default:
      return "Informação de apoio usada para montar esta visão.";
  }
}

export function fieldLabel(field: string): string {
  switch (field) {
    case "red_flag": return "Ponto critico";
    case "favorable_rate": return "Resultado favoravel";
    case "favorable_rate_delta": return "Diferenca da media";
    case "baseline_minister": return "Media do ministro";
    case "baseline_counsel": return "Media do advogado";
    case "stf_case_count": return "Processos no STF";
    case "sanction_source": return "Origem da sancao";
    case "risk_score": return "Indice de relevancia";
    case "total_donated_brl": return "Total doado (global do doador)";
    case "election_years": return "Eleicoes";
    case "parties_donated_to": return "Partidos";
    case "link_degree": return "Grau de vinculo";
    case "shared_case_count": return "Processos em comum";
    case "pair_favorable_rate": return "Taxa do par";
    default: return field.replace(/_/g, " ");
  }
}

export function fieldTooltip(field: string): string {
  switch (field) {
    case "red_flag": return "Resultado fora do padrao esperado";
    case "favorable_rate": return "Percentual de decisoes favoraveis no STF";
    case "favorable_rate_delta": return "Pontos percentuais acima ou abaixo da media";
    case "baseline_minister": return "Taxa geral do ministro em processos semelhantes";
    case "baseline_counsel": return "Taxa geral do advogado com qualquer ministro";
    case "stf_case_count": return "Quantidade de processos no STF";
    case "sanction_source": return "Cadastro onde a sancao foi registrada";
    case "risk_score": return "Combinacao de delta e distancia empresarial";
    default: return "";
  }
}

export function sanctionSourceLabel(source: string): string {
  switch (source.toLowerCase()) {
    case "ceis": return "CEIS";
    case "cnep": return "CNEP";
    case "cvm": return "CVM";
    case "leniencia": return "Leniencia";
    default: return source.toUpperCase();
  }
}

export function signalLabelSimple(signal: string): string {
  switch (signal) {
    case "sanction": return "Sancao";
    case "donation": return "Doacao";
    case "corporate": return "Vinculo";
    case "affinity": return "Afinidade";
    case "alert": return "Alerta";
    default: return signal;
  }
}

export function velocityFlagLabel(flag: string | null): string {
  if (flag === "queue_jump") return "Fura-fila";
  if (flag === "stalled") return "Parado";
  return "Normal";
}

export function velocityFlagColor(flag: string | null): string {
  if (flag === "queue_jump") return "text-red-700 bg-red-50 border-red-200";
  if (flag === "stalled") return "text-amber-700 bg-amber-50 border-amber-200";
  return "text-slate-600 bg-slate-50 border-slate-200";
}

export function emptyStateMessage(context: string): string {
  switch (context) {
    case "sanctions": return "Nenhuma entidade sancionada encontrada. Tente remover algum filtro para ampliar a busca.";
    case "donations": return "Nenhum doador encontrado. Tente remover algum filtro para ampliar a busca.";
    case "corporate": return "Nenhum vinculo empresarial encontrado. Tente remover algum filtro para ampliar a busca.";
    case "affinity": return "Nenhum par com afinidade atipica encontrado. Tente remover algum filtro para ampliar a busca.";
    case "convergence": return "Nenhum par com sinais combinados encontrado. Tente remover algum filtro para ampliar a busca.";
    case "velocity": return "Nenhuma anomalia de velocidade encontrada. Tente remover algum filtro para ampliar a busca.";
    case "redistribution": return "Nenhuma mudanca de relatoria encontrada. Tente remover algum filtro para ampliar a busca.";
    case "counsel_network": return "Nenhum cluster de advogados encontrado. Tente remover algum filtro para ampliar a busca.";
    default: return "Nenhum resultado encontrado. Tente remover algum filtro para ampliar a busca.";
  }
}

export function riskBadgeLabel(hasFlag: boolean): { text: string; ariaLabel: string } {
  return hasFlag
    ? { text: "Ponto critico", ariaLabel: "Ponto critico: resultado fora do padrao esperado" }
    : { text: "Normal", ariaLabel: "Sem ponto critico" };
}

export function deltaAriaLabel(value: number | null): string {
  if (value == null) return "Diferenca da media nao disponivel";
  const pp = value * 100;
  const sign = pp > 0 ? "+" : "";
  return `Diferenca da media: ${sign}${pp.toFixed(1)} pontos percentuais`;
}
