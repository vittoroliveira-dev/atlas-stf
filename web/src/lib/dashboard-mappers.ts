import type {
  AlertDetail,
  AlertSummaryItem,
  AppliedFilters,
  CaseRow,
  CaseSummaryItem,
  Dictionary,
  FlowSnapshot,
  MinisterFlowRecord,
  SegmentFlow,
  SegmentPoint,
  SourceAuditItem,
} from "./dashboard-types";

export function toSnapshot(flow: MinisterFlowRecord, filters: AppliedFilters): FlowSnapshot {
  const minister = filters.minister ?? flow.minister_query;
  const period = filters.period ?? flow.period;
  const collegiate = filters.collegiate ?? flow.collegiate_filter;
  return {
    id: `${minister}:${period}:${collegiate}`,
    slug: minister.toLowerCase().replace(/\s+/g, "-"),
    minister,
    period,
    collegiate,
    generatedAt: new Date().toISOString(),
    data: flow,
  };
}

export function mapSourceFiles(items: SourceAuditItem[]) {
  return items.map((item) => ({
    label: item.label,
    path: item.path,
    checksum: item.checksum,
    updatedAt: item.updated_at,
  }));
}

export function mapCaseRow(item: CaseSummaryItem): CaseRow {
  return {
    processId: item.process_id,
    processNumber: item.process_number,
    processClass: item.process_class,
    decisionEventId: item.decision_event_id,
    decisionDate: item.decision_date,
    decisionType: item.decision_type,
    decisionProgress: item.decision_progress,
    judgingBody: item.judging_body,
    collegiateLabel: item.collegiate_label,
    branchOfLaw: item.branch_of_law,
    firstSubject: item.first_subject,
    inteiroTeorUrl: item.inteiro_teor_url,
    docCountLabel: item.doc_count_label,
    acordaoLabel: item.acordao_label,
    monocraticDecisionLabel: item.monocratic_decision_label,
    originDescription: item.origin_description,
    decisionNoteSnippet: item.decision_note_snippet,
  };
}

export function mapAlertDetail(item: AlertSummaryItem): AlertDetail {
  return {
    alert_id: item.alert_id,
    process_id: item.process_id,
    decision_event_id: item.decision_event_id,
    comparison_group_id: item.comparison_group_id,
    alert_type: item.alert_type,
    alert_score: item.alert_score,
    ensemble_score: item.ensemble_score,
    expected_pattern: item.expected_pattern,
    observed_pattern: item.observed_pattern,
    evidence_summary: item.evidence_summary,
    uncertainty_note: item.uncertainty_note,
    status: item.status,
    risk_signal_count: item.risk_signal_count ?? 0,
    risk_signals: item.risk_signals ?? [],
    created_at: item.created_at,
    updated_at: item.updated_at,
    processNumber: item.process_number,
    processClass: item.process_class,
    decisionDate: item.decision_date,
    decisionType: item.decision_type,
    decisionProgress: item.decision_progress,
    judgingBody: item.judging_body,
    collegiateLabel: item.collegiate_label,
    inteiroTeorUrl: item.inteiro_teor_url,
    docCountLabel: item.doc_count_label,
    acordaoLabel: item.acordao_label,
    monocraticDecisionLabel: item.monocratic_decision_label,
    branchOfLaw: item.branch_of_law,
    firstSubject: item.first_subject,
    originDescription: item.origin_description,
    decisionNoteSnippet: item.decision_note_snippet,
  };
}

export function toChartRows(dictionary: Dictionary) {
  return Object.entries(dictionary)
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);
}

export function chartRows(flow: SegmentFlow[]) {
  return flow.map((item) => ({
    name: item.segment_value,
    value: item.event_count,
    historicalAverage: Number(item.historical_average_events_per_active_day.toFixed(3)),
  }));
}

export function dailyRows(points: SegmentPoint[]) {
  return points.map((point) => ({
    date: point.date,
    eventos: point.event_count,
    mediaHistorica: Number((point.event_count - point.delta_vs_historical_average).toFixed(3)),
    razao: Number(point.ratio_vs_historical_average.toFixed(3)),
  }));
}
