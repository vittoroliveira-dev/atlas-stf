type CollegiateFilter = "all" | "colegiado" | "monocratico";
type RelationLevel = "process_level" | "decision_derived" | "incerto";

export type Dictionary = Record<string, number>;

export type SegmentPoint = {
  date: string;
  event_count: number;
  delta_vs_historical_average: number;
  ratio_vs_historical_average: number;
};

export type SegmentFlow = {
  segment_value: string;
  event_count: number;
  process_count: number;
  active_day_count: number;
  historical_event_count: number;
  historical_active_day_count: number;
  historical_average_events_per_active_day: number;
  daily_counts: SegmentPoint[];
};

export type MinisterFlowRecord = {
  minister_query: string;
  minister_match_mode: string;
  minister_reference: string | null;
  period: string;
  status: "ok" | "empty";
  collegiate_filter: CollegiateFilter;
  event_count: number;
  process_count: number;
  active_day_count: number;
  first_decision_date: string | null;
  last_decision_date: string | null;
  historical_reference_period_start: string | null;
  historical_reference_period_end: string | null;
  historical_event_count: number;
  historical_active_day_count: number;
  historical_average_events_per_active_day: number;
  linked_alert_count: number;
  thematic_key_rule: string;
  thematic_source_distribution: Dictionary;
  historical_thematic_source_distribution: Dictionary;
  thematic_flow_interpretation_status: "comparativo" | "inconclusivo";
  thematic_flow_interpretation_reasons: string[];
  decision_type_distribution: Dictionary;
  decision_progress_distribution: Dictionary;
  judging_body_distribution: Dictionary;
  collegiate_distribution: Dictionary;
  process_class_distribution: Dictionary;
  thematic_distribution: Dictionary;
  daily_counts: SegmentPoint[];
  decision_type_flow: SegmentFlow[];
  judging_body_flow: SegmentFlow[];
  decision_progress_flow: SegmentFlow[];
  process_class_flow: SegmentFlow[];
  thematic_flow: SegmentFlow[];
};

export type FlowSnapshot = {
  id: string;
  slug: string;
  minister: string;
  period: string;
  collegiate: CollegiateFilter;
  generatedAt: string;
  data: MinisterFlowRecord;
};

export type AlertRecord = {
  alert_id: string;
  process_id: string;
  decision_event_id: string;
  comparison_group_id: string;
  alert_type: string;
  alert_score: number;
  ensemble_score: number | null;
  expected_pattern: string;
  observed_pattern: string;
  evidence_summary: string;
  uncertainty_note: string | null;
  status: string;
  risk_signal_count: number;
  risk_signals: string[];
  created_at: string | null;
  updated_at: string | null;
};

export type CaseRow = {
  processId: string;
  processNumber: string;
  processClass: string;
  decisionEventId: string;
  decisionDate: string;
  decisionType: string;
  decisionProgress: string;
  judgingBody: string;
  collegiateLabel: string;
  branchOfLaw: string;
  firstSubject: string;
  inteiroTeorUrl: string | null;
  docCountLabel: string;
  acordaoLabel: string;
  monocraticDecisionLabel: string;
  originDescription: string;
  decisionNoteSnippet: string;
};

export type AlertDetail = AlertRecord & {
  processNumber: string;
  processClass: string;
  decisionDate: string;
  decisionType: string;
  decisionProgress: string;
  judgingBody: string;
  collegiateLabel: string;
  inteiroTeorUrl: string | null;
  docCountLabel: string;
  acordaoLabel: string;
  monocraticDecisionLabel: string;
  branchOfLaw: string;
  firstSubject: string;
  originDescription: string;
  decisionNoteSnippet: string;
};

export type EntitySummary = {
  id: string;
  name_raw: string;
  name_normalized: string;
  associated_event_count: number;
  distinct_process_count: number;
  relation_level: RelationLevel;
  role_labels: string[];
};

export type DashboardData = {
  selectedSnapshot: FlowSnapshot;
  snapshots: FlowSnapshot[];
  ministers: string[];
  periods: string[];
  collegiates: CollegiateFilter[];
  judgingBodies: string[];
  processClasses: string[];
  kpis: {
    alertCount: number;
    validGroupCount: number;
    baselineCount: number;
    averageAlertScore: number;
    selectedEvents: number;
    selectedProcesses: number;
  };
  sourceFiles: Array<{ label: string; path: string; checksum: string; updatedAt: string }>;
  ministerProfiles: Array<{
    minister: string;
    period: string;
    collegiate: string;
    eventCount: number;
    historicalAverage: number;
    linkedAlertCount: number;
    processClasses: string[];
    themes: string[];
  }>;
  topAlerts: AlertDetail[];
  caseRows: CaseRow[];
  topCounsels: EntitySummary[];
  topParties: EntitySummary[];
};

export type AlertsPageData = DashboardData & {
  alertDetails: AlertDetail[];
  filteredAlertCount: number;
  totalAlertCount: number;
};

export type MinisterCorrelation = {
  minister: string;
  associated_event_count: number;
  distinct_process_count: number;
  relation_level: RelationLevel;
  role_labels: string[];
};

export type EntityListPageData = DashboardData & {
  entityKind: "counsel" | "party";
  entityLabel: string;
  entities: EntitySummary[];
  filteredEntityCount: number;
  page: number;
  pageSize: number;
};

export type EntityDetailData = DashboardData & {
  entityKind: "counsel" | "party";
  entityLabel: string;
  loadError: boolean;
  selectedEntity: EntitySummary | null;
  relatedMinisters: MinisterCorrelation[];
  relatedCases: CaseRow[];
};

export type MlOutlierAnalysis = {
  decisionEventId: string;
  comparisonGroupId: string;
  mlAnomalyScore: number;
  mlRarityScore: number;
  ensembleScore: number | null;
  nFeatures: number;
  nSamples: number;
  generatedAt: string | null;
};

export type AppliedFilters = {
  minister: string | null;
  period: string | null;
  collegiate: CollegiateFilter;
  judging_body: string | null;
  process_class: string | null;
};

export type FilterOptionsResponse = {
  ministers: string[];
  periods: string[];
  collegiates: CollegiateFilter[];
  judging_bodies: string[];
  process_classes: string[];
  applied: AppliedFilters;
};

export type SourceAuditItem = {
  label: string;
  category: string;
  path: string;
  checksum: string;
  updated_at: string;
};

export type MetricsSummary = {
  alert_count: number;
  valid_group_count: number;
  baseline_count: number;
  average_alert_score: number;
  selected_events: number;
  selected_processes: number;
};

export type MinisterProfileItem = {
  minister: string;
  period: string;
  collegiate: CollegiateFilter;
  event_count: number;
  historical_average: number;
  linked_alert_count: number;
  process_classes: string[];
  themes: string[];
};

export type CaseSummaryItem = {
  process_id: string;
  process_number: string;
  process_class: string;
  decision_event_id: string;
  decision_date: string;
  decision_type: string;
  decision_progress: string;
  judging_body: string;
  collegiate_label: string;
  branch_of_law: string;
  first_subject: string;
  inteiro_teor_url: string | null;
  doc_count_label: string;
  acordao_label: string;
  monocratic_decision_label: string;
  origin_description: string;
  decision_note_snippet: string;
};

export type AlertSummaryItem = AlertRecord & {
  process_number: string;
  process_class: string;
  decision_date: string;
  decision_type: string;
  decision_progress: string;
  judging_body: string;
  collegiate_label: string;
  inteiro_teor_url: string | null;
  doc_count_label: string;
  acordao_label: string;
  monocratic_decision_label: string;
  branch_of_law: string;
  first_subject: string;
  origin_description: string;
  decision_note_snippet: string;
};

export type MlOutlierScoreResponse = {
  decision_event_id: string;
  comparison_group_id: string;
  ml_anomaly_score: number;
  ml_rarity_score: number;
  ensemble_score: number | null;
  n_features: number;
  n_samples: number;
  generated_at: string | null;
};

export type DashboardResponse = {
  filters: FilterOptionsResponse;
  flow: MinisterFlowRecord;
  kpis: MetricsSummary;
  source_files: SourceAuditItem[];
  minister_profiles: MinisterProfileItem[];
  top_alerts: AlertSummaryItem[];
  case_rows: CaseSummaryItem[];
  top_counsels: EntitySummary[];
  top_parties: EntitySummary[];
};

export type PaginatedAlertsResponse = {
  filters: FilterOptionsResponse;
  flow: MinisterFlowRecord;
  source_files: SourceAuditItem[];
  total: number;
  page: number;
  page_size: number;
  items: AlertSummaryItem[];
  top_counsels: EntitySummary[];
  top_parties: EntitySummary[];
};

export type CaseDetailResponse = {
  filters: FilterOptionsResponse;
  flow: MinisterFlowRecord;
  source_files: SourceAuditItem[];
  case_item: CaseSummaryItem | null;
  ml_outlier_analysis: {
    decision_event_id: string;
    comparison_group_id: string;
    ml_anomaly_score: number;
    ml_rarity_score: number;
    ensemble_score: number | null;
    n_features: number;
    n_samples: number;
    generated_at: string | null;
  } | null;
  related_alerts: AlertSummaryItem[];
  counsels: EntitySummary[];
  parties: EntitySummary[];
};

export type MinisterCorrelationItem = {
  minister: string;
  associated_event_count: number;
  distinct_process_count: number;
  relation_level: RelationLevel;
  role_labels: string[];
};

export type PaginatedEntitiesResponse = {
  filters: FilterOptionsResponse;
  total: number;
  page: number;
  page_size: number;
  items: EntitySummary[];
};

export type EntityDetailResponse = {
  filters: FilterOptionsResponse;
  entity: EntitySummary;
  ministers: MinisterCorrelationItem[];
  cases: CaseSummaryItem[];
  source_files: SourceAuditItem[];
};
