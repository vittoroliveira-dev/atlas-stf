import { fetchApiJson } from "@/lib/api-client";

export type TemporalOverviewSummary = {
  total_records: number;
  counts_by_kind: Record<string, number>;
  ministers_covered: number;
  events_covered: number;
  rolling_window_months: number;
  event_window_days: number;
};

export type TemporalMinisterSummary = {
  rapporteur: string;
  record_count: number;
  breakpoint_count: number;
  latest_decision_month: string | null;
  latest_breakpoint_month: string | null;
};

export type TemporalMonthlyItem = {
  record_id: string;
  rapporteur: string | null;
  decision_month: string | null;
  decision_year: number | null;
  decision_count: number;
  favorable_count: number;
  unfavorable_count: number;
  favorable_rate: number | null;
  rolling_favorable_rate_6m: number | null;
  breakpoint_score: number | null;
  breakpoint_flag: boolean | null;
  generated_at: string | null;
};

export type TemporalYoyItem = {
  record_id: string;
  rapporteur: string | null;
  process_class: string | null;
  decision_year: number | null;
  decision_count: number;
  favorable_count: number;
  unfavorable_count: number;
  current_favorable_rate: number | null;
  favorable_rate: number | null;
  prior_decision_count: number | null;
  prior_favorable_rate: number | null;
  delta_vs_prior_year: number | null;
  generated_at: string | null;
};

export type TemporalSeasonalityItem = {
  record_id: string;
  rapporteur: string | null;
  month_of_year: number | null;
  decision_count: number;
  favorable_count: number;
  unfavorable_count: number;
  favorable_rate: number | null;
  delta_vs_overall: number | null;
  generated_at: string | null;
};

export type TemporalEventItem = {
  record_id: string;
  rapporteur: string | null;
  event_id: string | null;
  event_type: string | null;
  event_scope: string | null;
  event_date: string | null;
  event_title: string | null;
  source: string | null;
  source_url: string | null;
  status: string | null;
  before_decision_count: number | null;
  before_favorable_rate: number | null;
  after_decision_count: number | null;
  after_favorable_rate: number | null;
  delta_before_after: number | null;
  decision_count: number;
  favorable_count: number;
  unfavorable_count: number;
  generated_at: string | null;
};

export type TemporalCorporateLinkItem = {
  record_id: string;
  rapporteur: string | null;
  linked_entity_type: string | null;
  linked_entity_id: string | null;
  linked_entity_name: string | null;
  company_cnpj_basico: string | null;
  company_name: string | null;
  link_degree: number | null;
  link_chain: string | null;
  link_start_date: string | null;
  link_status: string | null;
  decision_count: number;
  favorable_count: number;
  unfavorable_count: number;
  favorable_rate: number | null;
  generated_at: string | null;
};

export type TemporalAnalysisOverview = {
  summary: TemporalOverviewSummary;
  minister_summaries: TemporalMinisterSummary[];
  breakpoints: TemporalMonthlyItem[];
  seasonality: TemporalSeasonalityItem[];
  events: TemporalEventItem[];
};

export type TemporalAnalysisMinister = {
  minister: string;
  rapporteur: string | null;
  monthly: TemporalMonthlyItem[];
  yoy: TemporalYoyItem[];
  seasonality: TemporalSeasonalityItem[];
  events: TemporalEventItem[];
  corporate_links: TemporalCorporateLinkItem[];
};

export async function getTemporalAnalysisOverview(query?: {
  minister?: string;
  processClass?: string;
  analysisKind?: string;
  eventType?: string;
}): Promise<TemporalAnalysisOverview> {
  return fetchApiJson<TemporalAnalysisOverview>("/temporal-analysis", {
    minister: query?.minister,
    process_class: query?.processClass,
    analysis_kind: query?.analysisKind,
    event_type: query?.eventType,
  });
}

export async function getTemporalAnalysisMinister(
  minister: string,
): Promise<TemporalAnalysisMinister> {
  return fetchApiJson<TemporalAnalysisMinister>(
    `/temporal-analysis/${encodeURIComponent(minister)}`,
  );
}
