import { fetchApiJson, isNotFoundError } from "@/lib/api-client";

export type AgendaEvent = {
  event_id: string;
  minister_slug: string;
  minister_name: string;
  owner_scope: string;
  owner_role: string;
  event_date: string;
  source_time_raw: string | null;
  event_title: string;
  event_description: string | null;
  event_category: string;
  meeting_nature: string;
  has_process_ref: boolean;
  classification_confidence: number;
  relevance_track: string;
  process_refs: Array<{ class: string; number: string }>;
  process_id: string | null;
  is_own_process: boolean | null;
  institutional_role_bias_flag: boolean;
};

export type AgendaExposure = {
  exposure_id: string;
  agenda_event_id: string;
  minister_slug: string;
  process_id: string | null;
  process_class: string | null;
  agenda_date: string;
  decision_date: string | null;
  days_between: number | null;
  window: string;
  is_own_process: boolean;
  event_title: string | null;
  decision_type: string | null;
  priority_score: number;
  priority_tier: string;
  coverage_comparability: string;
};

export type AgendaMinisterSummary = {
  minister_slug: string;
  minister_name: string;
  total_events: number;
  private_advocacy_count: number;
  track_a_count: number;
  coverage_months: number;
  avg_coverage_ratio: number;
};

type AgendaSummary = {
  total_events: number;
  total_ministerial_events: number;
  total_private_advocacy: number;
  total_with_process_ref: number;
  ministers_covered: number;
  total_exposures: number;
  high_priority_exposures: number;
  methodology_note: string;
  disclaimer: string;
};

type PaginatedExposures = { total: number; page: number; page_size: number; items: AgendaExposure[] };

export type AgendaPageData = {
  ministers: AgendaMinisterSummary[];
  summary: AgendaSummary;
  exposures: AgendaExposure[];
  exposureTotal: number;
};

export async function getAgendaPageData(): Promise<AgendaPageData> {
  const [ministers, summary, expData] = await Promise.all([
    fetchApiJson<AgendaMinisterSummary[]>("/agenda/ministers"),
    fetchApiJson<AgendaSummary>("/agenda/summary"),
    fetchApiJson<PaginatedExposures>("/agenda/exposures", { page: 1, page_size: 10, priority_tier: "high" }),
  ]);
  return { ministers, summary, exposures: expData.items, exposureTotal: expData.total };
}

export type MinisterDetailData = {
  events: AgendaEvent[];
  eventTotal: number;
  exposures: AgendaExposure[];
  coverages: Array<{ coverage_id: string; minister_slug: string; year: number; month: number; event_count: number; coverage_ratio: number; comparability_tier: string; court_recess_flag: boolean; publication_gap_flag: boolean }>;
  ministerName: string;
};

export async function getMinisterDetailData(slug: string, params: { page?: number } = {}): Promise<MinisterDetailData | null> {
  try {
    const detail = await fetchApiJson<{ events: { total: number; items: AgendaEvent[] }; exposures: AgendaExposure[]; coverages: MinisterDetailData["coverages"]; minister_name: string }>(`/agenda/ministers/${slug}`, { page: params.page ?? 1, page_size: 20 });
    return { events: detail.events.items, eventTotal: detail.events.total, exposures: detail.exposures, coverages: detail.coverages, ministerName: detail.minister_name };
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error; // unreachable — isNotFoundError throws non-404
  }
}
