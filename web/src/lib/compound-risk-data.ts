import { fetchApiJson } from "@/lib/api-client";

export type CompoundRiskCompany = {
  company_cnpj_basico: string;
  company_name: string;
  link_degree: number;
};

export type CompoundRiskItem = {
  pair_id: string;
  minister_name: string;
  entity_type: "party" | "counsel";
  entity_id: string;
  entity_name: string;
  signal_count: number;
  signals: string[];
  red_flag: boolean;
  shared_process_count: number;
  shared_process_ids: string[];
  alert_count: number;
  alert_ids: string[];
  max_alert_score: number | null;
  max_rate_delta: number | null;
  sanction_match_count: number;
  sanction_sources: string[];
  donation_match_count: number;
  donation_total_brl: number | null;
  donation_total_scope: "donor_global_sum";
  corporate_conflict_count: number;
  corporate_conflict_ids: string[];
  corporate_companies: CompoundRiskCompany[];
  affinity_count: number;
  affinity_ids: string[];
  top_process_classes: string[];
  supporting_party_ids: string[];
  supporting_party_names: string[];
  signal_details: Record<string, Record<string, unknown>> | null;
  earliest_year: number | null;
  latest_year: number | null;
  sanction_corporate_link_count: number;
  sanction_corporate_link_ids: string[];
  sanction_corporate_min_degree: number | null;
  adjusted_rate_delta: number | null;
  has_law_firm_group: boolean;
  donor_group_has_minister_partner: boolean;
  donor_group_has_party_partner: boolean;
  donor_group_has_counsel_partner: boolean;
  min_link_degree_to_minister: number | null;
};

export type CompoundRiskHeatmapEntity = {
  entity_type: "party" | "counsel";
  entity_id: string;
  entity_name: string;
};

export type CompoundRiskHeatmapCell = {
  pair_id: string;
  minister_name: string;
  entity_type: "party" | "counsel";
  entity_id: string;
  signal_count: number;
  signals: string[];
  red_flag: boolean;
  max_alert_score: number | null;
  max_rate_delta: number | null;
  adjusted_rate_delta: number | null;
};

type PaginatedCompoundRiskResponse = {
  total: number;
  page: number;
  page_size: number;
  items: CompoundRiskItem[];
};

type CompoundRiskRedFlagsResponse = {
  items: CompoundRiskItem[];
  total: number;
};

type CompoundRiskHeatmapResponse = {
  pair_count: number;
  display_limit: number;
  ministers: string[];
  entities: CompoundRiskHeatmapEntity[];
  cells: CompoundRiskHeatmapCell[];
};

export type CompoundRiskPageData = {
  items: CompoundRiskItem[];
  total: number;
  page: number;
  pageSize: number;
};

export type CompoundRiskRedFlags = {
  items: CompoundRiskItem[];
  total: number;
};

export type CompoundRiskHeatmapData = {
  pairCount: number;
  displayLimit: number;
  ministers: string[];
  entities: CompoundRiskHeatmapEntity[];
  cells: CompoundRiskHeatmapCell[];
};

export async function getCompoundRiskPageData(params: {
  page?: number;
  pageSize?: number;
  minister?: string;
  entityType?: "party" | "counsel";
  redFlagOnly?: boolean;
} = {}): Promise<CompoundRiskPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 20;
  const payload = await fetchApiJson<PaginatedCompoundRiskResponse>("/compound-risk", {
    page,
    page_size: pageSize,
    minister: params.minister,
    entity_type: params.entityType,
    red_flag_only: params.redFlagOnly,
  });

  return {
    items: payload.items,
    total: payload.total,
    page: payload.page,
    pageSize: payload.page_size,
  };
}

export async function getCompoundRiskRedFlags(params: {
  minister?: string;
  entityType?: "party" | "counsel";
} = {}): Promise<CompoundRiskRedFlags> {
  const payload = await fetchApiJson<CompoundRiskRedFlagsResponse>("/compound-risk/red-flags", {
    minister: params.minister,
    entity_type: params.entityType,
  });

  return {
    items: payload.items,
    total: payload.total,
  };
}

export async function getCompoundRiskHeatmapData(params: {
  limit?: number;
  minister?: string;
  entityType?: "party" | "counsel";
  redFlagOnly?: boolean;
} = {}): Promise<CompoundRiskHeatmapData> {
  const payload = await fetchApiJson<CompoundRiskHeatmapResponse>("/compound-risk/heatmap", {
    limit: params.limit ?? 20,
    minister: params.minister,
    entity_type: params.entityType,
    red_flag_only: params.redFlagOnly,
  });

  return {
    pairCount: payload.pair_count,
    displayLimit: payload.display_limit,
    ministers: payload.ministers,
    entities: payload.entities,
    cells: payload.cells,
  };
}
