import { fetchApiJson } from "@/lib/api-client";

export type CorporateConflict = {
  conflict_id: string;
  minister_name: string;
  company_cnpj_basico: string;
  company_name: string;
  minister_qualification: string | null;
  linked_entity_type: string;
  linked_entity_id: string;
  linked_entity_name: string;
  entity_qualification: string | null;
  shared_process_ids: string[];
  shared_process_count: number;
  favorable_rate: number | null;
  baseline_favorable_rate: number | null;
  favorable_rate_delta: number | null;
  risk_score: number | null;
  decay_factor: number | null;
  red_flag: boolean;
  link_chain: string | null;
  link_degree: number;
};

type PaginatedCorporateConflictsResponse = {
  total: number;
  page: number;
  page_size: number;
  items: CorporateConflict[];
};

type CorporateConflictRedFlagsResponse = {
  items: CorporateConflict[];
  total: number;
};

export type CorporateNetworkPageData = {
  conflicts: CorporateConflict[];
  total: number;
  page: number;
  pageSize: number;
};

export type CorporateNetworkRedFlags = {
  items: CorporateConflict[];
  total: number;
};

export async function getCorporateNetworkPageData(params: {
  page?: number;
  pageSize?: number;
  minister?: string;
  redFlagOnly?: boolean;
  linkDegree?: number;
} = {}): Promise<CorporateNetworkPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  const payload = await fetchApiJson<PaginatedCorporateConflictsResponse>("/corporate-network", {
    page,
    page_size: pageSize,
    minister: params.minister,
    red_flag_only: params.redFlagOnly,
    link_degree: params.linkDegree,
  });

  return {
    conflicts: payload.items,
    total: payload.total,
    page: payload.page,
    pageSize: payload.page_size,
  };
}

export async function getCorporateNetworkRedFlags(): Promise<CorporateNetworkRedFlags> {
  const payload = await fetchApiJson<CorporateConflictRedFlagsResponse>("/corporate-network/red-flags");
  return {
    items: payload.items,
    total: payload.total,
  };
}

export async function getMinisterCorporateConflicts(minister: string): Promise<CorporateConflict[]> {
  return fetchApiJson<CorporateConflict[]>(
    `/ministers/${encodeURIComponent(minister)}/corporate-conflicts`,
  );
}
