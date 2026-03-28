import { fetchApiJson } from "@/lib/api-client";

export type CounselAffinity = {
  affinity_id: string;
  rapporteur: string;
  counsel_id: string;
  counsel_name_normalized: string;
  shared_case_count: number;
  favorable_count: number;
  unfavorable_count: number;
  pair_favorable_rate: number | null;
  minister_baseline_favorable_rate: number | null;
  counsel_baseline_favorable_rate: number | null;
  pair_delta_vs_minister: number | null;
  pair_delta_vs_counsel: number | null;
  red_flag: boolean;
  top_process_classes: string[];
};

type PaginatedCounselAffinityResponse = {
  total: number;
  page: number;
  page_size: number;
  items: CounselAffinity[];
};

type CounselAffinityRedFlagsResponse = {
  items: CounselAffinity[];
  total: number;
};

export type CounselAffinityPageData = {
  affinities: CounselAffinity[];
  total: number;
  page: number;
  pageSize: number;
};

export type CounselAffinityRedFlags = {
  items: CounselAffinity[];
  total: number;
};

export async function getCounselAffinityPageData(params: {
  page?: number;
  pageSize?: number;
  minister?: string;
  redFlagOnly?: boolean;
} = {}): Promise<CounselAffinityPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  const payload = await fetchApiJson<PaginatedCounselAffinityResponse>("/counsel-affinity", {
    page,
    page_size: pageSize,
    minister: params.minister,
    red_flag_only: params.redFlagOnly,
  });

  return {
    affinities: payload.items,
    total: payload.total,
    page: payload.page,
    pageSize: payload.page_size,
  };
}

export async function getCounselAffinityRedFlags(): Promise<CounselAffinityRedFlags> {
  const payload = await fetchApiJson<CounselAffinityRedFlagsResponse>("/counsel-affinity/red-flags");
  return {
    items: payload.items,
    total: payload.total,
  };
}

export async function getMinisterCounselAffinities(minister: string): Promise<CounselAffinity[]> {
  return fetchApiJson<CounselAffinity[]>(
    `/ministers/${encodeURIComponent(minister)}/counsel-affinity`,
  );
}

