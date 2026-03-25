import { fetchApiJson, isApiFetchError } from "@/lib/api-client";

export type CounselNetworkCluster = {
  cluster_id: string;
  counsel_ids: string[];
  counsel_names: string[];
  cluster_size: number;
  shared_client_count: number;
  shared_process_count: number;
  minister_names: string[];
  cluster_favorable_rate: number | null;
  cluster_case_count: number;
  red_flag: boolean;
};

type PaginatedResponse = {
  total: number;
  page: number;
  page_size: number;
  items: CounselNetworkCluster[];
};

type RedFlagsResponse = {
  items: CounselNetworkCluster[];
  total: number;
};

export type CounselNetworkPageData = {
  items: CounselNetworkCluster[];
  total: number;
  page: number;
  pageSize: number;
};

export type CounselNetworkRedFlags = {
  items: CounselNetworkCluster[];
  total: number;
};

export async function getCounselNetworkPageData(params: {
  page?: number;
  pageSize?: number;
  redFlagOnly?: boolean;
} = {}): Promise<CounselNetworkPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  try {
    const payload = await fetchApiJson<PaginatedResponse>("/counsel-network", {
      page,
      page_size: pageSize,
      red_flag_only: params.redFlagOnly,
    });

    return {
      items: payload.items,
      total: payload.total,
      page: payload.page,
      pageSize: payload.page_size,
    };
  } catch (error) {
    if (!isApiFetchError(error)) throw error;
    console.error("Failed to fetch counsel network data:", error);
    return { items: [], total: 0, page, pageSize };
  }
}

export async function getCounselNetworkRedFlags(): Promise<CounselNetworkRedFlags> {
  try {
    const payload = await fetchApiJson<RedFlagsResponse>("/counsel-network/red-flags");
    return {
      items: payload.items,
      total: payload.total,
    };
  } catch (error) {
    if (!isApiFetchError(error)) throw error;
    console.error("Failed to fetch counsel network red flags:", error);
    return { items: [], total: 0 };
  }
}
