import { fetchApiJson } from "@/lib/api-client";

export type RapporteurChange = {
  change_id: string;
  process_id: string;
  process_class: string | null;
  previous_rapporteur: string;
  new_rapporteur: string;
  change_date: string | null;
  decision_event_id: string | null;
  post_change_decision_count: number;
  post_change_favorable_rate: number | null;
  new_rapporteur_baseline_rate: number | null;
  delta_vs_baseline: number | null;
  red_flag: boolean;
};

type PaginatedResponse = {
  total: number;
  page: number;
  page_size: number;
  items: RapporteurChange[];
};

type RedFlagsResponse = {
  items: RapporteurChange[];
  total: number;
};

export type RapporteurChangePageData = {
  items: RapporteurChange[];
  total: number;
  page: number;
  pageSize: number;
};

export type RapporteurChangeRedFlags = {
  items: RapporteurChange[];
  total: number;
};

export async function getRapporteurChangePageData(params: {
  page?: number;
  pageSize?: number;
  minister?: string;
  redFlagOnly?: boolean;
} = {}): Promise<RapporteurChangePageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  try {
    const payload = await fetchApiJson<PaginatedResponse>("/rapporteur-change", {
      page,
      page_size: pageSize,
      minister: params.minister,
      red_flag_only: params.redFlagOnly,
    });

    return {
      items: payload.items,
      total: payload.total,
      page: payload.page,
      pageSize: payload.page_size,
    };
  } catch (error) {
    console.error("Failed to fetch rapporteur change data:", error);
    return { items: [], total: 0, page, pageSize };
  }
}

export async function getRapporteurChangeRedFlags(): Promise<RapporteurChangeRedFlags> {
  try {
    const payload = await fetchApiJson<RedFlagsResponse>("/rapporteur-change/red-flags");
    return {
      items: payload.items,
      total: payload.total,
    };
  } catch (error) {
    console.error("Failed to fetch rapporteur change red flags:", error);
    return { items: [], total: 0 };
  }
}
