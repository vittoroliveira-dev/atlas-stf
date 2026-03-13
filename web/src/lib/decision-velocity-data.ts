import { fetchApiJson } from "@/lib/api-client";

export type DecisionVelocity = {
  velocity_id: string;
  decision_event_id: string;
  process_id: string;
  current_rapporteur: string | null;
  decision_date: string | null;
  filing_date: string | null;
  days_to_decision: number;
  process_class: string | null;
  thematic_key: string | null;
  decision_year: number | null;
  group_size: number | null;
  p5_days: number | null;
  p10_days: number | null;
  median_days: number | null;
  p90_days: number | null;
  p95_days: number | null;
  velocity_flag: string | null;
  velocity_z_score: number | null;
};

type PaginatedResponse = {
  total: number;
  page: number;
  page_size: number;
  items: DecisionVelocity[];
};

type FlagsResponse = {
  items: DecisionVelocity[];
  total: number;
};

export type DecisionVelocityPageData = {
  items: DecisionVelocity[];
  total: number;
  page: number;
  pageSize: number;
};

export type DecisionVelocityFlags = {
  items: DecisionVelocity[];
  total: number;
};

export async function getDecisionVelocityPageData(params: {
  page?: number;
  pageSize?: number;
  minister?: string;
  flagOnly?: boolean;
  velocityFlag?: string;
  processClass?: string;
} = {}): Promise<DecisionVelocityPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  try {
    const payload = await fetchApiJson<PaginatedResponse>("/decision-velocity", {
      page,
      page_size: pageSize,
      minister: params.minister,
      flag_only: params.flagOnly,
      velocity_flag: params.velocityFlag,
      process_class: params.processClass,
    });

    return {
      items: payload.items,
      total: payload.total,
      page: payload.page,
      pageSize: payload.page_size,
    };
  } catch (error) {
    console.error("Failed to fetch decision velocity data:", error);
    return { items: [], total: 0, page, pageSize };
  }
}

export async function getDecisionVelocityFlags(): Promise<DecisionVelocityFlags> {
  try {
    const payload = await fetchApiJson<FlagsResponse>("/decision-velocity/flags");
    return {
      items: payload.items,
      total: payload.total,
    };
  } catch (error) {
    console.error("Failed to fetch decision velocity flags:", error);
    return { items: [], total: 0 };
  }
}
