import { fetchApiJson, isNotFoundError } from "@/lib/api-client";

export type LawyerEntity = {
  lawyer_id: string;
  lawyer_name_raw: string;
  lawyer_name_normalized: string | null;
  oab_number: string | null;
  oab_state: string | null;
  oab_status: string | null;
  firm_id: string | null;
  process_count: number;
  event_count: number;
  first_seen_date: string | null;
  last_seen_date: string | null;
};

export type LawFirmEntity = {
  firm_id: string;
  firm_name_raw: string;
  firm_name_normalized: string | null;
  cnpj: string | null;
  cnsa_number: string | null;
  member_count: number;
  process_count: number;
};

export type RepresentationEdge = {
  edge_id: string;
  process_id: string;
  representative_entity_id: string;
  representative_kind: string | null;
  role_type: string | null;
  lawyer_id: string | null;
  firm_id: string | null;
  party_id: string | null;
  event_count: number;
  confidence: number | null;
};

export type RepresentationEvent = {
  event_id: string;
  process_id: string;
  edge_id: string | null;
  lawyer_id: string | null;
  event_type: string | null;
  event_date: string | null;
  event_description: string | null;
};

type PaginatedLawyers = {
  total: number;
  page: number;
  page_size: number;
  items: LawyerEntity[];
};

type PaginatedFirms = {
  total: number;
  page: number;
  page_size: number;
  items: LawFirmEntity[];
};

type LawyerDetailResponse = {
  lawyer: LawyerEntity;
  edges: RepresentationEdge[];
  events: RepresentationEvent[];
};

type FirmDetailResponse = {
  firm: LawFirmEntity;
  lawyers: LawyerEntity[];
};

type RepresentationSummary = {
  total_lawyers: number;
  total_firms: number;
  total_edges: number;
  total_events: number;
  lawyers_with_oab: number;
  lawyers_with_firm: number;
};

export type RepresentationPageData = {
  lawyers: LawyerEntity[];
  firms: LawFirmEntity[];
  summary: RepresentationSummary;
  lawyerTotal: number;
  firmTotal: number;
  page: number;
  pageSize: number;
};

export async function getRepresentationPageData(params: {
  page?: number;
  pageSize?: number;
  tab?: "advogados" | "escritorios";
  search?: string;
} = {}): Promise<RepresentationPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  const [lawyerData, firmData, summary] = await Promise.all([
    fetchApiJson<PaginatedLawyers>("/representation/lawyers", {
      page,
      page_size: pageSize,
      search: params.search,
    }),
    fetchApiJson<PaginatedFirms>("/representation/firms", {
      page: 1,
      page_size: 10,
    }),
    fetchApiJson<RepresentationSummary>("/representation/summary"),
  ]);
  return {
    lawyers: lawyerData.items,
    firms: firmData.items,
    summary,
    lawyerTotal: lawyerData.total,
    firmTotal: firmData.total,
    page: lawyerData.page,
    pageSize: lawyerData.page_size,
  };
}

export async function getLawyerDetail(lawyerId: string): Promise<LawyerDetailResponse | null> {
  try {
    return await fetchApiJson<LawyerDetailResponse>(`/representation/lawyers/${encodeURIComponent(lawyerId)}`);
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error; // unreachable — isNotFoundError throws non-404
  }
}

export async function getFirmDetail(firmId: string): Promise<FirmDetailResponse | null> {
  try {
    return await fetchApiJson<FirmDetailResponse>(`/representation/firms/${encodeURIComponent(firmId)}`);
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error; // unreachable — isNotFoundError throws non-404
  }
}
