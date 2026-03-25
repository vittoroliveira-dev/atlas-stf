import { fetchApiJson, isApiFetchError, isNotFoundError } from "@/lib/api-client";

export type SanctionMatch = {
  match_id: string;
  entity_type: string;
  party_id: string;
  counsel_id: string | null;
  party_name_normalized: string;
  sanction_source: string;
  sanction_id: string;
  sanctioning_body: string | null;
  sanction_type: string | null;
  sanction_start_date: string | null;
  sanction_end_date: string | null;
  sanction_description: string | null;
  stf_case_count: number;
  favorable_rate: number | null;
  baseline_favorable_rate: number | null;
  favorable_rate_delta: number | null;
  red_flag: boolean;
  match_strategy: string | null;
  match_score: number | null;
  match_confidence: string | null;
};

export type CounselSanctionProfile = {
  counsel_id: string;
  counsel_name_normalized: string;
  sanctioned_client_count: number;
  total_client_count: number;
  sanctioned_client_rate: number;
  sanctioned_favorable_rate: number | null;
  overall_favorable_rate: number | null;
  red_flag: boolean;
};

type PaginatedSanctionsResponse = {
  total: number;
  page: number;
  page_size: number;
  items: SanctionMatch[];
};

type SanctionRedFlagsResponse = {
  party_flags: SanctionMatch[];
  counsel_flags: CounselSanctionProfile[];
  total_party_flags: number;
  total_counsel_flags: number;
};

export type SanctionsPageData = {
  sanctions: SanctionMatch[];
  total: number;
  page: number;
  pageSize: number;
};

export type SanctionRedFlags = {
  partyFlags: SanctionMatch[];
  counselFlags: CounselSanctionProfile[];
  totalPartyFlags: number;
  totalCounselFlags: number;
};

async function fetchSanctions(params: {
  page?: number;
  pageSize?: number;
  source?: string;
  redFlagOnly?: boolean;
  entityType?: string;
}): Promise<SanctionsPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  const payload = await fetchApiJson<PaginatedSanctionsResponse>("/sanctions", {
    page,
    page_size: pageSize,
    source: params.source,
    red_flag_only: params.redFlagOnly,
    entity_type: params.entityType,
  });
  const totalPages = Math.max(1, Math.ceil(payload.total / payload.page_size));
  const normalizedPayload = payload.total > 0 && page > totalPages
    ? await fetchApiJson<PaginatedSanctionsResponse>("/sanctions", {
        page: totalPages,
        page_size: pageSize,
        source: params.source,
        red_flag_only: params.redFlagOnly,
        entity_type: params.entityType,
      })
    : payload;

  return {
    sanctions: normalizedPayload.items,
    total: normalizedPayload.total,
    page: normalizedPayload.page,
    pageSize: normalizedPayload.page_size,
  };
}

export async function getSanctionsPageData(params: {
  page?: number;
  pageSize?: number;
  source?: string;
  redFlagOnly?: boolean;
} = {}): Promise<SanctionsPageData> {
  return fetchSanctions(params);
}

export async function getPartySanctionsPageData(params: {
  page?: number;
  pageSize?: number;
  source?: string;
  redFlagOnly?: boolean;
} = {}): Promise<SanctionsPageData> {
  return fetchSanctions({ ...params, entityType: "party" });
}

export async function getCounselSanctionsPageData(params: {
  page?: number;
  pageSize?: number;
  source?: string;
  redFlagOnly?: boolean;
} = {}): Promise<SanctionsPageData> {
  return fetchSanctions({ ...params, entityType: "counsel" });
}

export async function getSanctionRedFlags(): Promise<SanctionRedFlags> {
  const payload = await fetchApiJson<SanctionRedFlagsResponse>("/sanctions/red-flags");
  return {
    partyFlags: payload.party_flags,
    counselFlags: payload.counsel_flags,
    totalPartyFlags: payload.total_party_flags,
    totalCounselFlags: payload.total_counsel_flags,
  };
}

export async function getPartySanctions(partyId: string): Promise<SanctionMatch[]> {
  try {
    return await fetchApiJson<SanctionMatch[]>(`/parties/${encodeURIComponent(partyId)}/sanctions`);
  } catch (error) {
    if (isApiFetchError(error)) {
      return [];
    }
    throw error;
  }
}

export async function getCounselSanctionProfile(counselId: string): Promise<CounselSanctionProfile | null> {
  try {
    return await fetchApiJson<CounselSanctionProfile>(
      `/counsels/${encodeURIComponent(counselId)}/sanction-profile`,
    );
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error; // unreachable — isNotFoundError throws non-404
  }
}

// --- Sanction Corporate Links ---

export type SanctionCorporateLink = {
  link_id: string;
  sanction_id: string;
  sanction_source: string;
  sanction_entity_name: string;
  sanction_entity_tax_id: string | null;
  sanction_type: string | null;
  bridge_company_cnpj_basico: string;
  bridge_company_name: string | null;
  bridge_link_basis: string;
  bridge_confidence: string;
  stf_entity_type: string;
  stf_entity_id: string;
  stf_entity_name: string;
  stf_match_confidence: string | null;
  link_degree: number;
  stf_process_count: number;
  risk_score: number | null;
  red_flag: boolean;
  evidence_chain: string[];
};

type PaginatedSanctionCorporateLinksResponse = {
  total: number;
  page: number;
  page_size: number;
  items: SanctionCorporateLink[];
};

type SanctionCorporateLinkRedFlagsResponse = {
  items: SanctionCorporateLink[];
  total: number;
};

export type SanctionCorporateLinksPageData = {
  links: SanctionCorporateLink[];
  total: number;
  page: number;
  pageSize: number;
};

export async function getSanctionCorporateLinksPageData(params: {
  page?: number;
  pageSize?: number;
  sanctionSource?: string;
  redFlagOnly?: boolean;
} = {}): Promise<SanctionCorporateLinksPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  const payload = await fetchApiJson<PaginatedSanctionCorporateLinksResponse>("/sanction-corporate-links", {
    page,
    page_size: pageSize,
    sanction_source: params.sanctionSource,
    red_flag_only: params.redFlagOnly,
  });
  const totalPages = Math.max(1, Math.ceil(payload.total / payload.page_size));
  const normalizedPayload = payload.total > 0 && page > totalPages
    ? await fetchApiJson<PaginatedSanctionCorporateLinksResponse>("/sanction-corporate-links", {
        page: totalPages,
        page_size: pageSize,
        sanction_source: params.sanctionSource,
        red_flag_only: params.redFlagOnly,
      })
    : payload;
  return {
    links: normalizedPayload.items,
    total: normalizedPayload.total,
    page: normalizedPayload.page,
    pageSize: normalizedPayload.page_size,
  };
}

export async function getSanctionCorporateLinkRedFlags(): Promise<SanctionCorporateLinkRedFlagsResponse> {
  return fetchApiJson<SanctionCorporateLinkRedFlagsResponse>("/sanction-corporate-links/red-flags");
}

export async function getPartySanctionCorporateLinks(partyId: string): Promise<SanctionCorporateLink[]> {
  try {
    return await fetchApiJson<SanctionCorporateLink[]>(
      `/parties/${encodeURIComponent(partyId)}/sanction-corporate-links`,
    );
  } catch (error) {
    if (isApiFetchError(error)) {
      return [];
    }
    throw error;
  }
}
