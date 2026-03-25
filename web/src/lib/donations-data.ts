import { fetchApiJson, isApiFetchError, isNotFoundError } from "@/lib/api-client";

export type DonationMatch = {
  match_id: string;
  entity_type: string;
  party_id: string;
  counsel_id: string | null;
  party_name_normalized: string;
  donor_cpf_cnpj: string;
  total_donated_brl: number;
  donation_count: number;
  election_years: number[];
  parties_donated_to: string[];
  candidates_donated_to: string[];
  positions_donated_to: string[];
  stf_case_count: number;
  favorable_rate: number | null;
  baseline_favorable_rate: number | null;
  favorable_rate_delta: number | null;
  red_flag: boolean;
  match_strategy: string | null;
  match_score: number | null;
  match_confidence: string | null;
};

export type CounselDonationProfile = {
  counsel_id: string;
  counsel_name_normalized: string;
  donor_client_count: number;
  total_client_count: number;
  donor_client_rate: number;
  donor_client_favorable_rate: number | null;
  overall_favorable_rate: number | null;
  red_flag: boolean;
};

type PaginatedDonationsResponse = {
  total: number;
  page: number;
  page_size: number;
  items: DonationMatch[];
};

type DonationRedFlagsResponse = {
  party_flags: DonationMatch[];
  counsel_flags: CounselDonationProfile[];
  total_party_flags: number;
  total_counsel_flags: number;
};

export type DonationsPageData = {
  donations: DonationMatch[];
  total: number;
  page: number;
  pageSize: number;
};

export type DonationRedFlags = {
  partyFlags: DonationMatch[];
  counselFlags: CounselDonationProfile[];
  totalPartyFlags: number;
  totalCounselFlags: number;
};

async function fetchDonations(params: {
  page?: number;
  pageSize?: number;
  redFlagOnly?: boolean;
  entityType?: string;
}): Promise<DonationsPageData> {
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  const payload = await fetchApiJson<PaginatedDonationsResponse>("/donations", {
    page,
    page_size: pageSize,
    red_flag_only: params.redFlagOnly,
    entity_type: params.entityType,
  });

  return {
    donations: payload.items,
    total: payload.total,
    page: payload.page,
    pageSize: payload.page_size,
  };
}

export async function getDonationsPageData(params: {
  page?: number;
  pageSize?: number;
  redFlagOnly?: boolean;
} = {}): Promise<DonationsPageData> {
  return fetchDonations(params);
}

export async function getPartyDonationsPageData(params: {
  page?: number;
  pageSize?: number;
  redFlagOnly?: boolean;
} = {}): Promise<DonationsPageData> {
  return fetchDonations({ ...params, entityType: "party" });
}

export async function getCounselDonationsPageData(params: {
  page?: number;
  pageSize?: number;
  redFlagOnly?: boolean;
} = {}): Promise<DonationsPageData> {
  return fetchDonations({ ...params, entityType: "counsel" });
}

export async function getDonationRedFlags(): Promise<DonationRedFlags> {
  const payload = await fetchApiJson<DonationRedFlagsResponse>("/donations/red-flags");
  return {
    partyFlags: payload.party_flags,
    counselFlags: payload.counsel_flags,
    totalPartyFlags: payload.total_party_flags,
    totalCounselFlags: payload.total_counsel_flags,
  };
}

export async function getPartyDonations(partyId: string): Promise<DonationMatch[]> {
  try {
    return await fetchApiJson<DonationMatch[]>(`/parties/${encodeURIComponent(partyId)}/donations`);
  } catch (error) {
    if (isApiFetchError(error)) {
      return [];
    }
    throw error;
  }
}

export async function getCounselDonationProfile(counselId: string): Promise<CounselDonationProfile | null> {
  try {
    return await fetchApiJson<CounselDonationProfile>(
      `/counsels/${encodeURIComponent(counselId)}/donation-profile`,
    );
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error; // unreachable — isNotFoundError throws non-404
  }
}
