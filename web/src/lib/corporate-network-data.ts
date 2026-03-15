import { fetchApiJson } from "@/lib/api-client";

export type EstablishmentSummary = {
  cnpj_full: string;
  matriz_filial: string;
  nome_fantasia: string;
  uf: string;
  municipio_label: string;
  cnae_fiscal: string;
  cnae_label: string;
  situacao_cadastral: string;
  data_inicio_atividade: string;
};

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
  // Decoded labels
  minister_qualification_label: string | null;
  entity_qualification_label: string | null;
  company_natureza_juridica_label: string | null;
  // Multi-establishment
  establishment_count: number | null;
  active_establishment_count: number | null;
  headquarters_uf: string | null;
  headquarters_municipio_label: string | null;
  headquarters_cnae_fiscal: string | null;
  headquarters_cnae_label: string | null;
  headquarters_situacao_cadastral: string | null;
  headquarters_motivo_situacao_label: string | null;
  establishment_ufs: string[];
  establishment_cnaes: string[];
  establishment_cnae_labels: string[];
  key_establishments: EstablishmentSummary[];
  // Economic group
  economic_group_id: string | null;
  economic_group_member_count: number | null;
  economic_group_razoes_sociais: string[];
  // Provenance
  evidence_type: string | null;
  source_dataset: string | null;
  source_snapshot: string | null;
  evidence_strength: string | null;
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
