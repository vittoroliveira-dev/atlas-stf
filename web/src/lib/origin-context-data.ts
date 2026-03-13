import { fetchApiJson } from "./api-client";

export interface OriginContextItem {
  origin_index: string;
  tribunal_label: string;
  state: string;
  datajud_total_processes: number;
  stf_process_count: number;
  stf_share_pct: number;
  top_assuntos: { nome: string; count: number }[];
  top_orgaos_julgadores: { nome: string; count: number }[];
  class_distribution: { nome: string; count: number }[];
}

export interface OriginContextResponse {
  items: OriginContextItem[];
  total: number;
}

export async function fetchOriginContext(
  state?: string,
): Promise<OriginContextResponse> {
  const pathname = state
    ? `/origin-context/${encodeURIComponent(state)}`
    : "/origin-context";
  return fetchApiJson<OriginContextResponse>(pathname);
}
