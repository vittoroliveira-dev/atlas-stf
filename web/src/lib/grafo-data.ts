import { fetchApiJson, isApiFetchError, isNotFoundError } from "@/lib/api-client";

// ---------------------------------------------------------------------------
// Types — graph core
// ---------------------------------------------------------------------------

export type GraphNodeItem = {
  node_id: string;
  node_type: string;
  canonical_label: string | null;
  entity_id: string | null;
  entity_identifier: string | null;
  entity_identifier_type: string | null;
  entity_identifier_quality: string | null;
  active_flag: boolean;
};

export type GraphEdgeItem = {
  edge_id: string;
  src_node_id: string;
  dst_node_id: string;
  edge_type: string;
  confidence_score: number | null;
  evidence_strength: string | null;
  match_strategy: string | null;
  match_score: number | null;
  traversal_policy: string | null;
  truncated_flag: boolean;
  weight: number;
  explanation: Record<string, unknown> | null;
};

export type GraphScoreItem = {
  score_id: string;
  entity_id: string | null;
  traversal_mode: string | null;
  signal_registry: string | null;
  documentary_score: number;
  statistical_score: number;
  network_score: number;
  temporal_score: number;
  fuzzy_penalty: number;
  truncation_penalty: number;
  singleton_penalty: number;
  missing_identifier_penalty: number;
  raw_score: number;
  calibrated_score: number;
  operational_priority: number;
  explanation: Record<string, unknown> | null;
};

export type GraphPathItem = {
  path_id: string;
  start_node_id: string;
  end_node_id: string;
  path_length: number;
  total_cost: number | null;
  min_confidence: number | null;
  min_evidence_strength: string | null;
  traversal_mode: string | null;
  has_truncated_edge: boolean;
  has_fuzzy_edge: boolean;
  edges: string[];
};

// ---------------------------------------------------------------------------
// Types — evidence, review, investigation
// ---------------------------------------------------------------------------

export type EvidenceBundleItem = {
  bundle_id: string;
  entity_id: string | null;
  bundle_type: string | null;
  signal_count: number;
  signal_types: string[];
  summary_text: string | null;
  evidence: Record<string, unknown>[];
};

export type ReviewQueueItem = {
  item_id: string;
  entity_id: string | null;
  path_id: string | null;
  bundle_id: string | null;
  priority_score: number;
  priority_tier: string | null;
  review_reason: string | null;
  status: string;
  queue_type: string | null;
};

export type InvestigationSummary = {
  entity_id: string;
  entity_label: string | null;
  node_type: string | null;
  score: GraphScoreItem | null;
  bundle: EvidenceBundleItem | null;
  edge_count: number;
  signal_count: number;
};

export type BuildMetricsResponse = {
  pct_deterministic_edges: number;
  pct_fuzzy_edges: number;
  pct_truncated_edges: number;
  pct_top100_strict_clean: number;
  pct_top100_single_signal: number;
  total_nodes: number;
  total_edges: number;
  total_scores: number;
  modules_available: number;
  modules_empty: number;
  modules_missing: number;
};

export type InvestigationDetailResponse = {
  entity_id: string;
  node: GraphNodeItem;
  score: GraphScoreItem | null;
  bundles: EvidenceBundleItem[];
  edges: GraphEdgeItem[];
  paths: GraphPathItem[];
};

// ---------------------------------------------------------------------------
// Paginated wrappers
// ---------------------------------------------------------------------------

type Paginated<T> = { total: number; page: number; page_size: number; items: T[] };

export type PageData<T> = { items: T[]; total: number; page: number; pageSize: number };

// ---------------------------------------------------------------------------
// Fetchers — graph core
// ---------------------------------------------------------------------------

export async function fetchGraphSearch(params: {
  query?: string;
  nodeType?: string;
  page?: number;
  pageSize?: number;
}): Promise<PageData<GraphNodeItem>> {
  const r = await fetchApiJson<Paginated<GraphNodeItem>>("/graph/search", {
    query: params.query,
    node_type: params.nodeType,
    page: params.page ?? 1,
    page_size: params.pageSize ?? 20,
  });
  return { items: r.items, total: r.total, page: r.page, pageSize: r.page_size };
}

export async function fetchGraphNode(nodeId: string): Promise<GraphNodeItem | null> {
  try {
    return await fetchApiJson<GraphNodeItem>(`/graph/nodes/${encodeURIComponent(nodeId)}`);
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error;
  }
}

export async function fetchGraphNeighbors(
  nodeId: string,
  opts?: { edgeTypes?: string; evidenceStrengthMin?: string; excludeTruncated?: boolean },
): Promise<{ center_node: GraphNodeItem; neighbors: GraphNodeItem[]; edges: GraphEdgeItem[] } | null> {
  try {
    return await fetchApiJson(`/graph/neighbors/${encodeURIComponent(nodeId)}`, {
      edge_types: opts?.edgeTypes,
      evidence_strength_min: opts?.evidenceStrengthMin,
      exclude_truncated: opts?.excludeTruncated,
    });
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error;
  }
}

export async function fetchGraphMetrics(): Promise<BuildMetricsResponse | null> {
  try {
    return await fetchApiJson<BuildMetricsResponse>("/graph/metrics");
  } catch (error) {
    if (isApiFetchError(error)) return null;
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Fetchers — scores
// ---------------------------------------------------------------------------

export async function fetchGraphScores(params: {
  mode?: string;
  minSignals?: number;
  page?: number;
  pageSize?: number;
}): Promise<PageData<GraphScoreItem>> {
  const r = await fetchApiJson<Paginated<GraphScoreItem>>("/graph/scores", {
    mode: params.mode,
    min_signals: params.minSignals,
    page: params.page ?? 1,
    page_size: params.pageSize ?? 20,
  });
  return { items: r.items, total: r.total, page: r.page, pageSize: r.page_size };
}

// ---------------------------------------------------------------------------
// Fetchers — investigations
// ---------------------------------------------------------------------------

export async function fetchTopInvestigations(params: {
  mode?: string;
  minSignals?: number;
  limit?: number;
  page?: number;
}): Promise<PageData<InvestigationSummary>> {
  const r = await fetchApiJson<Paginated<InvestigationSummary>>("/investigations/top", {
    mode: params.mode,
    min_signals: params.minSignals,
    limit: params.limit ?? 100,
    page: params.page ?? 1,
  });
  return { items: r.items, total: r.total, page: r.page, pageSize: r.page_size };
}

export async function fetchInvestigationDetail(
  entityId: string,
): Promise<InvestigationDetailResponse | null> {
  try {
    return await fetchApiJson<InvestigationDetailResponse>(
      `/investigations/entity/${encodeURIComponent(entityId)}`,
    );
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Fetchers — review queue
// ---------------------------------------------------------------------------

export async function fetchReviewQueue(params: {
  status?: string;
  tier?: string;
  page?: number;
  pageSize?: number;
}): Promise<PageData<ReviewQueueItem>> {
  const r = await fetchApiJson<Paginated<ReviewQueueItem>>("/review/queue", {
    status: params.status,
    tier: params.tier,
    page: params.page ?? 1,
    page_size: params.pageSize ?? 20,
  });
  return { items: r.items, total: r.total, page: r.page, pageSize: r.page_size };
}

export async function fetchReviewItem(itemId: string): Promise<ReviewQueueItem | null> {
  try {
    return await fetchApiJson<ReviewQueueItem>(
      `/review/queue/${encodeURIComponent(itemId)}`,
    );
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Label helpers
// ---------------------------------------------------------------------------

export function nodeTypeLabel(t: string): string {
  const map: Record<string, string> = {
    person: "Pessoa",
    company: "Empresa",
    minister: "Ministro",
    counsel: "Advogado",
    party: "Parte",
    process: "Processo",
  };
  return map[t] ?? t;
}

export function priorityTierLabel(t: string | null): string {
  if (t === "high") return "Alta";
  if (t === "medium") return "Média";
  if (t === "low") return "Baixa";
  return t ?? "---";
}

export function reviewStatusLabel(s: string): string {
  const map: Record<string, string> = {
    pending: "Pendente",
    confirmed_relevant: "Confirmado relevante",
    false_positive: "Falso positivo",
    needs_more_data: "Precisa mais dados",
    deferred: "Adiado",
  };
  return map[s] ?? s;
}
