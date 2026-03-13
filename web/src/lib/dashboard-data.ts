import { ApiClientError, fetchApiJson, isApiFetchError } from "@/lib/api-client";
import type {
  CaseDetailResponse,
  DashboardResponse,
  EntityDetailResponse,
  MlOutlierScoreResponse,
  PaginatedAlertsResponse,
  PaginatedEntitiesResponse,
} from "./dashboard-types";
import { mapAlertDetail, mapCaseRow, mapSourceFiles, toSnapshot } from "./dashboard-mappers";

export type {
  MinisterFlowRecord,
  FlowSnapshot,
  AlertRecord,
  CaseRow,
  AlertDetail,
  EntitySummary,
  DashboardData,
  AlertsPageData,
  MinisterCorrelation,
  EntityListPageData,
  EntityDetailData,
  MlOutlierAnalysis,
} from "./dashboard-types";

export { toChartRows, chartRows, dailyRows } from "./dashboard-mappers";

export async function getDashboardData(params: {
  minister?: string;
  period?: string;
  collegiate?: string;
  judgingBody?: string;
  processClass?: string;
}) {
  const payload = await fetchApiJson<DashboardResponse>("/dashboard", {
    minister: params.minister,
    period: params.period,
    collegiate: params.collegiate,
    judging_body: params.judgingBody,
    process_class: params.processClass,
  });

  return {
    selectedSnapshot: toSnapshot(payload.flow, payload.filters.applied),
    snapshots: [],
    ministers: payload.filters.ministers,
    periods: payload.filters.periods,
    collegiates: payload.filters.collegiates,
    judgingBodies: payload.filters.judging_bodies,
    processClasses: payload.filters.process_classes,
    kpis: {
      alertCount: payload.kpis.alert_count,
      validGroupCount: payload.kpis.valid_group_count,
      baselineCount: payload.kpis.baseline_count,
      averageAlertScore: payload.kpis.average_alert_score,
      selectedEvents: payload.kpis.selected_events,
      selectedProcesses: payload.kpis.selected_processes,
    },
    sourceFiles: mapSourceFiles(payload.source_files),
    ministerProfiles: payload.minister_profiles.map((item) => ({
      minister: item.minister,
      period: item.period,
      collegiate: item.collegiate,
      eventCount: item.event_count,
      historicalAverage: item.historical_average,
      linkedAlertCount: item.linked_alert_count,
      processClasses: item.process_classes,
      themes: item.themes,
    })),
    topAlerts: payload.top_alerts.map(mapAlertDetail),
    caseRows: payload.case_rows.map(mapCaseRow),
    topCounsels: payload.top_counsels,
    topParties: payload.top_parties,
  };
}

export async function getAlertsPageData(
  params: {
    minister?: string;
    period?: string;
    collegiate?: string;
    judgingBody?: string;
    processClass?: string;
  } = {},
  limit = 36,
) {
  const [dashboardData, payload] = await Promise.all([
    getDashboardData(params),
    fetchApiJson<PaginatedAlertsResponse>("/alerts", {
      minister: params.minister,
      period: params.period,
      collegiate: params.collegiate,
      judging_body: params.judgingBody,
      process_class: params.processClass,
      page: 1,
      page_size: limit,
    }),
  ]);

  return {
    ...dashboardData,
    sourceFiles: mapSourceFiles(payload.source_files),
    topCounsels: payload.top_counsels,
    topParties: payload.top_parties,
    alertDetails: payload.items.map(mapAlertDetail),
    filteredAlertCount: payload.total,
    totalAlertCount: dashboardData.kpis.alertCount,
  };
}

export async function getCaseDetailData(params: {
  minister?: string;
  period?: string;
  collegiate?: string;
  processId?: string;
  decisionEventId?: string;
  judgingBody?: string;
  processClass?: string;
}) {
  const dashboardData = await getDashboardData(params);
  if (!params.decisionEventId) {
    return {
      ...dashboardData,
      selectedCase: null,
      mlOutlierAnalysis: null,
      relatedAlerts: [],
      counsels: [],
      parties: [],
    };
  }

  try {
    const query = {
      minister: params.minister,
      period: params.period,
      collegiate: params.collegiate,
      judging_body: params.judgingBody,
      process_class: params.processClass,
    };
    const [payload, mlOutlierPayload] = await Promise.all([
      fetchApiJson<CaseDetailResponse>(`/cases/${encodeURIComponent(params.decisionEventId)}`, query),
      fetchApiJson<MlOutlierScoreResponse>(
        `/cases/${encodeURIComponent(params.decisionEventId)}/ml-outlier`,
        query,
      ).catch((error) => {
        if (error instanceof ApiClientError && error.status === 404) {
          return null;
        }
        throw error;
      }),
    ]);

    return {
      ...dashboardData,
      selectedSnapshot: toSnapshot(payload.flow, payload.filters.applied),
      sourceFiles: mapSourceFiles(payload.source_files),
      selectedCase: payload.case_item ? mapCaseRow(payload.case_item) : null,
      mlOutlierAnalysis: mlOutlierPayload
        ? {
            decisionEventId: mlOutlierPayload.decision_event_id,
            comparisonGroupId: mlOutlierPayload.comparison_group_id,
            mlAnomalyScore: mlOutlierPayload.ml_anomaly_score,
            mlRarityScore: mlOutlierPayload.ml_rarity_score,
            ensembleScore: mlOutlierPayload.ensemble_score,
            nFeatures: mlOutlierPayload.n_features,
            nSamples: mlOutlierPayload.n_samples,
            generatedAt: mlOutlierPayload.generated_at,
          }
        : null,
      relatedAlerts: payload.related_alerts.map(mapAlertDetail),
      counsels: payload.counsels,
      parties: payload.parties,
    };
  } catch (error) {
    if (error instanceof ApiClientError && error.status === 404) {
      return {
        ...dashboardData,
        selectedCase: null,
        mlOutlierAnalysis: null,
        relatedAlerts: [],
        counsels: [],
        parties: [],
      };
    }
    throw error;
  }
}

function entityConfig(kind: "counsel" | "party") {
  return kind === "counsel"
    ? {
        label: "advogados",
        listPath: "/counsels",
        detailPath: "/counsels",
      }
    : {
        label: "partes",
        listPath: "/parties",
        detailPath: "/parties",
      };
}

async function fetchPaginatedEntities(
  path: string,
  params: {
    minister?: string;
    period?: string;
    collegiate?: string;
    judgingBody?: string;
    processClass?: string;
    page: number;
    pageSize: number;
  },
): Promise<PaginatedEntitiesResponse> {
  const payload = await fetchApiJson<PaginatedEntitiesResponse>(path, {
    minister: params.minister,
    period: params.period,
    collegiate: params.collegiate,
    judging_body: params.judgingBody,
    process_class: params.processClass,
    page: params.page,
    page_size: params.pageSize,
  });
  const totalPages = Math.max(1, Math.ceil(payload.total / payload.page_size));
  if (payload.total > 0 && params.page > totalPages) {
    return fetchApiJson<PaginatedEntitiesResponse>(path, {
      minister: params.minister,
      period: params.period,
      collegiate: params.collegiate,
      judging_body: params.judgingBody,
      process_class: params.processClass,
      page: totalPages,
      page_size: params.pageSize,
    });
  }
  return payload;
}

export async function getEntityListPageData(
  kind: "counsel" | "party",
  params: {
    minister?: string;
    period?: string;
    collegiate?: string;
    judgingBody?: string;
    processClass?: string;
    page?: number;
    pageSize?: number;
  } = {},
) {
  const config = entityConfig(kind);
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 24;
  const [dashboardData, payload] = await Promise.all([
    getDashboardData(params),
    fetchPaginatedEntities(config.listPath, {
      minister: params.minister,
      period: params.period,
      collegiate: params.collegiate,
      judgingBody: params.judgingBody,
      processClass: params.processClass,
      page,
      pageSize,
    }),
  ]);

  return {
    ...dashboardData,
    entityKind: kind,
    entityLabel: config.label,
    entities: payload.items,
    filteredEntityCount: payload.total,
    page: payload.page,
    pageSize: payload.page_size,
  };
}

export async function getEntityDetailData(
  kind: "counsel" | "party",
  entityId: string,
  params: {
    minister?: string;
    period?: string;
    collegiate?: string;
    judgingBody?: string;
    processClass?: string;
  } = {},
) {
  const config = entityConfig(kind);
  const dashboardData = await getDashboardData(params);

  try {
    const payload = await fetchApiJson<EntityDetailResponse>(
      `${config.detailPath}/${encodeURIComponent(entityId)}`,
      {
        minister: params.minister,
        period: params.period,
        collegiate: params.collegiate,
        judging_body: params.judgingBody,
        process_class: params.processClass,
      },
    );

    const detailApplied = payload.filters.applied;
    // When the user explicitly passed a period, use the detail's resolved value;
    // when no period was requested, the entity detail API returns period=null
    // (no period filter), so we keep the dashboard period for display context.
    const effectivePeriod = params.period != null
      ? (detailApplied.period ?? dashboardData.selectedSnapshot.period)
      : dashboardData.selectedSnapshot.period;
    return {
      ...dashboardData,
      selectedSnapshot: {
        ...dashboardData.selectedSnapshot,
        minister: detailApplied.minister ?? dashboardData.selectedSnapshot.minister,
        period: effectivePeriod,
        collegiate: detailApplied.collegiate ?? dashboardData.selectedSnapshot.collegiate,
      },
      sourceFiles: mapSourceFiles(payload.source_files),
      entityKind: kind,
      entityLabel: config.label,
      loadError: false,
      selectedEntity: payload.entity,
      relatedMinisters: payload.ministers,
      relatedCases: payload.cases.map(mapCaseRow),
    };
  } catch (error) {
    if (error instanceof ApiClientError && error.status === 404) {
      return {
        ...dashboardData,
        entityKind: kind,
        entityLabel: config.label,
        loadError: false,
        selectedEntity: null,
        relatedMinisters: [],
        relatedCases: [],
      };
    }
    if (isApiFetchError(error)) {
      return {
        ...dashboardData,
        entityKind: kind,
        entityLabel: config.label,
        loadError: true,
        selectedEntity: null,
        relatedMinisters: [],
        relatedCases: [],
      };
    }
    throw error;
  }
}
