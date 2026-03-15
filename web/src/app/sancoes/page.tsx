import { AlertTriangle } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import { SanctionBadge } from "@/components/dashboard/sanction-badge";
import {
  CardGrid,
  DeltaIndicator,
  ExpandableCard,
  RateComparisonBar,
  RedFlagPill,
} from "@/components/dashboard/cross-ref-card";
import { emptyStateMessage } from "@/lib/ui-copy";
import { getPartySanctionsPageData, getCounselSanctionsPageData, getSanctionRedFlags } from "@/lib/sanctions-data";
import Link from "next/link";
import { readSearchParam } from "@/lib/filter-context";

function matchConfidenceLabel(confidence: string | null, score: number | null): string {
  switch (confidence) {
    case "deterministic": return "CPF/CNPJ exato";
    case "exact_name": return "Nome exato";
    case "fuzzy": return score != null ? `Match fuzzy (${score.toFixed(2)})` : "Match fuzzy";
    case "nominal_review_needed": return "Revisao manual necessaria";
    default: return "Confianca nao determinada";
  }
}

function matchConfidenceColor(confidence: string | null): string {
  switch (confidence) {
    case "deterministic": return "border-verde-200 bg-verde-50 text-verde-700";
    case "exact_name": return "border-blue-200 bg-blue-50 text-blue-700";
    case "fuzzy": return "border-ouro-200 bg-ouro-50 text-ouro-700";
    case "nominal_review_needed": return "border-red-200 bg-red-50 text-red-700";
    default: return "border-slate-200 bg-slate-50 text-slate-600";
  }
}

export default async function SancoesPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = (await searchParams) ?? {};
  const source = readSearchParam(query.source);
  const redFlagOnly = readSearchParam(query.red_flag_only) === "true";
  const page = Number(readSearchParam(query.page) ?? "1");
  const counselPage = Number(readSearchParam(query.counsel_page) ?? "1");

  const filterQuery: Record<string, string | undefined> = {
    source: source ?? undefined,
    red_flag_only: redFlagOnly ? "true" : undefined,
  };

  function buildFilterHref(overrides: Record<string, string | undefined>): string {
    const merged = { ...filterQuery, ...overrides, page: undefined, counsel_page: undefined };
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(merged)) {
      if (v !== undefined) params.set(k, v);
    }
    const qs = params.toString();
    return qs ? `/sancoes?${qs}` : "/sancoes";
  }

  const [partyData, counselData, redFlags] = await Promise.all([
    getPartySanctionsPageData({ page, source: source ?? undefined, redFlagOnly }),
    getCounselSanctionsPageData({ page: counselPage, source: source ?? undefined, redFlagOnly }),
    getSanctionRedFlags(),
  ]);

  return (
    <AppShell
      currentPath="/sancoes"
      eyebrow="Atlas STF · entidades sancionadas"
      title="Sancoes"
      description="Entidades do STF que constam em cadastros oficiais de sancao."
      guidance={{
        title: "Como interpretar esta tela",
        summary:
          "Mostra partes e advogados que constam em bases de sancoes administrativas (CGU), acordos de leniencia (CGU) e de mercado de capitais (CVM) e que litigam no STF.",
        bullets: [
          "Ponto critico indica taxa de exito significativamente acima da media para a classe processual.",
          "CEIS = Cadastro de Empresas Inidoneas e Suspensas (CGU).",
          "CNEP = Cadastro Nacional de Empresas Punidas (CGU).",
          "Leniencia = Acordos de Leniencia firmados com a CGU (delacao premiada corporativa).",
          "CVM = Processos Administrativos Sancionadores (mercado de capitais).",
          "A secao de advogados identifica profissionais que constam nas bases de sancoes e atuam no STF.",
        ],
      }}
    >
      {/* KPI cards */}
      <section className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Partes sancionadas</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{partyData.total}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Advogados sancionados</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{counselData.total}</p>
        </div>
        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 shadow-sm">
          <p className="text-sm text-red-600">Pontos criticos (partes)</p>
          <p className="mt-1 text-3xl font-semibold text-red-700">{redFlags.totalPartyFlags}</p>
        </div>
        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 shadow-sm">
          <p className="text-sm text-red-600">Pontos criticos (advogados)</p>
          <p className="mt-1 text-3xl font-semibold text-red-700">{redFlags.totalCounselFlags}</p>
        </div>
      </section>

      {/* Filters */}
      <section className="flex flex-wrap gap-3">
        <Link
          href="/sancoes"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            !source && !redFlagOnly
              ? "border-verde-600 bg-verde-50 text-verde-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Todos
        </Link>
        <Link
          href={buildFilterHref({ source: "ceis", red_flag_only: redFlagOnly ? "true" : undefined })}
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            source === "ceis"
              ? "border-red-500 bg-red-50 text-red-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          CEIS
        </Link>
        <Link
          href={buildFilterHref({ source: "cnep", red_flag_only: redFlagOnly ? "true" : undefined })}
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            source === "cnep"
              ? "border-orange-500 bg-orange-50 text-orange-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          CNEP
        </Link>
        <Link
          href={buildFilterHref({ source: "cvm", red_flag_only: redFlagOnly ? "true" : undefined })}
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            source === "cvm"
              ? "border-purple-500 bg-purple-50 text-purple-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          CVM
        </Link>
        <Link
          href={buildFilterHref({ source: "leniencia", red_flag_only: redFlagOnly ? "true" : undefined })}
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            source === "leniencia"
              ? "border-indigo-500 bg-indigo-50 text-indigo-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Leniencia
        </Link>
        <Link
          href={buildFilterHref({ red_flag_only: "true" })}
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            redFlagOnly
              ? "border-red-500 bg-red-50 text-red-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Apenas pontos criticos
        </Link>
      </section>

      {/* Partes sancionadas */}
      <section>
        <h2 className="mb-4 text-lg font-semibold text-slate-900">Partes sancionadas</h2>

        <PaginationControls
          pathname="/sancoes"
          query={{ ...filterQuery, counsel_page: counselPage !== 1 ? String(counselPage) : undefined }}
          page={partyData.page}
          pageSize={partyData.pageSize}
          total={partyData.total}
          orderingLabel="cruzamentos de sancoes (partes)"
          pageSizeOptions={[8, 16, 24]}
        />

        {partyData.sanctions.length === 0 ? (
          <div className="mt-4 flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
            <AlertTriangle className="h-5 w-5 text-amber-600" />
            <p className="text-sm text-amber-800">{emptyStateMessage("sanctions")}</p>
          </div>
        ) : (
          <div className="mt-4">
            <CardGrid columns={2}>
              {partyData.sanctions.map((s) => (
                <ExpandableCard
                  key={s.match_id}
                  summary={
                    <div className="flex flex-1 flex-wrap items-center gap-3">
                      <Link
                        href={`/partes/${encodeURIComponent(s.party_id)}`}
                        className="font-medium text-verde-700 hover:underline"
                      >
                        {s.party_name_normalized}
                      </Link>
                      <SanctionBadge source={s.sanction_source} />
                      <span className="text-2xl font-semibold text-slate-900">{s.stf_case_count}</span>
                      <DeltaIndicator value={s.favorable_rate_delta} compact />
                      <RedFlagPill show={s.red_flag} />
                    </div>
                  }
                >
                  <div className="space-y-4">
                    <div className="flex items-center gap-2">
                      <span className={`rounded-full border px-2.5 py-0.5 text-xs font-medium ${matchConfidenceColor(s.match_confidence)}`}>
                        {matchConfidenceLabel(s.match_confidence, s.match_score)}
                      </span>
                    </div>
                    <RateComparisonBar
                      rate={s.favorable_rate}
                      baseline={s.baseline_favorable_rate}
                      rateLabel="Taxa favoravel"
                      baselineLabel="media"
                    />
                    <dl className="grid gap-2 text-sm sm:grid-cols-2">
                      <div>
                        <dt className="text-slate-500">Tipo de sancao</dt>
                        <dd className="font-medium text-slate-900">{s.sanction_type ?? "---"}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-500">Orgao sancionador</dt>
                        <dd className="font-medium text-slate-900">{s.sanctioning_body ?? "---"}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-500">Inicio</dt>
                        <dd className="font-medium text-slate-900">{s.sanction_start_date ?? "---"}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-500">Fim</dt>
                        <dd className="font-medium text-slate-900">{s.sanction_end_date ?? "---"}</dd>
                      </div>
                    </dl>
                  </div>
                </ExpandableCard>
              ))}
            </CardGrid>
          </div>
        )}
      </section>

      {/* Advogados sancionados */}
      <section>
        <h2 className="mb-4 text-lg font-semibold text-slate-900">Advogados sancionados</h2>

        <PaginationControls
          pathname="/sancoes"
          query={{ ...filterQuery, page: page !== 1 ? String(page) : undefined }}
          page={counselData.page}
          pageSize={counselData.pageSize}
          total={counselData.total}
          orderingLabel="cruzamentos de sancoes (advogados)"
          pageParam="counsel_page"
          pageSizeParam="counsel_page_size"
          pageSizeOptions={[8, 16, 24]}
        />

        {counselData.sanctions.length === 0 ? (
          <div className="mt-4 flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
            <AlertTriangle className="h-5 w-5 text-amber-600" />
            <p className="text-sm text-amber-800">{emptyStateMessage("sanctions")}</p>
          </div>
        ) : (
          <div className="mt-4">
            <CardGrid columns={2}>
              {counselData.sanctions.map((s) => (
                <ExpandableCard
                  key={s.match_id}
                  summary={
                    <div className="flex flex-1 flex-wrap items-center gap-3">
                      <Link
                        href={`/advogados/${encodeURIComponent(s.counsel_id ?? s.party_id)}`}
                        className="font-medium text-verde-700 hover:underline"
                      >
                        {s.party_name_normalized}
                      </Link>
                      <SanctionBadge source={s.sanction_source} />
                      <span className="text-2xl font-semibold text-slate-900">{s.stf_case_count}</span>
                      <DeltaIndicator value={s.favorable_rate_delta} compact />
                      <RedFlagPill show={s.red_flag} />
                    </div>
                  }
                >
                  <div className="space-y-4">
                    <div className="flex items-center gap-2">
                      <span className={`rounded-full border px-2.5 py-0.5 text-xs font-medium ${matchConfidenceColor(s.match_confidence)}`}>
                        {matchConfidenceLabel(s.match_confidence, s.match_score)}
                      </span>
                    </div>
                    <RateComparisonBar
                      rate={s.favorable_rate}
                      baseline={s.baseline_favorable_rate}
                      rateLabel="Taxa favoravel"
                      baselineLabel="media"
                    />
                    <dl className="grid gap-2 text-sm sm:grid-cols-2">
                      <div>
                        <dt className="text-slate-500">Tipo de sancao</dt>
                        <dd className="font-medium text-slate-900">{s.sanction_type ?? "---"}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-500">Orgao sancionador</dt>
                        <dd className="font-medium text-slate-900">{s.sanctioning_body ?? "---"}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-500">Inicio</dt>
                        <dd className="font-medium text-slate-900">{s.sanction_start_date ?? "---"}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-500">Fim</dt>
                        <dd className="font-medium text-slate-900">{s.sanction_end_date ?? "---"}</dd>
                      </div>
                    </dl>
                  </div>
                </ExpandableCard>
              ))}
            </CardGrid>
          </div>
        )}
      </section>
    </AppShell>
  );
}
