import { AlertTriangle } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import {
  CardGrid,
  DeltaIndicator,
  ExpandableCard,
  RateComparisonBar,
  RedFlagPill,
} from "@/components/dashboard/cross-ref-card";
import { emptyStateMessage } from "@/lib/ui-copy";
import { getCounselAffinityPageData, getCounselAffinityRedFlags } from "@/lib/counsel-affinity-data";
import Link from "next/link";
import { readSearchParam } from "@/lib/filter-context";

export default async function AfinidadePage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = (await searchParams) ?? {};
  const minister = readSearchParam(query.minister);
  const redFlagOnly = readSearchParam(query.red_flag_only) === "true";
  const page = Number(readSearchParam(query.page) ?? "1");

  const filterQuery: Record<string, string | undefined> = {
    minister: minister ?? undefined,
    red_flag_only: redFlagOnly ? "true" : undefined,
  };

  const [data, redFlags] = await Promise.all([
    getCounselAffinityPageData({ page, minister: minister ?? undefined, redFlagOnly }),
    getCounselAffinityRedFlags(),
  ]);

  return (
    <AppShell
      currentPath="/afinidade"
      eyebrow="Atlas STF · pares com resultado atípico"
      title="Afinidade ministro-advogado"
      description="Pares com taxa de resultado favorável atípica."
      guidance={{
        title: "Como interpretar esta tela",
        summary:
          "Mostra pares ministro-advogado com taxa de vitória anômala comparada aos baselines.",
        bullets: [
          "Ponto crítico indica que o par tem delta > 15pp em relação ao baseline do ministro ou do advogado, com pelo menos 5 casos compartilhados.",
          "Delta vs. ministro compara a taxa do par com a taxa geral do ministro nas mesmas classes processuais.",
          "Delta vs. advogado compara a taxa do par com a taxa geral do advogado com qualquer ministro.",
          "Afinidade alta não implica irregularidade -- pode refletir especialização temática do advogado.",
        ],
      }}
    >
      {/* KPI cards */}
      <section className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Pares analisados</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{data.total}</p>
        </div>
        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 shadow-sm">
          <p className="text-sm text-red-600">Pontos críticos</p>
          <p className="mt-1 text-3xl font-semibold text-red-700">{redFlags.total}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Pares na página</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{data.affinities.length}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Página</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{data.page}/{Math.max(1, Math.ceil(data.total / data.pageSize))}</p>
        </div>
      </section>

      {/* Filters */}
      <section className="flex flex-wrap gap-3">
        <Link
          href="/afinidade"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            !redFlagOnly && !minister
              ? "border-verde-600 bg-verde-50 text-verde-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Todos
        </Link>
        <Link
          href={`/afinidade?red_flag_only=true${minister ? `&minister=${encodeURIComponent(minister)}` : ""}`}
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            redFlagOnly
              ? "border-red-500 bg-red-50 text-red-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Apenas pontos críticos
        </Link>
      </section>

      <PaginationControls
        pathname="/afinidade"
        query={filterQuery}
        page={data.page}
        pageSize={data.pageSize}
        total={data.total}
        orderingLabel="pares ministro-advogado"
        pageSizeOptions={[8, 16, 24]}
      />

      {/* Cards */}
      {data.affinities.length === 0 ? (
        <section className="flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
          <AlertTriangle className="h-5 w-5 text-amber-600" />
          <p className="text-sm text-amber-800">{emptyStateMessage("affinity")}</p>
        </section>
      ) : (
        <section>
          <CardGrid columns={1}>
            {data.affinities.map((a) => (
              <ExpandableCard
                key={a.affinity_id}
                summary={
                  <div className="flex flex-1 flex-wrap items-center gap-3">
                    <span className="font-medium text-slate-900">{a.rapporteur}</span>
                    <Link
                      href={`/advogados/${encodeURIComponent(a.counsel_id)}`}
                      className="font-medium text-verde-700 hover:underline"
                    >
                      {a.counsel_name_normalized}
                    </Link>
                    <span className="text-2xl font-semibold text-slate-900">{a.shared_case_count}</span>
                    <span className="text-sm text-slate-500">
                      {a.pair_favorable_rate != null ? `${(a.pair_favorable_rate * 100).toFixed(1)}%` : "---"}
                    </span>
                    <RedFlagPill show={a.red_flag} />
                  </div>
                }
              >
                <div className="space-y-4">
                  <div className="space-y-3">
                    <div>
                      <p className="mb-1 text-xs font-semibold uppercase tracking-wider text-slate-500">Par vs. baseline do ministro</p>
                      <RateComparisonBar
                        rate={a.pair_favorable_rate}
                        baseline={a.minister_baseline_favorable_rate}
                        rateLabel="Taxa do par"
                        baselineLabel="media ministro"
                      />
                    </div>
                    <div>
                      <p className="mb-1 text-xs font-semibold uppercase tracking-wider text-slate-500">Par vs. baseline do advogado</p>
                      <RateComparisonBar
                        rate={a.pair_favorable_rate}
                        baseline={a.counsel_baseline_favorable_rate}
                        rateLabel="Taxa do par"
                        baselineLabel="media advogado"
                      />
                    </div>
                    <div>
                      <p className="mb-1 text-xs font-semibold uppercase tracking-wider text-slate-500">Taxa absoluta do par</p>
                      <RateComparisonBar
                        rate={a.pair_favorable_rate}
                        baseline={null}
                        rateLabel="Taxa favorável"
                      />
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-3">
                    <DeltaIndicator value={a.pair_delta_vs_minister} label="Delta vs. ministro" />
                    <DeltaIndicator value={a.pair_delta_vs_counsel} label="Delta vs. advogado" />
                  </div>

                  <div>
                    <p className="text-sm text-slate-500">Classes processuais</p>
                    <div className="mt-1.5 flex flex-wrap gap-1.5">
                      {a.top_process_classes.length > 0 ? a.top_process_classes.map((cls) => (
                        <span key={cls} className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-medium text-slate-700">
                          {cls}
                        </span>
                      )) : <span className="text-sm text-slate-400">---</span>}
                    </div>
                  </div>
                </div>
              </ExpandableCard>
            ))}
          </CardGrid>
        </section>
      )}
    </AppShell>
  );
}
