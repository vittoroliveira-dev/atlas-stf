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
import { getRapporteurChangePageData, getRapporteurChangeRedFlags } from "@/lib/rapporteur-change-data";
import Link from "next/link";
import { readSearchParam } from "@/lib/filter-context";

export default async function RedistribuicaoPage({
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
    getRapporteurChangePageData({ page, minister: minister ?? undefined, redFlagOnly }),
    getRapporteurChangeRedFlags(),
  ]);

  return (
    <AppShell
      currentPath="/redistribuicao"
      eyebrow="Atlas STF · mudanca de relatoria"
      title="Redistribuicao de processos"
      description="Processos que mudaram de relator durante a tramitacao."
      guidance={{
        title: "Como interpretar esta tela",
        summary:
          "Detecta mudancas de relator entre decisoes consecutivas do mesmo processo e analisa o resultado pos-redistribuicao.",
        bullets: [
          "Ponto critico: taxa favoravel pos-redistribuicao > 15pp acima do baseline do novo relator, com pelo menos 2 decisoes.",
          "Delta vs. baseline compara a taxa favoravel pos-mudanca com a taxa geral do novo relator.",
          "Redistribuicao ocorre por varias razoes legitimas: aposentadoria, impedimento, redistribuicao por sorteio.",
          "O fato de haver mudanca e resultado favoravel nao implica irregularidade.",
        ],
      }}
    >
      {/* KPI cards */}
      <section className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Mudancas detectadas</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{data.total}</p>
        </div>
        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 shadow-sm">
          <p className="text-sm text-red-600">Pontos criticos</p>
          <p className="mt-1 text-3xl font-semibold text-red-700">{redFlags.total}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Na pagina</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{data.items.length}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Pagina</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">
            {data.page}/{Math.max(1, Math.ceil(data.total / data.pageSize))}
          </p>
        </div>
      </section>

      {/* Filters */}
      <section className="flex flex-wrap gap-3">
        <Link
          href="/redistribuicao"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            !redFlagOnly && !minister
              ? "border-verde-600 bg-verde-50 text-verde-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Todos
        </Link>
        <Link
          href={`/redistribuicao?red_flag_only=true${minister ? `&minister=${encodeURIComponent(minister)}` : ""}`}
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            redFlagOnly
              ? "border-red-500 bg-red-50 text-red-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Apenas pontos criticos
        </Link>
      </section>

      <PaginationControls
        pathname="/redistribuicao"
        query={filterQuery}
        page={data.page}
        pageSize={data.pageSize}
        total={data.total}
        orderingLabel="mudancas de relatoria"
        pageSizeOptions={[8, 16, 24]}
      />

      {data.items.length === 0 ? (
        <section className="flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
          <AlertTriangle className="h-5 w-5 text-amber-600" />
          <p className="text-sm text-amber-800">{emptyStateMessage("redistribution")}</p>
        </section>
      ) : (
        <section>
          <CardGrid columns={1}>
            {data.items.map((c) => (
              <ExpandableCard
                key={c.change_id}
                summary={
                  <div className="flex flex-1 flex-wrap items-center gap-3">
                    <Link
                      href={`/caso/${encodeURIComponent(c.process_id)}`}
                      className="font-medium text-verde-700 hover:underline"
                    >
                      {c.process_id}
                    </Link>
                    <span className="text-sm text-slate-500">{c.previous_rapporteur}</span>
                    <span className="text-slate-400">&rarr;</span>
                    <span className="text-sm font-medium text-slate-900">{c.new_rapporteur}</span>
                    {c.process_class && (
                      <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs text-slate-600">
                        {c.process_class}
                      </span>
                    )}
                    <RedFlagPill show={c.red_flag} />
                  </div>
                }
              >
                <div className="space-y-4">
                  <div className="grid gap-4 sm:grid-cols-3">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Data da mudanca</p>
                      <p className="mt-1 text-sm text-slate-900">{c.change_date ?? "---"}</p>
                    </div>
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                        Decisoes pos-mudanca
                      </p>
                      <p className="mt-1 text-sm text-slate-900">{c.post_change_decision_count}</p>
                    </div>
                    <div>
                      <DeltaIndicator value={c.delta_vs_baseline} label="Delta vs. baseline" />
                    </div>
                  </div>

                  {(c.post_change_favorable_rate != null || c.new_rapporteur_baseline_rate != null) && (
                    <div>
                      <p className="mb-1 text-xs font-semibold uppercase tracking-wider text-slate-500">
                        Taxa favoravel pos-mudanca vs. baseline do novo relator
                      </p>
                      <RateComparisonBar
                        rate={c.post_change_favorable_rate}
                        baseline={c.new_rapporteur_baseline_rate}
                        rateLabel="Pos-mudanca"
                        baselineLabel="Baseline relator"
                      />
                    </div>
                  )}
                </div>
              </ExpandableCard>
            ))}
          </CardGrid>
        </section>
      )}
    </AppShell>
  );
}
