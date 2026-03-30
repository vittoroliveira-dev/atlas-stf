import { AlertTriangle } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import {
  CardGrid,
  ExpandableCard,
  RedFlagPill,
} from "@/components/dashboard/cross-ref-card";
import { emptyStateMessage } from "@/lib/ui-copy";
import { getCounselNetworkPageData, getCounselNetworkRedFlags } from "@/lib/counsel-network-data";
import Link from "next/link";
import { readSearchParam } from "@/lib/filter-context";

export default async function RedeAdvogadosPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = (await searchParams) ?? {};
  const redFlagOnly = readSearchParam(query.red_flag_only) === "true";
  const page = Number(readSearchParam(query.page) ?? "1");

  const filterQuery: Record<string, string | undefined> = {
    red_flag_only: redFlagOnly ? "true" : undefined,
  };

  const [data, redFlags] = await Promise.all([
    getCounselNetworkPageData({ page, redFlagOnly }),
    getCounselNetworkRedFlags(),
  ]);

  return (
    <AppShell
      currentPath="/rede-advogados"
      eyebrow="Atlas STF · clusters de representantes"
      title="Rede de advogados"
      description="Grupos de advogados que compartilham clientes em processos no STF."
      guidance={{
        title: "Como interpretar esta tela",
        summary:
          "Identifica escritorios ou redes de advogados que representam os mesmos clientes perante o STF.",
        bullets: [
          "Cluster: grupo de advogados conectados por compartilharem pelo menos 2 clientes em comum.",
          "Taxa favoravel do cluster: taxa de resultado favoravel dos processos do grupo inteiro.",
          "Ponto critico: cluster com taxa favoravel > 65% e pelo menos 5 processos.",
          "Clusters grandes podem representar escritorios legitimos -- o indicador complementa a analise de afinidade individual.",
        ],
      }}
    >
      {/* KPI cards */}
      <section className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Clusters encontrados</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{data.total.toLocaleString("pt-BR")}</p>
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
          href="/rede-advogados"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            !redFlagOnly
              ? "border-verde-600 bg-verde-50 text-verde-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Todos
        </Link>
        <Link
          href="/rede-advogados?red_flag_only=true"
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
        pathname="/rede-advogados"
        query={filterQuery}
        page={data.page}
        pageSize={data.pageSize}
        total={data.total}
        orderingLabel="clusters"
        pageSizeOptions={[8, 16, 24]}
      />

      {data.items.length === 0 ? (
        <section className="flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
          <AlertTriangle className="h-5 w-5 text-amber-600" />
          <p className="text-sm text-amber-800">{emptyStateMessage("counsel_network")}</p>
        </section>
      ) : (
        <section>
          <CardGrid columns={1}>
            {data.items.map((c) => (
              <ExpandableCard
                key={c.cluster_id}
                summary={
                  <div className="flex flex-1 flex-wrap items-center gap-3">
                    <span className="text-2xl font-semibold text-slate-900">{c.cluster_size}</span>
                    <span className="text-sm text-slate-500">advogados</span>
                    <span className="text-sm text-slate-400">|</span>
                    <span className="text-sm text-slate-500">{c.shared_client_count} clientes</span>
                    <span className="text-sm text-slate-400">|</span>
                    <span className="text-sm text-slate-500">{c.shared_process_count} processos</span>
                    {c.cluster_favorable_rate != null && (
                      <span className="text-sm font-medium text-slate-700">
                        {(c.cluster_favorable_rate * 100).toFixed(1)}% fav.
                      </span>
                    )}
                    <RedFlagPill show={c.red_flag} />
                  </div>
                }
              >
                <div className="space-y-4">
                  <div className="grid gap-4 sm:grid-cols-2">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                        Advogados no cluster
                      </p>
                      <div className="mt-1.5 flex flex-wrap gap-1.5">
                        {c.counsel_names.length > 0
                          ? c.counsel_names.slice(0, 10).map((name, i) => (
                              <Link
                                key={c.counsel_ids[i] ?? i}
                                href={`/advogados/${encodeURIComponent(c.counsel_ids[i] ?? "")}`}
                                className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-medium text-verde-700 hover:underline"
                              >
                                {name}
                              </Link>
                            ))
                          : <span className="text-sm text-slate-400">---</span>}
                        {c.counsel_names.length > 10 && (
                          <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs text-slate-500">
                            +{c.counsel_names.length - 10}
                          </span>
                        )}
                      </div>
                    </div>
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                        Ministros envolvidos
                      </p>
                      <div className="mt-1.5 flex flex-wrap gap-1.5">
                        {c.minister_names.length > 0
                          ? c.minister_names.map((name) => (
                              <span
                                key={name}
                                className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-medium text-slate-700"
                              >
                                {name}
                              </span>
                            ))
                          : <span className="text-sm text-slate-400">---</span>}
                      </div>
                    </div>
                  </div>

                  <div className="grid gap-4 sm:grid-cols-3">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Taxa favorável</p>
                      <p className="mt-1 text-sm text-slate-900">
                        {c.cluster_favorable_rate != null
                          ? `${(c.cluster_favorable_rate * 100).toFixed(1)}%`
                          : "---"}
                        {c.baseline_rate != null && (
                          <span className="ml-1 text-xs text-slate-400">
                            (base: {(c.baseline_rate * 100).toFixed(1)}%)
                          </span>
                        )}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                        Processos no cluster
                      </p>
                      <p className="mt-1 text-sm text-slate-900">{c.cluster_case_count}</p>
                    </div>
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                        Clientes compartilhados
                      </p>
                      <p className="mt-1 text-sm text-slate-900">{c.shared_client_count}</p>
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
