import { AlertTriangle, Link2 } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { CorporateNetworkGraph } from "@/components/dashboard/corporate-network-graph";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import {
  CardGrid,
  DeltaIndicator,
  ExpandableCard,
  RateComparisonBar,
  RedFlagPill,
} from "@/components/dashboard/cross-ref-card";
import { emptyStateMessage } from "@/lib/ui-copy";
import { getCorporateNetworkPageData, getCorporateNetworkRedFlags } from "@/lib/corporate-network-data";
import Link from "next/link";
import { readSearchParam } from "@/lib/filter-context";

function degreeBadge(degree: number) {
  const tone =
    degree >= 3
      ? "bg-red-100 text-red-800"
      : degree === 2
        ? "bg-amber-100 text-amber-800"
        : "bg-slate-100 text-slate-600";
  return (
    <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${tone}`}>
      <Link2 className="h-3 w-3" />
      Grau {degree}
    </span>
  );
}

export default async function VinculosPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = (await searchParams) ?? {};
  const minister = readSearchParam(query.minister);
  const redFlagOnly = readSearchParam(query.red_flag_only) === "true";
  const degreeParam = readSearchParam(query.grau);
  const linkDegree = degreeParam && Number.isFinite(Number(degreeParam)) ? Number(degreeParam) : undefined;
  const page = Number(readSearchParam(query.page) ?? "1");

  const filterQuery: Record<string, string | undefined> = {
    minister: minister ?? undefined,
    red_flag_only: redFlagOnly ? "true" : undefined,
    grau: degreeParam ?? undefined,
  };

  const [data, redFlags] = await Promise.all([
    getCorporateNetworkPageData({
      page,
      minister: minister ?? undefined,
      redFlagOnly,
      linkDegree,
    }),
    getCorporateNetworkRedFlags(),
  ]);

  const degree1Count = data.conflicts.filter((c) => c.link_degree === 1).length;
  const indirectCount = data.conflicts.filter((c) => c.link_degree >= 2).length;
  const maxDegree = Math.max(1, ...data.conflicts.map((c) => c.link_degree), linkDegree ?? 1);
  const degreeOptions = Array.from(new Set([1, 2, 3, linkDegree].filter((value): value is number => Boolean(value)))).sort((a, b) => a - b);

  return (
    <AppShell
      currentPath="/vinculos"
      eyebrow="Atlas STF · rede corporativa RFB"
      title="Vinculos empresariais"
      description="Empresas em comum entre ministros e quem litiga no STF."
      guidance={{
        title: "Como interpretar esta tela",
        summary:
          "Mostra vinculos societarios entre ministros do STF e partes/advogados que litigam perante eles.",
        bullets: [
          "Grau 1: ministro e parte/advogado sao socios diretos na mesma empresa.",
          "Grau 2: vinculo indireto -- empresa do ministro tem socio PJ que participa de outra empresa junto com a parte/advogado.",
          "Graus 3+ seguem busca em largura por cadeia societaria; o score de risco recebe decay conforme a distancia.",
          "Ponto critico indica taxa de exito significativamente acima da media para a classe processual, com pelo menos 3 casos compartilhados.",
          "Os dados vem do cadastro de socios da Receita Federal (CNPJ aberto).",
          "Vinculo societario nao implica irregularidade -- indica potencial conflito de interesse a ser verificado.",
        ],
      }}
    >
      {/* KPI cards */}
      <section className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Total de vinculos</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{data.total}</p>
        </div>
        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 shadow-sm">
          <p className="text-sm text-red-600">Pontos criticos</p>
          <p className="mt-1 text-3xl font-semibold text-red-700">{redFlags.total}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Grau 1 (direto)</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{degree1Count}</p>
        </div>
        <div className="rounded-2xl border border-amber-200 bg-amber-50 p-5 shadow-sm">
          <p className="text-sm text-amber-600">Graus 2+ (indireto)</p>
          <p className="mt-1 text-3xl font-semibold text-amber-700">{indirectCount}</p>
          <p className="mt-1 text-xs text-amber-700/80">Maior grau na pagina: {maxDegree}</p>
        </div>
      </section>

      {/* Filters */}
      <section className="flex flex-wrap gap-3">
        <Link
          href="/vinculos"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            !redFlagOnly && !minister && !linkDegree
              ? "border-verde-600 bg-verde-50 text-verde-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Todos
        </Link>
        <Link
          href={`/vinculos?red_flag_only=true${minister ? `&minister=${encodeURIComponent(minister)}` : ""}`}
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            redFlagOnly
              ? "border-red-500 bg-red-50 text-red-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Apenas pontos criticos
        </Link>
        {degreeOptions.map((degree) => (
          <Link
            key={degree}
            href={`/vinculos?grau=${degree}${minister ? `&minister=${encodeURIComponent(minister)}` : ""}`}
            className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
              linkDegree === degree
                ? degree >= 3
                  ? "border-red-500 bg-red-50 text-red-700"
                  : degree === 2
                    ? "border-amber-500 bg-amber-50 text-amber-700"
                    : "border-slate-500 bg-slate-100 text-slate-700"
                : "border-slate-200 text-slate-600 hover:border-slate-400"
            }`}
          >
            Grau {degree}
          </Link>
        ))}
      </section>

      {/* Graph promoted before cards */}
      {data.conflicts.length > 0 && (
        <div className="h-[520px]">
          <CorporateNetworkGraph conflicts={data.conflicts} />
        </div>
      )}

      <PaginationControls
        pathname="/vinculos"
        query={filterQuery}
        page={data.page}
        pageSize={data.pageSize}
        total={data.total}
        orderingLabel="vinculos societarios"
        pageSizeOptions={[8, 16, 24]}
      />

      {/* Cards */}
      {data.conflicts.length === 0 ? (
        <section className="flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
          <AlertTriangle className="h-5 w-5 text-amber-600" />
          <p className="text-sm text-amber-800">{emptyStateMessage("corporate")}</p>
        </section>
      ) : (
        <section>
          <CardGrid columns={1}>
            {data.conflicts.map((c) => (
              <ExpandableCard
                key={c.conflict_id}
                summary={
                  <div className="flex flex-1 flex-wrap items-center gap-3">
                    <span className="font-medium text-slate-900">{c.minister_name}</span>
                    <Link
                      href={
                        `/${c.linked_entity_type === "party" ? "partes" : "advogados"}`
                        + `/${encodeURIComponent(c.linked_entity_id)}`
                      }
                      className="font-medium text-verde-700 hover:underline"
                    >
                      {c.linked_entity_name}
                    </Link>
                    <span className="text-sm text-slate-600">{c.company_name || c.company_cnpj_basico}</span>
                    {degreeBadge(c.link_degree)}
                    <DeltaIndicator value={c.favorable_rate_delta} compact />
                    <RedFlagPill show={c.red_flag} />
                  </div>
                }
              >
                <div className="space-y-4">
                  <RateComparisonBar
                    rate={c.favorable_rate}
                    baseline={c.baseline_favorable_rate}
                    rateLabel="Taxa favoravel"
                    baselineLabel="media"
                  />
                  <dl className="grid gap-2 text-sm sm:grid-cols-2">
                    <div>
                      <dt className="text-slate-500">CNPJ</dt>
                      <dd className="font-mono text-xs font-medium text-slate-900">{c.company_cnpj_basico}</dd>
                    </div>
                    <div>
                      <dt className="text-slate-500">Tipo</dt>
                      <dd className="font-medium text-slate-900">
                        {c.linked_entity_type === "party" ? "Parte" : "Advogado"}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-slate-500">Qualificacao ministro</dt>
                      <dd className="font-medium text-slate-900">{c.minister_qualification ?? "---"}</dd>
                    </div>
                    <div>
                      <dt className="text-slate-500">Qualificacao entidade</dt>
                      <dd className="font-medium text-slate-900">{c.entity_qualification ?? "---"}</dd>
                    </div>
                    <div>
                      <dt className="text-slate-500">Casos compartilhados</dt>
                      <dd className="font-medium text-slate-900">{c.shared_process_count}</dd>
                    </div>
                    {c.risk_score != null && (
                      <div>
                        <dt className="text-slate-500">Score de risco</dt>
                        <dd className="font-medium text-slate-900">
                          {(c.risk_score * 100).toFixed(1)}pp
                          <span className="ml-1 text-xs text-slate-500">decay {c.decay_factor?.toFixed(2) ?? "1.00"}x</span>
                        </dd>
                      </div>
                    )}
                  </dl>
                  {c.link_chain && c.link_degree >= 2 && (
                    <div>
                      <p className="mb-1.5 text-sm text-slate-500">Cadeia de vinculo</p>
                      <div className="flex flex-wrap items-center gap-1.5">
                        {c.link_chain.split(" -> ").map((step, i) => (
                          <span key={i} className="inline-flex items-center">
                            {i > 0 && <span className="mx-1 text-slate-400">&rarr;</span>}
                            <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-medium text-slate-700">
                              {step.trim()}
                            </span>
                          </span>
                        ))}
                      </div>
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
