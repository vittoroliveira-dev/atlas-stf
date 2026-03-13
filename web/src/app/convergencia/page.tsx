import { AppShell } from "@/components/dashboard/app-shell";
import {
  CompoundRiskFilterPanel,
  CompoundRiskHeatmapPanel,
} from "@/components/dashboard/compound-risk-panels";
import { CompoundRiskRanking } from "@/components/dashboard/compound-risk-ranking";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import {
  getCompoundRiskHeatmapData,
  getCompoundRiskPageData,
  getCompoundRiskRedFlags,
} from "@/lib/compound-risk-data";
import { readSearchParam } from "@/lib/filter-context";

export default async function ConvergenciaPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = (await searchParams) ?? {};
  const minister = readSearchParam(query.minister);
  const entityTypeParam = readSearchParam(query.entity_type);
  const entityType =
    entityTypeParam === "party" || entityTypeParam === "counsel"
      ? entityTypeParam
      : undefined;
  const redFlagOnly = readSearchParam(query.red_flag_only) === "true";
  const page = Number(readSearchParam(query.page) ?? "1");
  const pageSizeValue = Number(readSearchParam(query.page_size) ?? "20");
  const pageSize = Number.isFinite(pageSizeValue) && pageSizeValue > 0 ? pageSizeValue : 20;

  const filterQuery: Record<string, string | undefined> = {
    minister: minister ?? undefined,
    entity_type: entityType ?? undefined,
    red_flag_only: redFlagOnly ? "true" : undefined,
  };

  const [data, redFlags, heatmap] = await Promise.all([
    getCompoundRiskPageData({
      page,
      pageSize,
      minister: minister ?? undefined,
      entityType,
      redFlagOnly,
    }),
    getCompoundRiskRedFlags({
      minister: minister ?? undefined,
      entityType,
    }),
    getCompoundRiskHeatmapData({
      limit: 20,
      minister: minister ?? undefined,
      entityType,
      redFlagOnly,
    }),
  ]);

  const filterContext = {
    minister: minister ?? undefined,
  };
  const partyCount = data.items.filter((item) => item.entity_type === "party").length;
  const counselCount = data.items.filter((item) => item.entity_type === "counsel").length;
  const pairsWithAlerts = data.items.filter((item) => item.alert_count > 0).length;
  const pairsWithThreeSignals = data.items.filter((item) => item.signal_count >= 3).length;

  return (
    <AppShell
      currentPath="/convergencia"
      filterContext={filterContext}
      heroState={
        data.total === 0
          ? {
              status: "empty",
              title: "Nenhum par ministro-entidade foi materializado com este recorte",
              description:
                "Amplie os filtros para recuperar pares com sinais combinados e comparar o ranking composto.",
            }
          : redFlags.total === 0
            ? {
                status: "inconclusivo",
                title: "Ha pares cruzados, mas sem convergencia forte o suficiente para ponto critico composto",
                description:
                  "O ranking ainda ajuda a priorizar leitura, mas os sinais distribuidos permanecem abaixo do corte atual.",
              }
            : {
                status: "ok",
                title: "Os sinais compostos ja permitem priorizar os pares mais densos",
                description:
                  "Use o heatmap para localizar concentracoes e o ranking para abrir os pares com mais convergencia entre sancoes, doacoes, vinculos, afinidade e alertas.",
              }
      }
      eyebrow="Atlas STF · sobreposicao de indicadores"
      title="Sinais combinados"
      description="Pares onde multiplos indicadores de cruzamento convergem."
      guidance={{
        title: "Como interpretar esta tela",
        summary:
          "Esta visao prioriza densidade de sinais, nao conclusoes. A utilidade aqui e reduzir o custo de cruzamento manual entre paginas.",
        bullets: [
          "Cada celula mostra quantos sinais convergem no mesmo par ministro-entidade.",
          "Ponto critico composto indica apenas convergencia relevante no recorte atual; nao implica causalidade nem irregularidade.",
          "Abra o ranking para ver quais sinais contribuiram, classes processuais e contexto adicional do par.",
        ],
      }}
    >
      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <div className="rounded-[28px] border border-slate-200/80 bg-white/95 p-5 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
          <p className="text-sm text-slate-500">Pares materializados</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">{data.total}</p>
        </div>
        <div className="rounded-[28px] border border-rose-200/80 bg-rose-50/90 p-5 shadow-[0_20px_70px_rgba(225,29,72,0.10)]">
          <p className="text-sm text-rose-700">Pontos criticos compostos</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-rose-800">{redFlags.total}</p>
        </div>
        <div className="rounded-[28px] border border-slate-200/80 bg-white/95 p-5 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
          <p className="text-sm text-slate-500">Partes nesta pagina</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">{partyCount}</p>
        </div>
        <div className="rounded-[28px] border border-slate-200/80 bg-white/95 p-5 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
          <p className="text-sm text-slate-500">Advogados nesta pagina</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">{counselCount}</p>
        </div>
        <div className="rounded-[28px] border border-amber-200/80 bg-amber-50/90 p-5 shadow-[0_20px_70px_rgba(217,119,6,0.10)]">
          <p className="text-sm text-amber-700">Pares com 3+ sinais</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-amber-800">{pairsWithThreeSignals}</p>
        </div>
      </section>

      <CompoundRiskFilterPanel
        minister={minister ?? undefined}
        entityType={entityType}
        redFlagOnly={redFlagOnly}
        heatmapEntityCount={heatmap.entities.length}
        heatmapMinisterCount={heatmap.ministers.length}
        pairsWithAlerts={pairsWithAlerts}
        displayLimit={heatmap.displayLimit}
      />

      <CompoundRiskHeatmapPanel heatmap={heatmap} />

      <PaginationControls
        pathname="/convergencia"
        query={filterQuery}
        page={data.page}
        pageSize={data.pageSize}
        total={data.total}
        orderingLabel="pares por contagem de sinais, escore de alerta e delta"
      />

      <CompoundRiskRanking items={data.items} />
    </AppShell>
  );
}
