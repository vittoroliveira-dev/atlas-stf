import { Gavel, Network, Users } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { EntityIndexGrid } from "@/components/dashboard/entity-pages";
import { FilterBar } from "@/components/dashboard/filter-bar";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import { SourceAudit } from "@/components/dashboard/source-audit";
import { StatCard } from "@/components/dashboard/stat-card";
import { getEntityListPageData } from "@/lib/dashboard-data";
import { buildFilterQuery, readSearchParam } from "@/lib/filter-context";

export default async function PartiesPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) ?? {};
  const page = Number(readSearchParam(params.page) ?? "1");
  const pageSize = Number(readSearchParam(params.page_size) ?? "24");
  const judgingBody = readSearchParam(params.judging_body);
  const processClass = readSearchParam(params.process_class);
  const data = await getEntityListPageData("party", {
    minister: readSearchParam(params.minister),
    period: readSearchParam(params.period),
    collegiate: readSearchParam(params.collegiate),
    judgingBody,
    processClass,
    page: Number.isFinite(page) && page > 0 ? page : 1,
    pageSize: Number.isFinite(pageSize) && pageSize > 0 ? pageSize : 24,
  });
  const flow = data.selectedSnapshot.data;
  const filterContext = {
    minister: data.selectedSnapshot.minister,
    period: data.selectedSnapshot.period,
    collegiate: flow.collegiate_filter,
    judgingBody,
    processClass,
  };
  const contextQuery = buildFilterQuery(filterContext);
  const listQuery = {
    minister: filterContext.minister,
    period: filterContext.period,
    collegiate: filterContext.collegiate,
    judging_body: filterContext.judgingBody,
    process_class: filterContext.processClass,
  };

  return (
    <AppShell
      currentPath="/partes"
      filterContext={filterContext}
      heroState={
        data.filteredEntityCount === 0
            ? {
                status: "empty",
                title: "Nenhuma parte neste período",
                description:
                  "Não encontramos partes ligadas ao filtro atual.",
            }
          : flow.thematic_flow_interpretation_status === "inconclusivo"
            ? {
                status: "inconclusivo",
                title: "Há partes ligadas ao período, mas ainda falta contexto para comparação mais firme",
                description:
                  "Os nomes aparecem no período, mas ainda pedem leitura cuidadosa do contexto.",
              }
            : {
                status: "ok",
                title: "Partes ligadas ao período selecionado",
                description:
                  "A lista ajuda a entender quem aparece com mais frequência e em que tipo de ligação.",
              }
      }
      eyebrow="Atlas STF · partes envolvidas"
      title="Partes que aparecem neste período"
      description="Esta página mostra quais partes aparecem no filtro atual e em quantos casos elas se repetem."
      guidance={{
        title: "Como usar esta lista",
        summary: "Use esta visão para encontrar partes recorrentes e abrir o detalhe quando quiser entender em quais casos elas aparecem.",
        bullets: [
          "Veja primeiro quem aparece mais vezes no período.",
          "Use o tipo de ligação para entender se o nome aparece no mesmo processo, no mesmo caso ou em contexto que ainda pede cuidado.",
          "Abra o detalhe para ver casos relacionados e ministros que aparecem junto desse nome.",
        ],
      }}
    >
      <FilterBar
        ministers={data.ministers}
        periods={data.periods}
        judgingBodies={data.judgingBodies}
        processClasses={data.processClasses}
        selectedMinister={data.selectedSnapshot.minister}
        selectedPeriod={data.selectedSnapshot.period}
        selectedCollegiate={flow.collegiate_filter}
        selectedJudgingBody={judgingBody}
        selectedProcessClass={processClass}
        action="/partes"
      />

      <section className="grid gap-4 md:grid-cols-3">
        <StatCard icon={Users} label="Partes encontradas" value={String(data.filteredEntityCount)} help="Quantidade de partes ligadas ao filtro atual." />
        <StatCard icon={Network} label="Página atual" value={String(data.page)} help={`Você está vendo ${data.pageSize} itens por página.`} />
        <StatCard icon={Gavel} label="Ocorrências no período" value={String(data.kpis.selectedEvents)} help="Quantidade de decisões usadas para montar esta lista." />
      </section>

      <PaginationControls
        pathname="/partes"
        query={listQuery}
        page={data.page}
        pageSize={data.pageSize}
        total={data.filteredEntityCount}
        orderingLabel="ocorrências → casos diferentes"
      />

      <EntityIndexGrid
        title="Partes mais recorrentes"
        subtitle="A ordenação destaca quem aparece mais vezes no período e depois quem aparece em mais casos diferentes."
        items={data.entities}
        detailBasePath="/partes"
        contextQuery={contextQuery}
        emptyMessage="Nenhuma parte apareceu com estes filtros."
      />

      <SourceAudit sourceFiles={data.sourceFiles} />
    </AppShell>
  );
}
