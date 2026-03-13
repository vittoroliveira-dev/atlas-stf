import { Activity, BookText, Sparkles, Users } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { CaseTable } from "@/components/dashboard/case-table";
import { DailyAreaChart, DistributionBars, DistributionDonut, SegmentBarChart } from "@/components/dashboard/charts";
import { EntityRanking } from "@/components/dashboard/entity-ranking";
import { FilterBar } from "@/components/dashboard/filter-bar";
import { StatCard } from "@/components/dashboard/stat-card";
import { SourceAudit } from "@/components/dashboard/source-audit";
import { chartRows, dailyRows, getDashboardData, toChartRows } from "@/lib/dashboard-data";
import { buildFilterQuery, readSearchParam } from "@/lib/filter-context";

export default async function MinisterPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) ?? {};
  const judgingBody = readSearchParam(params.judging_body);
  const processClass = readSearchParam(params.process_class);
  const data = await getDashboardData({
    minister: readSearchParam(params.minister),
    period: readSearchParam(params.period),
    collegiate: readSearchParam(params.collegiate),
    judgingBody,
    processClass,
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

  return (
    <AppShell
      currentPath="/ministros"
      filterContext={filterContext}
      heroState={
        flow.status === "empty"
            ? {
                status: "empty",
                title: "Não há decisões suficientes neste período",
                description:
                  "Com os filtros atuais, ainda não há volume suficiente para montar uma leitura útil deste período.",
            }
          : flow.thematic_flow_interpretation_status === "inconclusivo"
            ? {
                status: "inconclusivo",
                title: "Ainda não há base suficiente para comparar este período com segurança",
                description:
                  "Há dados para leitura, mas o contexto ainda é curto para comparações mais firmes.",
              }
            : {
                status: "ok",
                title: "Este período já pode ser comparado",
                description:
                  "Os dados atuais já permitem uma leitura comparativa inicial do período selecionado.",
              }
      }
      eyebrow="Atlas STF · comparação do período"
      title={`Como foi o período de ${data.selectedSnapshot.minister}`}
      description="Esta página ajuda a comparar volume, temas, tipos de decisão e casos do período selecionado em uma visão mais detalhada."
      guidance={{
        title: "Como usar esta comparação",
        summary: "Aqui você aprofunda o período já escolhido e entende onde estão as principais mudanças ou concentrações.",
        bullets: [
          "Comece pelos números principais para saber se o período é pequeno ou robusto.",
          "Depois veja distribuição por tipo de decisão, andamento e tema.",
          "Se algo chamar atenção, abra os casos ou os nomes relacionados para aprofundar.",
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
        action="/ministros"
      />

      <section className="grid gap-4 md:grid-cols-4">
        <StatCard icon={Users} label="Ocorrências no período" value={String(flow.event_count)} help="Quantidade de decisões encontradas neste filtro." />
        <StatCard icon={BookText} label="Casos diferentes" value={String(flow.process_count)} help="Número de casos distintos ligados ao período selecionado." />
        <StatCard icon={Activity} label="Dias com atividade" value={String(flow.active_day_count)} help="Dias em que houve pelo menos uma decisão dentro deste filtro." />
        <StatCard icon={Sparkles} label="Média histórica por dia" value={flow.historical_average_events_per_active_day.toFixed(3)} help="Referência histórica usada para comparar este período com o que já vinha acontecendo." />
      </section>

      <section className="grid gap-5 xl:grid-cols-2">
        <DailyAreaChart data={dailyRows(flow.daily_counts)} />
        <DistributionDonut title="Tipo de decisão" subtitle="Mostra a diferença entre decisões individuais e decisões colegiadas." data={toChartRows(flow.collegiate_distribution)} />
        <DistributionBars title="Formato da decisão" subtitle="Tipos de decisão encontrados neste período." data={toChartRows(flow.decision_type_distribution)} valueLabel="ocorrências" />
        <DistributionBars title="Resultado da decisão" subtitle="Situação final das decisões encontradas neste período." data={toChartRows(flow.decision_progress_distribution)} valueLabel="ocorrências" />
      </section>

      <section className="grid gap-5 xl:grid-cols-2">
        <SegmentBarChart title="Tipos de ação" subtitle="Mostra quais tipos de ação aparecem com mais frequência neste período." data={chartRows(flow.process_class_flow)} />
        <SegmentBarChart title="Temas mais presentes" subtitle="Mostra os temas que mais aparecem entre os casos deste período." data={chartRows(flow.thematic_flow)} />
      </section>

      <section className="grid gap-5 xl:grid-cols-2">
        <EntityRanking
          title="Representantes que mais aparecem neste período"
          subtitle="A lista ajuda a ver quais representantes aparecem com mais frequência e qual é o tipo de ligação observada."
          items={data.topCounsels}
          emptyMessage="Nenhum representante apareceu neste período dentro do filtro atual."
        />
        <EntityRanking
          title="Partes que mais aparecem neste período"
          subtitle="A lista ajuda a entender quais partes se repetem mais neste período e em que contexto."
          items={data.topParties}
          emptyMessage="Nenhuma parte apareceu neste período dentro do filtro atual."
        />
      </section>

      <CaseTable rows={data.caseRows} contextQuery={contextQuery} />
      <SourceAudit sourceFiles={data.sourceFiles} />
    </AppShell>
  );
}
