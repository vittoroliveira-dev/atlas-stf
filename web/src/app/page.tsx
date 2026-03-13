import Link from "next/link";
import { Activity, AlertTriangle, ArrowRight, BookText, Scale, Sparkles, Users } from "lucide-react";
import { AlertTable } from "@/components/dashboard/alert-table";
import { AppShell } from "@/components/dashboard/app-shell";
import { CaseTable } from "@/components/dashboard/case-table";
import { DailyAreaChart, DistributionBars, DistributionDonut, SegmentBarChart } from "@/components/dashboard/charts";
import { EntityRanking } from "@/components/dashboard/entity-ranking";
import { FilterBar } from "@/components/dashboard/filter-bar";
import { ProfileStrip } from "@/components/dashboard/profile-strip";
import { StatCard } from "@/components/dashboard/stat-card";
import { SourceAudit } from "@/components/dashboard/source-audit";
import { chartRows, dailyRows, getDashboardData, toChartRows } from "@/lib/dashboard-data";
import { buildFilterHref, buildFilterQuery, readSearchParam } from "@/lib/filter-context";
import {
  interpretationReasonText,
  interpretationSummary,
  interpretationTitle,
} from "@/lib/ui-copy";

export default async function Home({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) ?? {};
  const selectedMinister = readSearchParam(params.minister);
  const selectedPeriod = readSearchParam(params.period);
  const selectedCollegiate = readSearchParam(params.collegiate);

  const data = await getDashboardData({
    minister: selectedMinister,
    period: selectedPeriod,
    collegiate: selectedCollegiate,
  });

  const flow = data.selectedSnapshot.data;
  const filterContext = {
    minister: data.selectedSnapshot.minister,
    period: data.selectedSnapshot.period,
    collegiate: flow.collegiate_filter,
  };
  const contextQuery = buildFilterQuery(filterContext);

  return (
    <AppShell
      currentPath="/"
      filterContext={filterContext}
      heroState={
        flow.status === "empty"
          ? {
              status: "empty",
              title: "Ainda não há resultados suficientes com estes filtros",
              description:
                "Tente ampliar o período ou incluir outros tipos de decisão para montar uma leitura útil.",
            }
          : flow.thematic_flow_interpretation_status === "inconclusivo"
            ? {
                status: "inconclusivo",
                title: "Ainda não há contexto suficiente para uma leitura segura",
                description:
                  "Há resultados para este período, mas ainda faltam elementos para uma comparação temática mais firme.",
              }
            : {
                status: "ok",
                title: "Há base suficiente para comparar este período",
                description:
                  "Os dados deste período já permitem uma leitura comparativa inicial sem esconder os limites da análise.",
              }
      }
      eyebrow="Atlas STF · resumo do período"
      title="Entenda rapidamente o período selecionado"
      description="Comece por esta visão para saber o que mudou, o que merece atenção e quais casos valem uma leitura mais cuidadosa."
      guidance={{
        title: "Como ler esta página",
        summary: "Use este resumo como porta de entrada antes de abrir pontos de atenção, casos e nomes relacionados.",
        bullets: [
          "Primeiro veja os números principais para entender o tamanho do período.",
          "Se algo chamar atenção, abra a área de pontos de atenção para entender por quê.",
          "Os sinais aqui ajudam a priorizar leitura. Eles não substituem a análise humana do caso.",
        ],
      }}
    >
      <section className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <FilterBar
          ministers={data.ministers}
          periods={data.periods}
          selectedMinister={data.selectedSnapshot.minister}
          selectedPeriod={data.selectedSnapshot.period}
          selectedCollegiate={flow.collegiate_filter}
          action="/"
        />

        <div className="grid gap-4 rounded-[28px] border border-slate-200/80 bg-white/90 p-5 shadow-[0_20px_70px_rgba(15,23,42,0.08)] backdrop-blur-xl">
          <div className="flex items-center justify-between">
            <div>
              <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Leitura do recorte</p>
              <h2 className="mt-2 text-2xl font-semibold text-slate-950">
                {interpretationTitle(flow.thematic_flow_interpretation_status)}
              </h2>
            </div>
            <Sparkles className="h-6 w-6 text-orange-500" />
          </div>
          <p className="text-sm leading-6 text-slate-600">
            {interpretationSummary(flow.thematic_flow_interpretation_status)}
          </p>
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="rounded-2xl bg-slate-50 p-4">
              <p className="font-mono text-[11px] uppercase tracking-[0.2em] text-slate-500">O que sustenta esta leitura</p>
              <p className="mt-2 text-sm leading-6 text-slate-700">
                {Object.entries(flow.thematic_source_distribution)
                  .map(([key, value]) => `${value} registros com ${key === "serving_process_thematic_key" ? "tema identificado" : "apoio complementar"}`)
                  .join(" · ") || "Ainda não há informação suficiente para resumir este ponto."}
              </p>
            </div>
            <div className="rounded-2xl bg-slate-50 p-4">
              <p className="font-mono text-[11px] uppercase tracking-[0.2em] text-slate-500">Por que a leitura ainda pode estar limitada</p>
              <p className="mt-2 text-sm leading-6 text-slate-700">
                {flow.thematic_flow_interpretation_reasons
                  .map((reason) => interpretationReasonText(reason))
                  .join(" · ") || "Não há bloqueios adicionais para esta leitura inicial."}
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-6">
        <div className="xl:col-span-1"><StatCard icon={Users} label="Ocorrências no período" value={String(data.kpis.selectedEvents)} help="Quantidade de decisões encontradas dentro do filtro atual." /></div>
        <div className="xl:col-span-1"><StatCard icon={BookText} label="Casos analisados" value={String(data.kpis.selectedProcesses)} help="Número de casos diferentes ligados a essas decisões." /></div>
        <div className="xl:col-span-1"><StatCard icon={AlertTriangle} label="Pontos de atenção" value={String(data.kpis.alertCount)} help="Quantidade de sinais que merecem leitura mais cuidadosa." /></div>
        <div className="xl:col-span-1"><StatCard icon={Scale} label="Grupos de comparação" value={String(data.kpis.validGroupCount)} help="Conjuntos usados como referência para comparar comportamentos parecidos." /></div>
        <div className="xl:col-span-1"><StatCard icon={Activity} label="Referências usadas" value={String(data.kpis.baselineCount)} help="Quantidade de referências usadas para dar contexto aos resultados." /></div>
        <div className="xl:col-span-1"><StatCard icon={Sparkles} label="Força média dos sinais" value={data.kpis.averageAlertScore.toFixed(3)} help="Intensidade média dos pontos de atenção mostrados no painel." /></div>
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        {[
          {
            href: buildFilterHref("/ministros", filterContext),
            title: 'Comparar este período',
            text: 'Veja o comportamento do período com mais detalhe e compare tipos de decisão, temas e volume.',
          },
          {
            href: buildFilterHref("/alertas", filterContext),
            title: 'Pontos de atenção',
            text: 'Abra a lista de sinais que merecem revisão e entenda o que foi esperado e o que apareceu.',
          },
          {
            href: data.caseRows[0] ? `/caso/${encodeURIComponent(data.caseRows[0].decisionEventId)}${contextQuery}` : '/caso',
            title: 'Abrir um caso',
            text: 'Entre direto em um caso para entender a decisão, as pessoas ligadas a ele e a documentação disponível.',
          },
        ].map((item) => (
          <Link key={item.href} href={item.href} className="group rounded-[28px] border border-slate-200/80 bg-white/90 p-5 shadow-[0_20px_70px_rgba(15,23,42,0.08)] transition duration-200 hover:border-verde-300 hover:bg-white">
            <p className="font-mono text-xs uppercase tracking-[0.22em] text-slate-500">Navegação</p>
            <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">{item.title}</h2>
            <p className="mt-3 text-sm leading-6 text-slate-600">{item.text}</p>
            <span className="mt-5 inline-flex items-center gap-2 text-sm font-semibold text-verde-700">
              Ver agora
              <ArrowRight className="h-4 w-4 transition group-hover:translate-x-1" />
            </span>
          </Link>
        ))}
      </section>

      <ProfileStrip profiles={data.ministerProfiles} />

      <section className="grid gap-5 xl:grid-cols-2">
        <DailyAreaChart data={dailyRows(flow.daily_counts)} />
        <DistributionDonut title="Tipos de decisão" subtitle="Distribuição do recorte ativo por tipo de decisão." data={toChartRows(flow.decision_type_distribution)} />
        <DistributionBars title="Órgão julgador" subtitle="Volume por órgão julgador no recorte filtrado." data={toChartRows(flow.judging_body_distribution)} valueLabel="eventos" />
        <DistributionBars title="Resultado das decisões" subtitle="Situação final das decisões encontradas neste período." data={toChartRows(flow.decision_progress_distribution)} valueLabel="ocorrências" />
      </section>

      <section className="grid gap-5 xl:grid-cols-2">
        <SegmentBarChart title="Tipo de ação" subtitle="Mostra quais tipos de ação aparecem com mais frequência neste período." data={chartRows(flow.process_class_flow)} />
        <SegmentBarChart title="Temas mais presentes" subtitle="Mostra os temas que mais aparecem entre os casos deste período." data={chartRows(flow.thematic_flow)} />
      </section>

      <div className="rounded-[30px] border border-amber-200/80 bg-amber-50/50 px-6 py-5">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-amber-700">Frequência no recorte — não indica relação especial</p>
        <p className="mt-2 text-sm leading-6 text-amber-900/80">
          Os nomes abaixo são os que mais aparecem nos casos do filtro atual. Isso reflete volume processual, não vínculo com o ministro.
          Atores institucionais como o Procurador-Geral da República aparecem no topo por atuação obrigatória, não por ligação pessoal.
        </p>
      </div>

      <section className="grid gap-5 xl:grid-cols-2">
        <EntityRanking
          title="Representantes mais frequentes"
          subtitle="Nomes que aparecem com mais frequência nos casos deste recorte. A presença aqui reflete volume, não anomalia."
          items={data.topCounsels}
          emptyMessage="Nenhum representante apareceu neste período dentro do filtro atual."
        />
        <EntityRanking
          title="Partes mais frequentes"
          subtitle="Partes que aparecem com mais frequência nos casos deste recorte. Entidades públicas tendem a liderar por atuação institucional."
          items={data.topParties}
          emptyMessage="Nenhuma parte apareceu neste período dentro do filtro atual."
        />
      </section>

      <AlertTable alerts={data.topAlerts} />
      <CaseTable rows={data.caseRows} contextQuery={contextQuery} />
      <SourceAudit sourceFiles={data.sourceFiles} />
    </AppShell>
  );
}
