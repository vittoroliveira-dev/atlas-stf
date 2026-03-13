import Link from "next/link";
import { ArrowRight, FileSearch, Sparkles, TableProperties } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { CaseTable } from "@/components/dashboard/case-table";
import { FilterBar } from "@/components/dashboard/filter-bar";
import { StatCard } from "@/components/dashboard/stat-card";
import { SourceAudit } from "@/components/dashboard/source-audit";
import { getDashboardData } from "@/lib/dashboard-data";
import { buildFilterQuery, readSearchParam } from "@/lib/filter-context";

export default async function CaseIndexPage({
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
      currentPath="/caso"
      filterContext={filterContext}
      heroState={
        flow.status === "empty"
            ? {
                status: "empty",
                title: "Nenhum caso disponível com estes filtros",
                description:
                  "Amplie o período ou mude o tipo de decisão para encontrar casos nesta visão.",
            }
          : flow.thematic_flow_interpretation_status === "inconclusivo"
            ? {
                status: "inconclusivo",
                title: "Os casos estão disponíveis, mas ainda pedem contexto adicional",
                description:
                  "Há casos para leitura, porém o período ainda não sustenta uma comparação temática mais firme.",
              }
            : {
                status: "ok",
                title: "Casos prontos para leitura",
                description:
                  "Você já pode abrir os casos deste período para entender a decisão, o contexto e os nomes envolvidos.",
              }
      }
      eyebrow="Atlas STF · lista de casos"
      title="Casos para explorar neste período"
      description="Use esta página para encontrar casos do filtro atual e abrir a leitura detalhada de cada um."
      guidance={{
        title: "Como usar a lista de casos",
        summary: "Esta tela ajuda a sair da visão geral e entrar na leitura concreta de um caso específico.",
        bullets: [
          "Use os filtros para restringir o tipo de decisão ou de ação que você quer ver.",
          "Abra um caso para entender o que aconteceu, quem aparece nele e quais documentos estão disponíveis.",
          "Se você veio da área de pontos de atenção, use esta tela para aprofundar a leitura do caso ligado ao sinal.",
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
        action="/caso"
      />

      <section className="grid gap-4 md:grid-cols-3">
        <StatCard icon={FileSearch} label="Casos nesta lista" value={String(data.caseRows.length)} help="Quantidade de casos encontrados com os filtros atuais." />
        <StatCard icon={TableProperties} label="Ministro analisado" value={data.selectedSnapshot.minister} help="Nome usado como base para esta leitura." />
        <StatCard icon={Sparkles} label="Período analisado" value={data.selectedSnapshot.period} help="Período usado para montar a lista atual de casos." />
      </section>

      {data.caseRows[0] ? (
        <section className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
          <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Atalho rápido</p>
          <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">Abrir o primeiro caso da lista</h2>
          <p className="mt-2 text-sm leading-6 text-slate-600">
            Se quiser começar imediatamente, use este botão. A lista logo abaixo também permite abrir qualquer outro caso.
          </p>
          <Link
            href={`/caso/${encodeURIComponent(data.caseRows[0].decisionEventId)}${contextQuery}`}
            className="mt-5 inline-flex h-12 cursor-pointer items-center justify-center gap-2 rounded-2xl bg-slate-950 px-5 text-sm font-semibold text-white transition hover:bg-slate-800"
          >
            Abrir caso
            <ArrowRight className="h-4 w-4" />
          </Link>
        </section>
      ) : null}

      <CaseTable rows={data.caseRows} contextQuery={contextQuery} />
      <SourceAudit sourceFiles={data.sourceFiles} />
    </AppShell>
  );
}
