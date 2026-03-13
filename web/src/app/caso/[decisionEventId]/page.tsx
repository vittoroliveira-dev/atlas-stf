import Link from "next/link";
import { ArrowRight, BookText, FileSearch, Scale, Sparkles } from "lucide-react";
import { AlertTable } from "@/components/dashboard/alert-table";
import { AppShell } from "@/components/dashboard/app-shell";
import { CaseEntities } from "@/components/dashboard/entity-ranking";
import { FilterBar } from "@/components/dashboard/filter-bar";
import { SourceAudit } from "@/components/dashboard/source-audit";
import { StatCard } from "@/components/dashboard/stat-card";
import { getCaseDetailData } from "@/lib/dashboard-data";
import { buildFilterHref, readSearchParam } from "@/lib/filter-context";
import { getSafeExternalHref } from "@/lib/safe-external-url";
import { formatDateSafe } from "@/lib/ui-copy";

export default async function CaseDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ decisionEventId: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const route = await params;
  const query = (await searchParams) ?? {};
  const judgingBody = readSearchParam(query.judging_body);
  const processClass = readSearchParam(query.process_class);
  const data = await getCaseDetailData({
    minister: readSearchParam(query.minister),
    period: readSearchParam(query.period),
    collegiate: readSearchParam(query.collegiate),
    judgingBody,
    processClass,
    processId: readSearchParam(query.processId),
    decisionEventId: route.decisionEventId,
  });

  const selectedCase = data.selectedCase;
  const inteiroTeorHref = getSafeExternalHref(selectedCase?.inteiroTeorUrl);
  const filterContext = {
    minister: data.selectedSnapshot.minister,
    period: data.selectedSnapshot.period,
    collegiate: data.selectedSnapshot.data.collegiate_filter,
    judgingBody,
    processClass,
  };

  return (
    <AppShell
      currentPath="/caso"
      filterContext={filterContext}
      heroState={
        selectedCase == null
            ? {
                status: "empty",
                title: "Não encontramos este caso com os filtros atuais",
                description:
                  "Tente abrir este caso a partir da lista de casos ou ajustar o período selecionado.",
            }
          : data.selectedSnapshot.data.thematic_flow_interpretation_status === "inconclusivo"
            ? {
                status: "inconclusivo",
                title: "O caso foi encontrado, mas ainda pede contexto adicional",
                description:
                  "Há informação suficiente para leitura do caso, mas não para interpretações comparativas mais fortes.",
              }
            : {
                status: "ok",
                title: "Caso pronto para leitura",
                description:
                  "Abaixo você vê o resumo do caso, a decisão e os sinais relacionados dentro do período selecionado.",
              }
      }
      eyebrow="Atlas STF · detalhe do caso"
      title={selectedCase ? `Caso ${selectedCase.processNumber}` : 'Caso não encontrado no recorte'}
      description="Esta página reúne o essencial para entender o caso, a decisão, os nomes envolvidos e os pontos de atenção relacionados."
      guidance={{
        title: "Como ler o detalhe do caso",
        summary: "Comece pelo resumo do caso, depois veja a decisão e, por fim, os nomes e sinais relacionados.",
        bullets: [
          "Use o bloco principal para entender o que aconteceu e em que contexto.",
          "Abra a documentação quando ela estiver disponível para aprofundar a leitura.",
          "Leia os pontos de atenção como sinais informativos, nunca como conclusão automática.",
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
        selectedCollegiate={data.selectedSnapshot.data.collegiate_filter}
        selectedJudgingBody={judgingBody}
        selectedProcessClass={processClass}
        action="/caso"
      />

      {selectedCase ? (
        <>
          <section className="grid gap-4 md:grid-cols-4">
            <StatCard icon={FileSearch} label="Data da decisão" value={selectedCase.decisionDate} help="Dia em que a decisão mostrada nesta página foi registrada." />
            <StatCard icon={BookText} label="Documentos disponíveis" value={selectedCase.docCountLabel} help="Quantidade de documentos ligados a este caso nesta análise." />
            <StatCard icon={Scale} label="Tipo de ação" value={selectedCase.processClass} help="Categoria principal do caso." />
            <StatCard icon={Sparkles} label="Onde foi decidido" value={selectedCase.judgingBody} help="Local de decisão informado para este caso." />
          </section>

          <section className="grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
            <article className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
              <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Resumo do caso</p>
              <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">{selectedCase.processNumber}</h2>
              <dl className="mt-5 grid gap-4 md:grid-cols-2">
                <div><dt className="text-sm text-slate-500">Data da decisão</dt><dd className="mt-1 font-semibold text-slate-950">{selectedCase.decisionDate}</dd></div>
                <div><dt className="text-sm text-slate-500">Tipo de decisão</dt><dd className="mt-1 font-semibold text-slate-950">{selectedCase.decisionType}</dd></div>
                <div><dt className="text-sm text-slate-500">Resultado</dt><dd className="mt-1 font-semibold text-slate-950">{selectedCase.decisionProgress}</dd></div>
                <div><dt className="text-sm text-slate-500">Forma de decisão</dt><dd className="mt-1 font-semibold text-slate-950">{selectedCase.collegiateLabel}</dd></div>
                <div><dt className="text-sm text-slate-500">Origem</dt><dd className="mt-1 font-semibold text-slate-950">{selectedCase.originDescription}</dd></div>
                <div><dt className="text-sm text-slate-500">Tema principal</dt><dd className="mt-1 font-semibold text-slate-950">{selectedCase.firstSubject !== 'INCERTO' ? selectedCase.firstSubject : selectedCase.branchOfLaw}</dd></div>
              </dl>
              <div className="mt-6 rounded-2xl bg-slate-50 p-4">
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Resumo da decisão</p>
                <p className="mt-2 text-sm leading-6 text-slate-700">{selectedCase.decisionNoteSnippet}</p>
              </div>
            </article>

            <article className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
              <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Documentação</p>
              <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">O que pode ser consultado</h2>
              <div className="mt-5 grid gap-4">
                <div className="rounded-2xl border border-slate-200 p-4 text-sm text-slate-600">
                  <p>Acórdão: <span className="font-semibold text-slate-950">{selectedCase.acordaoLabel}</span></p>
                  <p className="mt-2">Decisão monocrática: <span className="font-semibold text-slate-950">{selectedCase.monocraticDecisionLabel}</span></p>
                </div>
                {inteiroTeorHref ? (
                  <a href={inteiroTeorHref} target="_blank" rel="noreferrer" className="inline-flex h-12 cursor-pointer items-center justify-center gap-2 rounded-2xl bg-slate-950 px-5 text-sm font-semibold text-white transition hover:bg-slate-800">
                    Abrir documento completo
                    <ArrowRight className="h-4 w-4" />
                  </a>
                ) : (
                  <div className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                    Não há documento completo disponível para este caso na base atual.
                  </div>
                )}
              </div>
            </article>
          </section>

          <section className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
            <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Leitura comparativa adicional</p>
            <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">ML outlier por Isolation Forest</h2>
            {data.mlOutlierAnalysis ? (
              <div className="mt-5 grid gap-4 md:grid-cols-4">
                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm text-slate-500">ML anomaly</p>
                  <p className="mt-1 text-2xl font-semibold text-slate-950">{data.mlOutlierAnalysis.mlAnomalyScore.toFixed(3)}</p>
                </div>
                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm text-slate-500">Raridade estimada</p>
                  <p className="mt-1 text-2xl font-semibold text-slate-950">{data.mlOutlierAnalysis.mlRarityScore.toFixed(3)}</p>
                </div>
                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm text-slate-500">Score combinado</p>
                  <p className="mt-1 text-2xl font-semibold text-slate-950">
                    {data.mlOutlierAnalysis.ensembleScore !== null
                      ? data.mlOutlierAnalysis.ensembleScore.toFixed(3)
                      : "INCERTO"}
                  </p>
                </div>
                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm text-slate-500">Grupo comparável</p>
                  <p className="mt-1 text-sm font-semibold text-slate-950">{data.mlOutlierAnalysis.comparisonGroupId}</p>
                  <p className="mt-2 text-xs text-slate-500">
                    {data.mlOutlierAnalysis.nSamples} eventos, {data.mlOutlierAnalysis.nFeatures} variáveis
                  </p>
                  {data.mlOutlierAnalysis.generatedAt ? (
                    <p className="mt-2 text-xs text-slate-500">
                      Gerado em {formatDateSafe(data.mlOutlierAnalysis.generatedAt)}
                    </p>
                  ) : null}
                </div>
              </div>
            ) : (
              <div className="mt-5 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                Não há score de ML outlier disponível para este caso na base atual.
              </div>
            )}
            <p className="mt-4 text-sm leading-6 text-slate-600">
              Este bloco é descritivo. Ele mostra como o pipeline de ML classificou a raridade deste caso dentro do grupo comparável, sem inferir causa ou intenção.
            </p>
          </section>

          <section className="flex justify-start">
            <Link
              href={buildFilterHref("/caso", filterContext)}
              className="inline-flex h-11 cursor-pointer items-center justify-center gap-2 rounded-2xl border border-slate-300 px-4 text-sm font-semibold text-slate-900 transition hover:border-verde-600 hover:text-verde-700"
            >
              Voltar para a lista de casos
              <ArrowRight className="h-4 w-4" />
            </Link>
          </section>

          <AlertTable alerts={data.relatedAlerts} />
          <CaseEntities
            title="Pessoas e organizações ligadas a este caso"
            subtitle="Aqui você vê quem aparece no caso e como essa ligação foi identificada dentro da análise."
            counsels={data.counsels}
            parties={data.parties}
          />
        </>
      ) : (
        <section className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)] text-slate-600">
          Não encontramos este caso com os filtros atuais. Tente voltar para a lista de casos ou abrir esta página a partir da área de pontos de atenção.
        </section>
      )}

      <SourceAudit sourceFiles={data.sourceFiles} />
    </AppShell>
  );
}
