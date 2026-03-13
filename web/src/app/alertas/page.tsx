import Link from "next/link";
import { AlertTriangle, ArrowRight, Scale, Sparkles } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { FilterBar } from "@/components/dashboard/filter-bar";
import { SourceAudit } from "@/components/dashboard/source-audit";
import { StatCard } from "@/components/dashboard/stat-card";
import { getAlertsPageData } from "@/lib/dashboard-data";
import { buildFilterHref, readSearchParam } from "@/lib/filter-context";
import { alertTypeLabel, humanizePattern, ML_ANOMALY_THRESHOLD, signalLabelSimple } from "@/lib/ui-copy";

function dimensionCount(note: string | null): number | null {
  if (!note) return null;
  const match = note.match(/apenas (\d+) dimensão|apenas (\d+) dimensões/);
  if (match) return parseInt(match[1] ?? match[2], 10);
  return null;
}

function strengthLabel(score: number, note: string | null): { text: string; tone: string } {
  const dims = dimensionCount(note);
  if (dims !== null && dims <= 1) {
    return { text: "Sinal fraco — apenas 1 dimensão", tone: "text-amber-700 bg-amber-50" };
  }
  if (score >= 0.9) return { text: "Sinal forte", tone: "text-rose-700 bg-rose-50" };
  if (score >= 0.7) return { text: "Sinal moderado", tone: "text-orange-700 bg-orange-50" };
  return { text: "Sinal leve", tone: "text-slate-600 bg-slate-50" };
}

export default async function AlertsPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) ?? {};
  const data = await getAlertsPageData({
    minister: readSearchParam(params.minister),
    period: readSearchParam(params.period),
    collegiate: readSearchParam(params.collegiate),
  });
  const filterContext = {
    minister: data.selectedSnapshot.minister,
    period: data.selectedSnapshot.period,
    collegiate: data.selectedSnapshot.data.collegiate_filter,
  };

  const meaningful = data.alertDetails.filter((a) => {
    const dims = dimensionCount(a.uncertainty_note);
    return dims === null || dims >= 2;
  });

  return (
    <AppShell
      currentPath="/alertas"
      filterContext={filterContext}
      heroState={
        meaningful.length === 0
            ? {
                status: "empty",
                title: "Nenhum ponto de atenção relevante neste período",
                description:
                  "Não encontramos sinais com força suficiente para revisão dentro do filtro atual.",
              }
          : data.selectedSnapshot.data.thematic_flow_interpretation_status === "inconclusivo"
            ? {
                status: "inconclusivo",
                title: "Há sinais, mas ainda falta contexto para interpretá-los melhor",
                description:
                  "Os pontos de atenção aparecem neste período, mas ainda exigem leitura cuidadosa do caso antes de qualquer interpretação mais forte.",
              }
            : {
                status: "ok",
                title: "Pontos de atenção ligados ao período selecionado",
                description:
                  "Os sinais mostrados aqui ajudam a priorizar a revisão humana dos casos mais relevantes.",
              }
      }
      eyebrow="Atlas STF · pontos de atenção"
      title="Movimentos fora do padrão esperado"
      description="Cada item mostra uma decisão cujo perfil difere do padrão estatístico do grupo de comparação. Isso não significa irregularidade — apenas que o caso merece uma leitura mais atenta. Sinais com apenas 1 dimensão discriminativa são omitidos por serem pouco informativos."
      guidance={{
        title: "Como interpretar um ponto de atenção",
        summary: "Use esta tela para decidir quais casos vale abrir primeiro.",
        bullets: [
          "Leia sempre o que era esperado e o que apareceu neste caso.",
          "Um ponto de atenção informa que algo saiu do padrão. Ele não prova causa, intenção ou irregularidade.",
          "Se o contexto ainda estiver curto, abra o caso para entender melhor antes de comparar.",
        ],
      }}
    >
      <FilterBar
        ministers={data.ministers}
        periods={data.periods}
        selectedMinister={data.selectedSnapshot.minister}
        selectedPeriod={data.selectedSnapshot.period}
        selectedCollegiate={data.selectedSnapshot.data.collegiate_filter}
        action="/alertas"
      />

      <section className="grid gap-4 md:grid-cols-3">
        <StatCard icon={AlertTriangle} label="Sinais relevantes" value={String(meaningful.length)} help="Alertas com 2 ou mais dimensões discriminativas no filtro atual." />
        <StatCard icon={Scale} label="Grupos de comparação" value={String(data.kpis.validGroupCount)} help="Conjuntos usados para comparar contextos parecidos." />
        <StatCard icon={Sparkles} label="Total no banco (antes do filtro)" value={String(data.filteredAlertCount)} help={`Total de alertas no banco para este recorte, incluindo sinais fracos omitidos.`} />
      </section>

      {meaningful.length === 0 ? (
        <section className="rounded-[28px] border border-slate-200/80 bg-white/95 p-6 text-slate-600 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
          Não encontramos pontos de atenção com força suficiente nestes filtros. Sinais com apenas 1 dimensão discriminativa foram omitidos. Tente ampliar o período ou incluir outros tipos de decisão.
        </section>
      ) : (
      <section className="grid gap-4">
        {meaningful.map((alert) => {
          const strength = strengthLabel(alert.alert_score, alert.uncertainty_note);

          return (
            <article key={alert.alert_id} className="rounded-[28px] border border-slate-200/80 bg-white/95 p-5 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="rounded-full bg-orange-100 px-3 py-1 text-xs font-semibold text-orange-700">{alertTypeLabel(alert.alert_type)}</span>
                    <span className={`rounded-full px-3 py-1 text-xs font-semibold ${strength.tone}`}>{strength.text}</span>
                    {alert.ensemble_score !== null && alert.ensemble_score > ML_ANOMALY_THRESHOLD ? (
                      <span className="rounded-full bg-rose-100 px-3 py-1 text-xs font-semibold text-rose-700">Anomalia ML</span>
                    ) : null}
                    {(alert.risk_signals ?? []).map((signal) => (
                      <span key={signal} className={`rounded-full px-3 py-1 text-xs font-semibold ${signal === "sanction" ? "bg-rose-100 text-rose-700" : signal === "donation" ? "bg-amber-100 text-amber-700" : signal === "corporate" ? "bg-blue-100 text-blue-700" : signal === "affinity" ? "bg-purple-100 text-purple-700" : "bg-orange-100 text-orange-700"}`}>
                        {signalLabelSimple(signal)}
                      </span>
                    ))}
                  </div>
                  <p className="mt-3 text-lg font-semibold text-slate-950">{alert.processNumber} · {alert.decisionDate}</p>
                  <p className="mt-1 text-sm text-slate-600">{alert.processClass} · {alert.judgingBody} · {alert.collegiateLabel}</p>
                </div>
                <div className="rounded-[20px] bg-slate-950 px-4 py-3 text-right text-white shadow-sm">
                  <p className="font-mono text-[11px] uppercase tracking-[0.2em] text-verde-100">Intensidade do sinal</p>
                  <p className="mt-1 text-2xl font-semibold tracking-tight">{alert.alert_score.toFixed(3)}</p>
                </div>
              </div>

              <div className="mt-4 grid gap-4 xl:grid-cols-[1fr_1fr_auto]">
                <div className="rounded-2xl bg-slate-50 p-4">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Padrão esperado no grupo</p>
                  <p className="mt-2 text-sm leading-6 text-slate-700">{humanizePattern(alert.expected_pattern)}</p>
                </div>
                <div className="rounded-2xl bg-slate-50 p-4">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">O que apareceu neste caso</p>
                  <p className="mt-2 text-sm leading-6 text-slate-700">{humanizePattern(alert.observed_pattern)}</p>
                </div>
                <div className="flex items-end">
                  <Link
                    href={buildFilterHref(`/caso/${encodeURIComponent(alert.decision_event_id)}`, filterContext)}
                    className="inline-flex h-11 cursor-pointer items-center justify-center gap-2 rounded-2xl bg-verde-700 px-4 text-sm font-semibold text-white transition hover:bg-verde-800"
                  >
                    Ver caso
                    <ArrowRight className="h-4 w-4" />
                  </Link>
                </div>
              </div>

              <div className="mt-4 grid gap-3 md:grid-cols-3">
                <div className="rounded-2xl border border-slate-200 p-4 text-sm text-slate-600">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Documentos disponíveis</p>
                  <p className="mt-2">{alert.docCountLabel} peças · {alert.acordaoLabel}</p>
                </div>
                <div className="rounded-2xl border border-slate-200 p-4 text-sm text-slate-600">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Tema principal</p>
                  <p className="mt-2">{alert.firstSubject !== 'INCERTO' ? alert.firstSubject : alert.branchOfLaw}</p>
                </div>
                <div className="rounded-2xl border border-slate-200 p-4 text-sm text-slate-600">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Contexto do sinal</p>
                  <p className="mt-2">{alert.uncertainty_note ? humanizePattern(alert.uncertainty_note) : "Sinal informativo que pede leitura do caso."}</p>
                  {alert.ensemble_score !== null ? (
                    <p className="mt-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                      Score combinado: {alert.ensemble_score.toFixed(3)}
                    </p>
                  ) : null}
                </div>
              </div>
            </article>
          );
        })}
      </section>
      )}

      <SourceAudit sourceFiles={data.sourceFiles} />
    </AppShell>
  );
}
