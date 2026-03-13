import { alertTypeLabel, humanizePattern, ML_ANOMALY_THRESHOLD, signalLabelSimple } from "@/lib/ui-copy";

function dimensionCount(uncertaintyNote: string | null): number | null {
  if (!uncertaintyNote) return null;
  const match = uncertaintyNote.match(/apenas (\d+) dimensão|apenas (\d+) dimensões/);
  if (match) return parseInt(match[1] ?? match[2], 10);
  return null;
}

function strengthLabel(score: number, uncertaintyNote: string | null): { text: string; tone: string } {
  const dims = dimensionCount(uncertaintyNote);
  if (dims !== null && dims <= 1) {
    return { text: "Sinal fraco — apenas 1 dimensão", tone: "text-amber-700 bg-amber-50" };
  }
  if (score >= 0.9) {
    return { text: "Sinal forte", tone: "text-rose-700 bg-rose-50" };
  }
  if (score >= 0.7) {
    return { text: "Sinal moderado", tone: "text-orange-700 bg-orange-50" };
  }
  return { text: "Sinal leve", tone: "text-slate-600 bg-slate-50" };
}

export function AlertTable({
  alerts,
}: {
  alerts: Array<{
    alert_id: string;
    process_id: string;
    decision_event_id: string;
    alert_type: string;
    alert_score: number;
    ensemble_score: number | null;
    status: string;
    expected_pattern: string;
    observed_pattern: string;
    uncertainty_note: string | null;
    risk_signal_count?: number;
    risk_signals?: string[];
    processNumber?: string;
    processClass?: string;
    decisionDate?: string;
  }>;
}) {
  const meaningful = alerts.filter((a) => {
    const dims = dimensionCount(a.uncertainty_note);
    return dims === null || dims >= 2;
  });

  if (meaningful.length === 0) return null;

  return (
    <section className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
      <div className="max-w-3xl">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Pontos de atenção</p>
        <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">
          Movimentos fora do padrão esperado
        </h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">
          Cada cartão mostra uma decisão cujo perfil difere do padrão estatístico do grupo de comparação.
          Isso não significa irregularidade — apenas que o caso merece uma leitura mais atenta para entender o contexto.
        </p>
      </div>

      <div className="mt-6 grid gap-4">
        {meaningful.map((alert) => {
          const strength = strengthLabel(alert.alert_score, alert.uncertainty_note);

          return (
            <article key={alert.alert_id} className="rounded-[24px] border border-slate-200 bg-slate-50/70 p-4 transition hover:border-verde-300 hover:bg-verde-50/40">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="rounded-full bg-orange-100 px-3 py-1 text-xs font-semibold text-orange-700">
                      {alertTypeLabel(alert.alert_type)}
                    </span>
                    <span className={`rounded-full px-3 py-1 text-xs font-semibold ${strength.tone}`}>
                      {strength.text}
                    </span>
                    {alert.ensemble_score !== null && alert.ensemble_score > ML_ANOMALY_THRESHOLD ? (
                      <span className="rounded-full bg-rose-100 px-3 py-1 text-xs font-semibold text-rose-700">
                        Anomalia ML
                      </span>
                    ) : null}
                    {(alert.risk_signals ?? []).map((signal) => (
                      <span key={signal} className={`rounded-full px-3 py-1 text-xs font-semibold ${signal === "sanction" ? "bg-rose-100 text-rose-700" : signal === "donation" ? "bg-amber-100 text-amber-700" : signal === "corporate" ? "bg-blue-100 text-blue-700" : signal === "affinity" ? "bg-purple-100 text-purple-700" : "bg-orange-100 text-orange-700"}`}>
                        {signalLabelSimple(signal)}
                      </span>
                    ))}
                  </div>
                  {alert.processNumber ? (
                    <p className="mt-3 text-lg font-semibold text-slate-950">
                      {alert.processClass ? `${alert.processClass} — ` : ""}{alert.processNumber}
                    </p>
                  ) : (
                    <p className="mt-3 text-sm font-medium text-slate-900">Caso sem número identificado</p>
                  )}
                  {alert.decisionDate ? (
                    <p className="mt-1 text-sm text-slate-500">Decisão em {alert.decisionDate}</p>
                  ) : null}
                </div>
                <div className="rounded-[20px] bg-white px-4 py-3 text-right shadow-sm">
                  <p className="font-mono text-[11px] uppercase tracking-[0.2em] text-slate-500">Intensidade do sinal</p>
                  <p className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">{alert.alert_score.toFixed(3)}</p>
                </div>
              </div>

              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <div className="rounded-2xl bg-white p-4">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Padrão esperado no grupo</p>
                  <p className="mt-2 text-sm leading-6 text-slate-700">{humanizePattern(alert.expected_pattern)}</p>
                </div>
                <div className="rounded-2xl bg-white p-4">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">O que apareceu neste caso</p>
                  <p className="mt-2 text-sm leading-6 text-slate-700">{humanizePattern(alert.observed_pattern)}</p>
                </div>
              </div>

              <div className="mt-4 grid gap-3 md:grid-cols-[minmax(0,1fr)_auto]">
                <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-700">
                  <span className="font-semibold text-slate-900">Contexto:</span>{" "}
                  {alert.uncertainty_note
                    ? humanizePattern(alert.uncertainty_note)
                    : "Este sinal é informativo. Use o caso relacionado para entender o contexto antes de tirar conclusões."}
                </div>
                {alert.ensemble_score !== null ? (
                  <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-700">
                    <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Score combinado</p>
                    <p className="mt-1 text-lg font-semibold text-slate-950">{alert.ensemble_score.toFixed(3)}</p>
                  </div>
                ) : null}
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}
