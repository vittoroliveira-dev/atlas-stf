import Link from "next/link";
import type {
  TemporalEventItem,
  TemporalMonthlyItem,
  TemporalSeasonalityItem,
} from "@/lib/temporal-analysis-data";
import { getSafeExternalHref } from "@/lib/safe-external-url";

export const MONTH_LABELS = [
  "",
  "Jan",
  "Fev",
  "Mar",
  "Abr",
  "Mai",
  "Jun",
  "Jul",
  "Ago",
  "Set",
  "Out",
  "Nov",
  "Dez",
];

export function formatPercent(value: number | null): string {
  return value == null ? "\u2014" : `${(value * 100).toFixed(1)}%`;
}

export function formatDelta(value: number | null): string {
  if (value == null) return "\u2014";
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${(value * 100).toFixed(1)} pp`;
}

export function aggregateSeasonality(rows: TemporalSeasonalityItem[]) {
  const byMonth = new Map<number, { decisions: number; favorable: number; unfavorable: number }>();
  for (const row of rows) {
    if (!row.month_of_year) continue;
    const current = byMonth.get(row.month_of_year) ?? {
      decisions: 0,
      favorable: 0,
      unfavorable: 0,
    };
    current.decisions += row.decision_count;
    current.favorable += row.favorable_count;
    current.unfavorable += row.unfavorable_count;
    byMonth.set(row.month_of_year, current);
  }
  return Array.from(byMonth.entries())
    .map(([month, values]) => ({
      month,
      decisionCount: values.decisions,
      favorableRate:
        values.decisions > 0 ? values.favorable / values.decisions : null,
    }))
    .sort((left, right) => left.month - right.month);
}

export function BreakpointTable({ rows }: { rows: TemporalMonthlyItem[] }) {
  if (rows.length === 0) {
    return (
      <p className="text-sm text-slate-500">
        Nenhuma mudança de padrão materializada neste recorte.
      </p>
    );
  }
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">
        Mudanças de padrão por mês
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-3 py-2">Ministro</th>
              <th className="px-3 py-2">Mês</th>
              <th className="px-3 py-2">Decisões</th>
              <th className="px-3 py-2">Taxa favorável</th>
              <th className="px-3 py-2">Média móvel 6m</th>
              <th className="px-3 py-2">Score</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.record_id} className="border-b border-slate-100">
                <td className="px-3 py-2">
                  <Link
                    href={`/temporal?minister=${encodeURIComponent(row.rapporteur ?? "")}`}
                    className="text-verde-700 hover:underline"
                  >
                    {row.rapporteur}
                  </Link>
                </td>
                <td className="px-3 py-2 font-mono text-xs">{row.decision_month}</td>
                <td className="px-3 py-2">{row.decision_count}</td>
                <td className="px-3 py-2">{formatPercent(row.favorable_rate)}</td>
                <td className="px-3 py-2">
                  {formatPercent(row.rolling_favorable_rate_6m)}
                </td>
                <td className="px-3 py-2">{row.breakpoint_score?.toFixed(2) ?? "\u2014"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function EventTable({ rows }: { rows: TemporalEventItem[] }) {
  if (rows.length === 0) {
    return (
      <p className="text-sm text-slate-500">
        Nenhum evento externo documentado foi materializado.
      </p>
    );
  }
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">
        Antes e depois de eventos documentados
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-3 py-2">Evento</th>
              <th className="px-3 py-2">Ministro</th>
              <th className="px-3 py-2">Data</th>
              <th className="px-3 py-2">Status</th>
              <th className="px-3 py-2">Antes</th>
              <th className="px-3 py-2">Depois</th>
              <th className="px-3 py-2">Delta</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.record_id} className="border-b border-slate-100">
                <td className="px-3 py-2">
                  {getSafeExternalHref(row.source_url) ? (
                    <a
                      href={getSafeExternalHref(row.source_url) ?? undefined}
                      target="_blank"
                      rel="noreferrer"
                      className="text-verde-700 hover:underline"
                    >
                      {row.event_title ?? row.event_id}
                    </a>
                  ) : (
                    row.event_title ?? row.event_id
                  )}
                </td>
                <td className="px-3 py-2">{row.rapporteur ?? "Global"}</td>
                <td className="px-3 py-2 font-mono text-xs">{row.event_date ?? "\u2014"}</td>
                <td className="px-3 py-2">{row.status ?? "\u2014"}</td>
                <td className="px-3 py-2">{formatPercent(row.before_favorable_rate)}</td>
                <td className="px-3 py-2">{formatPercent(row.after_favorable_rate)}</td>
                <td className="px-3 py-2">{formatDelta(row.delta_before_after)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function SeasonalityTable({ rows }: { rows: TemporalSeasonalityItem[] }) {
  const aggregated = aggregateSeasonality(rows);
  if (aggregated.length === 0) {
    return (
      <p className="text-sm text-slate-500">
        Ainda não há sazonalidade suficiente para leitura.
      </p>
    );
  }
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">
        Sazonalidade agregada por mês
      </h2>
      <div className="grid gap-3 md:grid-cols-4 xl:grid-cols-6">
        {aggregated.map((row) => (
          <article
            key={row.month}
            className="rounded-2xl border border-slate-200 bg-slate-50 p-4"
          >
            <p className="font-mono text-xs uppercase tracking-[0.22em] text-slate-500">
              {MONTH_LABELS[row.month]}
            </p>
            <p className="mt-2 text-2xl font-semibold text-slate-950">
              {row.decisionCount}
            </p>
            <p className="mt-2 text-sm text-slate-600">
              Taxa favorável {formatPercent(row.favorableRate)}
            </p>
          </article>
        ))}
      </div>
    </section>
  );
}
