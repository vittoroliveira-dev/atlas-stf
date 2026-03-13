import type {
  TemporalCorporateLinkItem,
  TemporalEventItem,
  TemporalMonthlyItem,
  TemporalYoyItem,
} from "@/lib/temporal-analysis-data";
import { EventTable, formatDelta, formatPercent } from "./temporal-tables";

export function MinisterDetail({
  minister,
  monthly,
  yoy,
  events,
  corporateLinks,
}: {
  minister: string;
  monthly: TemporalMonthlyItem[];
  yoy: TemporalYoyItem[];
  events: TemporalEventItem[];
  corporateLinks: TemporalCorporateLinkItem[];
}) {
  return (
    <section className="grid gap-5">
      <div className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">
          Drill-down
        </p>
        <h2 className="mt-3 text-2xl font-semibold text-slate-950">{minister}</h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">
          S\u00e9rie mensal descritiva, compara\u00e7\u00e3o ano contra ano por classe,
          eventos documentados e v\u00ednculos societ\u00e1rios ativos desde a entrada.
        </p>
      </div>

      <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
        <h3 className="mb-4 text-lg font-semibold text-slate-950">
          S\u00e9rie mensal e m\u00e9dia m\u00f3vel de 6 meses
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
                <th className="px-3 py-2">M\u00eas</th>
                <th className="px-3 py-2">Decis\u00f5es</th>
                <th className="px-3 py-2">Taxa favor\u00e1vel</th>
                <th className="px-3 py-2">M\u00e9dia m\u00f3vel</th>
                <th className="px-3 py-2">Score</th>
                <th className="px-3 py-2">Mudan\u00e7a</th>
              </tr>
            </thead>
            <tbody>
              {monthly.map((row) => (
                <tr key={row.record_id} className="border-b border-slate-100">
                  <td className="px-3 py-2 font-mono text-xs">{row.decision_month}</td>
                  <td className="px-3 py-2">{row.decision_count}</td>
                  <td className="px-3 py-2">{formatPercent(row.favorable_rate)}</td>
                  <td className="px-3 py-2">
                    {formatPercent(row.rolling_favorable_rate_6m)}
                  </td>
                  <td className="px-3 py-2">{row.breakpoint_score?.toFixed(2) ?? "\u2014"}</td>
                  <td className="px-3 py-2">
                    {row.breakpoint_flag ? "mudan\u00e7a de padr\u00e3o" : "\u2014"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
        <h3 className="mb-4 text-lg font-semibold text-slate-950">
          Comparativo ano contra ano por classe processual
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
                <th className="px-3 py-2">Classe</th>
                <th className="px-3 py-2">Ano</th>
                <th className="px-3 py-2">Taxa atual</th>
                <th className="px-3 py-2">Taxa anterior</th>
                <th className="px-3 py-2">Delta</th>
              </tr>
            </thead>
            <tbody>
              {yoy.map((row) => (
                <tr key={row.record_id} className="border-b border-slate-100">
                  <td className="px-3 py-2 font-mono text-xs">{row.process_class}</td>
                  <td className="px-3 py-2">{row.decision_year}</td>
                  <td className="px-3 py-2">
                    {formatPercent(row.current_favorable_rate)}
                  </td>
                  <td className="px-3 py-2">{formatPercent(row.prior_favorable_rate)}</td>
                  <td className="px-3 py-2">{formatDelta(row.delta_vs_prior_year)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <EventTable rows={events} />

      <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
        <h3 className="mb-4 text-lg font-semibold text-slate-950">
          Linha do tempo de v\u00ednculos societ\u00e1rios
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
                <th className="px-3 py-2">In\u00edcio observ\u00e1vel</th>
                <th className="px-3 py-2">Empresa</th>
                <th className="px-3 py-2">Entidade ligada</th>
                <th className="px-3 py-2">Grau</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">Taxa favor\u00e1vel</th>
              </tr>
            </thead>
            <tbody>
              {corporateLinks.map((row) => (
                <tr key={row.record_id} className="border-b border-slate-100">
                  <td className="px-3 py-2 font-mono text-xs">{row.link_start_date ?? "\u2014"}</td>
                  <td className="px-3 py-2">{row.company_name ?? row.company_cnpj_basico}</td>
                  <td className="px-3 py-2">{row.linked_entity_name}</td>
                  <td className="px-3 py-2">{row.link_degree ?? "\u2014"}</td>
                  <td className="px-3 py-2">{row.link_status ?? "\u2014"}</td>
                  <td className="px-3 py-2">{formatPercent(row.favorable_rate)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
}
