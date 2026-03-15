import { AlertTriangle, Calendar, FileText, Shield, Users } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { StatCard } from "@/components/dashboard/stat-card";
import { getAgendaPageData } from "@/lib/agenda-data";
import Link from "next/link";

export default async function AgendaPage() {
  const data = await getAgendaPageData();
  return (
    <AppShell currentPath="/agenda" eyebrow="Atlas STF · agenda ministerial" title="Agenda ministerial publica"
      description="Exposicao temporal entre agenda publica e atos processuais subsequentes."
      guidance={{ title: "Como usar esta pagina", summary: "Analise a exposicao temporal entre eventos de agenda e decisoes.",
        bullets: ["Cobertura parcial: dados publicos de agenda para subconjunto de ministros desde jan/2024.",
          "Ausencia de registro nao significa ausencia de contato.",
          "Dados servem para priorizacao investigativa, nao para inferencia causal."] }}>
      <div className="rounded-xl border border-ouro-200 bg-ouro-50 p-4">
        <div className="flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 shrink-0 text-ouro-600 mt-0.5" />
          <div className="text-sm text-ouro-800">
            <p className="font-semibold">Cobertura parcial</p>
            <p className="mt-1">Dados publicos de agenda disponiveis para {data.summary.ministers_covered} ministros desde jan/2024. Ausencia de registro nao significa ausencia de contato.</p>
          </div>
        </div>
      </div>
      <section className="grid gap-4 md:grid-cols-4">
        <StatCard icon={Calendar} label="Total eventos" value={String(data.summary.total_events)} help="Total de eventos registrados." />
        <StatCard icon={Shield} label="Advocacy privada" value={String(data.summary.total_private_advocacy)} help="Eventos private_advocacy." />
        <StatCard icon={FileText} label="Com ref. processual" value={String(data.summary.total_with_process_ref)} help="Eventos Track A." />
        <StatCard icon={Users} label="Ministros cobertos" value={String(data.summary.ministers_covered)} help="Ministros com dados." />
      </section>
      <section>
        <h2 className="mb-3 text-lg font-semibold text-slate-900">Ministros com cobertura</h2>
        <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white">
          <table className="w-full text-sm">
            <thead><tr className="border-b border-slate-100 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
              <th className="px-4 py-3">Ministro</th><th className="px-4 py-3 text-right">Eventos</th>
              <th className="px-4 py-3 text-right">Advocacy</th><th className="px-4 py-3 text-right">Track A</th>
              <th className="px-4 py-3 text-right">Meses</th><th className="px-4 py-3 text-right">Cobertura</th>
            </tr></thead>
            <tbody>{data.ministers.map((m) => (
              <tr key={m.minister_slug} className="border-b border-slate-50 hover:bg-slate-50 transition">
                <td className="px-4 py-3"><Link href={`/agenda/ministro/${m.minister_slug}`} className="font-medium text-verde-700 hover:underline">{m.minister_name}</Link></td>
                <td className="px-4 py-3 text-right tabular-nums">{m.total_events}</td>
                <td className="px-4 py-3 text-right tabular-nums">{m.private_advocacy_count}</td>
                <td className="px-4 py-3 text-right tabular-nums">{m.track_a_count}</td>
                <td className="px-4 py-3 text-right tabular-nums">{m.coverage_months}</td>
                <td className="px-4 py-3 text-right tabular-nums font-medium">{(m.avg_coverage_ratio * 100).toFixed(0)}%</td>
              </tr>
            ))}{data.ministers.length === 0 ? <tr><td colSpan={6} className="px-4 py-8 text-center text-slate-500">Nenhum ministro com dados.</td></tr> : null}</tbody>
          </table>
        </div>
      </section>
    </AppShell>
  );
}
