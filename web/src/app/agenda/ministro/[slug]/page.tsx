import { AlertTriangle, Calendar, Clock, FileText, TrendingUp } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { StatCard } from "@/components/dashboard/stat-card";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import { getMinisterDetailData } from "@/lib/agenda-data";
import { readSearchParam } from "@/lib/filter-context";
import { notFound } from "next/navigation";

function catColor(c: string) {
  if (c === "institutional_core") return "bg-blue-100 text-blue-700 border-blue-200";
  if (c === "institutional_external_actor") return "bg-verde-50 text-verde-700 border-verde-200";
  if (c === "private_advocacy") return "bg-red-50 text-red-700 border-red-200";
  return "bg-slate-100 text-slate-600 border-slate-200";
}
function catLabel(c: string) {
  if (c === "institutional_core") return "Institucional";
  if (c === "institutional_external_actor") return "Ator externo";
  if (c === "private_advocacy") return "Advocacy privada";
  return "Indefinido";
}

export default async function AgendaMinistroPage({ params, searchParams }: {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { slug } = await params;
  const query = (await searchParams) ?? {};
  const page = Number(readSearchParam(query.page) ?? "1");
  const data = await getMinisterDetailData(slug, { page });
  if (!data) notFound();

  return (
    <AppShell currentPath="/agenda" eyebrow={`Atlas STF · agenda · ${data.ministerName}`} title={data.ministerName}
      description="Timeline de eventos de agenda publica e exposicoes temporais."
      guidance={{ title: "Nota metodologica", summary: "Dados parciais. Ausencia de registro nao significa ausencia de contato.",
        bullets: ["Cobertura varia por ministro e periodo.", "Baselines condicionados intra-ministro."] }}>
      <div className="rounded-xl border border-ouro-200 bg-ouro-50 p-3 text-sm text-ouro-800">
        <div className="flex items-start gap-2"><AlertTriangle className="h-4 w-4 shrink-0 text-ouro-600 mt-0.5" /><p>Cobertura parcial. Dados servem para priorizacao investigativa.</p></div>
      </div>
      <section className="grid gap-4 md:grid-cols-4">
        <StatCard icon={Calendar} label="Eventos" value={String(data.eventTotal)} help="Total de eventos." />
        <StatCard icon={FileText} label="Track A" value={String(data.events.filter(e => e.relevance_track === "A").length)} help="Com ref. processual." />
        <StatCard icon={TrendingUp} label="Advocacy" value={String(data.events.filter(e => e.event_category === "private_advocacy").length)} help="Advocacy privada." />
        <StatCard icon={Clock} label="Exposicoes" value={String(data.exposures.length)} help="Coincidencias temporais." />
      </section>
      {data.coverages.length > 0 ? <section><h2 className="mb-3 text-lg font-semibold text-slate-900">Cobertura mensal</h2>
        <div className="grid gap-2 sm:grid-cols-3 lg:grid-cols-4">{data.coverages.map(c => (
          <div key={c.coverage_id} className="rounded-lg border border-slate-200 bg-white p-3 text-sm">
            <div className="flex items-center justify-between"><span className="font-medium text-slate-700">{c.year}-{String(c.month).padStart(2, "0")}</span>
              <span className={`text-xs font-medium ${c.comparability_tier === "high" ? "text-verde-700" : c.comparability_tier === "medium" ? "text-ouro-700" : "text-slate-500"}`}>{(c.coverage_ratio * 100).toFixed(0)}%</span></div>
            <div className="mt-1 text-xs text-slate-500">{c.event_count} eventos{c.court_recess_flag ? " · recesso" : ""}{c.publication_gap_flag ? " · gap" : ""}</div>
          </div>
        ))}</div></section> : null}
      <section><h2 className="mb-3 text-lg font-semibold text-slate-900">Eventos</h2>
        <PaginationControls pathname={`/agenda/ministro/${slug}`} query={{}} page={page} pageSize={20} total={data.eventTotal} orderingLabel="data" />
        <div className="space-y-2 mt-3">{data.events.map(ev => (
          <div key={ev.event_id} className="rounded-xl border border-slate-200 bg-white p-4 hover:shadow-sm transition">
            <div className="flex items-start justify-between gap-2"><div className="flex-1 min-w-0">
              <div className="flex items-center gap-2"><span className="text-xs text-slate-500 tabular-nums">{ev.event_date}</span>
                <span className={`rounded-full border px-2 py-0.5 text-[10px] font-medium ${catColor(ev.event_category)}`}>{catLabel(ev.event_category)}</span></div>
              <h3 className="mt-1 text-sm font-medium text-slate-900">{ev.event_title}</h3>
              {ev.event_description ? <p className="mt-1 text-xs text-slate-600 line-clamp-2">{ev.event_description}</p> : null}
            </div>{ev.has_process_ref ? <span className="shrink-0 rounded-full bg-verde-50 border border-verde-200 px-2 py-0.5 text-[10px] font-medium text-verde-700">Track A</span> : null}</div>
            {ev.process_refs.length > 0 ? <div className="mt-2 flex flex-wrap gap-1">{ev.process_refs.map((r, i) => (
              <span key={i} className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-mono text-slate-600">{r.class} {r.number}</span>
            ))}</div> : null}
          </div>
        ))}{data.events.length === 0 ? <p className="text-center text-sm text-slate-500 py-8">Nenhum evento.</p> : null}</div>
      </section>
      {data.exposures.length > 0 ? <section><h2 className="mb-3 text-lg font-semibold text-slate-900">Exposicoes temporais</h2>
        <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white"><table className="w-full text-sm">
          <thead><tr className="border-b border-slate-100 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
            <th className="px-4 py-3">Data</th><th className="px-4 py-3">Processo</th><th className="px-4 py-3">Tipo</th>
            <th className="px-4 py-3 text-right">Dias</th><th className="px-4 py-3">Janela</th><th className="px-4 py-3">Prioridade</th>
          </tr></thead><tbody>{data.exposures.map(exp => (
            <tr key={exp.exposure_id} className="border-b border-slate-50 hover:bg-slate-50 transition">
              <td className="px-4 py-3 tabular-nums">{exp.agenda_date}</td>
              <td className="px-4 py-3 font-mono text-xs">{exp.process_class ?? "-"}</td>
              <td className="px-4 py-3">{exp.decision_type ?? "-"}</td>
              <td className="px-4 py-3 text-right tabular-nums">{exp.days_between ?? "-"}</td>
              <td className="px-4 py-3">{exp.window}</td>
              <td className="px-4 py-3"><span className={`rounded-full border px-2 py-0.5 text-[10px] font-medium ${exp.priority_tier === "high" ? "bg-red-50 text-red-700 border-red-200" : exp.priority_tier === "medium" ? "bg-ouro-50 text-ouro-700 border-ouro-200" : "bg-slate-100 text-slate-600 border-slate-200"}`}>{exp.priority_tier === "high" ? "Alta" : exp.priority_tier === "medium" ? "Media" : "Baixa"}</span></td>
            </tr>
          ))}</tbody></table></div></section> : null}
    </AppShell>
  );
}
