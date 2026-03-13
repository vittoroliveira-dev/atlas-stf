import Link from "next/link";
import {
  Activity,
  CalendarClock,
  Clock3,
  GitCompareArrows,
  Landmark,
  TrendingUp,
} from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { StatCard } from "@/components/dashboard/stat-card";
import {
  BreakpointTable,
  EventTable,
  SeasonalityTable,
} from "@/components/dashboard/temporal-tables";
import { MinisterDetail } from "@/components/dashboard/temporal-minister-detail";
import { readSearchParam } from "@/lib/filter-context";
import {
  getTemporalAnalysisMinister,
  getTemporalAnalysisOverview,
} from "@/lib/temporal-analysis-data";

function SearchPanel({
  selectedMinister,
  suggestedMinisters,
}: {
  selectedMinister?: string;
  suggestedMinisters: string[];
}) {
  return (
    <section className="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
      <div className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">
          Recorte temporal
        </p>
        <h2 className="mt-3 text-2xl font-semibold text-slate-950">
          Buscar ministro
        </h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">
          Digite um nome para abrir a s&#233;rie mensal, o comparativo anual,
          eventos documentados e a linha do tempo de v&#237;nculos societ&#225;rios.
        </p>
        <form action="/temporal" method="get" className="mt-5 flex gap-3">
          <input
            type="text"
            name="minister"
            defaultValue={selectedMinister}
            placeholder="Ex.: TESTE"
            className="min-w-0 flex-1 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-verde-600 focus:ring-2 focus:ring-verde-100"
          />
          <button
            type="submit"
            className="rounded-2xl bg-slate-950 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800"
          >
            Abrir
          </button>
        </form>
      </div>
      <div className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">
          Atalhos
        </p>
        <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {suggestedMinisters.map((minister) => (
            <Link
              key={minister}
              href={`/temporal?minister=${encodeURIComponent(minister)}`}
              className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-700 transition hover:border-verde-300 hover:bg-verde-50 hover:text-verde-800"
            >
              {minister}
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

export default async function TemporalPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) ?? {};
  const selectedMinister = readSearchParam(params.minister);

  const overview = await getTemporalAnalysisOverview();
  const ministerDetail = selectedMinister
    ? await getTemporalAnalysisMinister(selectedMinister)
    : null;

  const breakpointCount = overview.breakpoints.length;
  const comparativeEvents = overview.events.filter(
    (row) => row.status === "comparativo",
  ).length;

  return (
    <AppShell
      currentPath="/temporal"
      eyebrow="Atlas STF &middot; an&#225;lise temporal"
      title="Mudan&#231;as de padr&#227;o ao longo do tempo"
      description="Esta p&#225;gina transforma snapshots em s&#233;ries descritivas: evolu&#231;&#227;o mensal, sazonalidade, compara&#231;&#227;o ano contra ano e leitura before/after de eventos documentados, sem tratar correla&#231;&#227;o como causalidade."
      heroState={
        breakpointCount > 0
          ? {
              status: "ok",
              title: `${breakpointCount} mudança(s) de padrão materializada(s)`,
              description:
                "O recorte temporal já permite apontar meses com ruptura descritiva e acompanhar a trajetória por ministro.",
            }
          : {
              status: "inconclusivo",
              title: "Ainda há pouca ruptura materializada no recorte atual",
              description:
                "A série temporal está carregada, mas nem todo ministro terá volume suficiente para comparação forte em todos os blocos.",
            }
      }
      guidance={{
        title: "Como ler esta trilha temporal",
        summary:
          "Use os blocos na ordem: rupturas mensais, sazonalidade, eventos documentados e drill-down por ministro.",
        bullets: [
          "Mudança de padrão é leitura descritiva do sinal mensal, não inferência causal.",
          "Eventos before/after ficam como inconclusivos quando a janela não tem volume comparável.",
          "Vínculos societários mostram apenas início observável do vínculo; não há dissolução inferida.",
        ],
      }}
    >
      <section className="grid gap-4 md:grid-cols-4">
        <StatCard
          icon={Clock3}
          label="Registros temporais"
          value={String(overview.summary.total_records)}
          help="Total de séries, comparativos, eventos e vínculos materializados."
        />
        <StatCard
          icon={Landmark}
          label="Ministros cobertos"
          value={String(overview.summary.ministers_covered)}
          help="Quantidade de ministros com algum recorte temporal persistido."
        />
        <StatCard
          icon={TrendingUp}
          label="Breakpoints"
          value={String(breakpointCount)}
          help="Meses marcados com mudança de padrão pela série mensal."
        />
        <StatCard
          icon={CalendarClock}
          label="Eventos comparativos"
          value={String(comparativeEvents)}
          help="Eventos documentados com janela antes/depois suficiente para comparação."
        />
      </section>

      <SearchPanel
        selectedMinister={selectedMinister}
        suggestedMinisters={overview.minister_summaries
          .slice(0, 6)
          .map((item) => item.rapporteur)}
      />

      <section className="grid gap-4 md:grid-cols-3">
        <StatCard
          icon={Activity}
          label="Janela rolling"
          value={`${overview.summary.rolling_window_months}m`}
          help="Média móvel usada para suavizar a série de taxa favorável."
        />
        <StatCard
          icon={GitCompareArrows}
          label="Janela de evento"
          value={`${overview.summary.event_window_days}d`}
          help="Dias antes e depois usados no comparativo de eventos externos."
        />
        <StatCard
          icon={TrendingUp}
          label="Tipos materializados"
          value={String(Object.keys(overview.summary.counts_by_kind).length)}
          help="Blocos analíticos disponíveis no serving temporal."
        />
      </section>

      <BreakpointTable rows={overview.breakpoints} />
      <SeasonalityTable rows={overview.seasonality} />
      <EventTable rows={overview.events} />

      {ministerDetail ? (
        <MinisterDetail
          minister={ministerDetail.rapporteur ?? ministerDetail.minister}
          monthly={ministerDetail.monthly}
          yoy={ministerDetail.yoy}
          events={ministerDetail.events}
          corporateLinks={ministerDetail.corporate_links}
        />
      ) : null}
    </AppShell>
  );
}
