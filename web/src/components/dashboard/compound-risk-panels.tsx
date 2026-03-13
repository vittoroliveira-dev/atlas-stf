import Link from "next/link";
import { AlertTriangle, Flame, ShieldAlert } from "lucide-react";
import type {
  CompoundRiskHeatmapData,
} from "@/lib/compound-risk-data";

export function buildCompoundRiskPageHref(filters: {
  minister?: string;
  entityType?: "party" | "counsel";
  redFlagOnly?: boolean;
}) {
  const params = new URLSearchParams();
  if (filters.minister) {
    params.set("minister", filters.minister);
  }
  if (filters.entityType) {
    params.set("entity_type", filters.entityType);
  }
  if (filters.redFlagOnly) {
    params.set("red_flag_only", "true");
  }
  const query = params.toString();
  return query ? `/convergencia?${query}` : "/convergencia";
}

export function entityTypeLabel(entityType: "party" | "counsel") {
  return entityType === "party" ? "Parte" : "Advogado";
}

function signalLabel(signal: string) {
  switch (signal) {
    case "sanction":
      return "Sancao";
    case "donation":
      return "Doacao";
    case "corporate":
      return "Vinculo";
    case "affinity":
      return "Afinidade";
    case "alert":
      return "Alerta";
    default:
      return signal;
  }
}

function heatmapCellTone(signalCount: number | null) {
  if (signalCount == null) {
    return "border-slate-200/80 bg-slate-100/70 text-slate-300";
  }
  if (signalCount >= 4) {
    return "border-rose-300 bg-[linear-gradient(135deg,#7f1d1d,#be123c)] text-white shadow-[0_16px_36px_rgba(159,18,57,0.28)]";
  }
  if (signalCount === 3) {
    return "border-orange-300 bg-[linear-gradient(135deg,#9a3412,#f97316)] text-white shadow-[0_14px_30px_rgba(234,88,12,0.24)]";
  }
  if (signalCount === 2) {
    return "border-amber-300 bg-[linear-gradient(135deg,#92400e,#f59e0b)] text-white shadow-[0_14px_28px_rgba(217,119,6,0.22)]";
  }
  return "border-verde-200 bg-verde-50 text-verde-900";
}

export function CompoundRiskFilterPanel({
  minister,
  entityType,
  redFlagOnly,
  heatmapEntityCount,
  heatmapMinisterCount,
  pairsWithAlerts,
  displayLimit,
}: {
  minister?: string;
  entityType?: "party" | "counsel";
  redFlagOnly: boolean;
  heatmapEntityCount: number;
  heatmapMinisterCount: number;
  pairsWithAlerts: number;
  displayLimit: number;
}) {
  return (
    <section className="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
      <div className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Filtros</p>
            <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">
              Ajuste o recorte da convergencia
            </h2>
          </div>
          <Flame className="mt-1 h-5 w-5 text-rose-500" />
        </div>
        <p className="mt-3 text-sm leading-6 text-slate-600">
          Os filtros abaixo se aplicam ao ranking e ao heatmap do topo.
        </p>
        <div className="mt-5 flex flex-wrap gap-3">
          <Link
            href="/convergencia"
            className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
              !minister && !entityType && !redFlagOnly
                ? "border-slate-900 bg-slate-900 text-white"
                : "border-slate-200 bg-white text-slate-700 hover:border-slate-400"
            }`}
          >
            Todos os pares
          </Link>
          <Link
            href={buildCompoundRiskPageHref({
              minister,
              entityType: "party",
              redFlagOnly,
            })}
            className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
              entityType === "party"
                ? "border-verde-600 bg-verde-50 text-verde-700"
                : "border-slate-200 bg-white text-slate-700 hover:border-slate-400"
            }`}
          >
            Apenas partes
          </Link>
          <Link
            href={buildCompoundRiskPageHref({
              minister,
              entityType: "counsel",
              redFlagOnly,
            })}
            className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
              entityType === "counsel"
                ? "border-ouro-500 bg-ouro-50 text-ouro-700"
                : "border-slate-200 bg-white text-slate-700 hover:border-slate-400"
            }`}
          >
            Apenas advogados
          </Link>
          <Link
            href={buildCompoundRiskPageHref({
              minister,
              entityType,
              redFlagOnly: true,
            })}
            className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
              redFlagOnly
                ? "border-rose-500 bg-rose-50 text-rose-700"
                : "border-slate-200 bg-white text-slate-700 hover:border-slate-400"
            }`}
          >
            Apenas pontos criticos compostos
          </Link>
        </div>
      </div>

      <div className="rounded-[30px] border border-slate-200/80 bg-[linear-gradient(145deg,rgba(15,23,42,0.96),rgba(0,99,40,0.88),rgba(0,39,118,0.84))] p-6 text-white shadow-[0_24px_90px_rgba(15,23,42,0.26)]">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="font-mono text-xs uppercase tracking-[0.24em] text-white/70">Leitura rapida</p>
            <h2 className="mt-3 text-2xl font-semibold tracking-tight">
              Onde a convergencia esta mais concentrada
            </h2>
          </div>
          <ShieldAlert className="mt-1 h-5 w-5 text-rose-200" />
        </div>
        <div className="mt-5 grid gap-3 md:grid-cols-3">
          <div className="rounded-2xl border border-white/10 bg-white/10 p-4">
            <p className="text-sm text-white/70">Pares com alertas estatisticos</p>
            <p className="mt-2 text-3xl font-semibold">{pairsWithAlerts}</p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/10 p-4">
            <p className="text-sm text-white/70">Entidades no heatmap</p>
            <p className="mt-2 text-3xl font-semibold">{heatmapEntityCount}</p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/10 p-4">
            <p className="text-sm text-white/70">Ministros no heatmap</p>
            <p className="mt-2 text-3xl font-semibold">{heatmapMinisterCount}</p>
          </div>
        </div>
        <p className="mt-5 text-sm leading-6 text-white/80">
          O heatmap abaixo mostra ate {displayLimit} pares do ranking composto. Quanto mais quente a celula, maior a quantidade de sinais convergentes no mesmo par.
        </p>
      </div>
    </section>
  );
}

export function CompoundRiskHeatmapPanel({
  heatmap,
}: {
  heatmap: CompoundRiskHeatmapData;
}) {
  const heatmapCells = new Map(
    heatmap.cells.map((cell) => [
      `${cell.minister_name}::${cell.entity_type}::${cell.entity_id}`,
      cell,
    ]),
  );

  return (
    <section className="rounded-[32px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Heatmap</p>
          <h2 className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">
            Ministro x entidade
          </h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
            A matriz usa os pares mais altos do ranking atual para mostrar concentracoes de sinais compostos no mesmo relacionamento.
          </p>
        </div>
        <div className="flex flex-wrap gap-2 text-xs">
          {[
            { label: "1 sinal", tone: "bg-verde-50 text-verde-800 border-verde-200" },
            { label: "2 sinais", tone: "bg-amber-50 text-amber-800 border-amber-200" },
            { label: "3 sinais", tone: "bg-orange-50 text-orange-800 border-orange-200" },
            { label: "4+ sinais", tone: "bg-rose-50 text-rose-800 border-rose-200" },
          ].map((item) => (
            <span
              key={item.label}
              className={`inline-flex items-center rounded-full border px-3 py-1.5 font-medium ${item.tone}`}
            >
              {item.label}
            </span>
          ))}
        </div>
      </div>

      {heatmap.entities.length === 0 || heatmap.ministers.length === 0 ? (
        <div className="mt-6 flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
          <AlertTriangle className="h-5 w-5 text-amber-600" />
          <p className="text-sm text-amber-800">
            O heatmap nao tem celulas para o recorte atual.
          </p>
        </div>
      ) : (
        <div className="mt-6 overflow-x-auto">
          <div
            className="grid min-w-[920px] gap-3"
            style={{
              gridTemplateColumns: `220px repeat(${heatmap.entities.length}, minmax(120px, 1fr))`,
            }}
          >
            <div className="rounded-2xl border border-transparent bg-transparent p-3" />
            {heatmap.entities.map((entity) => (
              <div
                key={`${entity.entity_type}:${entity.entity_id}`}
                className="rounded-2xl border border-slate-200 bg-slate-50 p-3"
              >
                <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate-500">
                  {entityTypeLabel(entity.entity_type)}
                </p>
                <p className="mt-2 text-sm font-semibold leading-5 text-slate-950">
                  {entity.entity_name}
                </p>
              </div>
            ))}

            {heatmap.ministers.map((ministerName) => (
              <div
                key={ministerName}
                className="contents"
              >
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate-500">
                    Ministro
                  </p>
                  <p className="mt-2 text-sm font-semibold leading-5 text-slate-950">
                    {ministerName}
                  </p>
                </div>
                {heatmap.entities.map((entity) => {
                  const cell = heatmapCells.get(
                    `${ministerName}::${entity.entity_type}::${entity.entity_id}`,
                  );
                  return (
                    <div
                      key={`${ministerName}:${entity.entity_type}:${entity.entity_id}`}
                      className={`rounded-2xl border p-3 transition ${heatmapCellTone(cell?.signal_count ?? null)}`}
                    >
                      {cell ? (
                        <div className="flex h-full flex-col gap-2">
                          <div className="flex items-center justify-between gap-2">
                            <span className="text-xs font-medium uppercase tracking-[0.14em] opacity-80">
                              {cell.red_flag ? "Ponto critico" : "Sinal"}
                            </span>
                            <span className="text-2xl font-semibold">{cell.signal_count}</span>
                          </div>
                          <div className="flex flex-wrap gap-1">
                            {cell.signals.map((signal) => (
                              <span
                                key={`${cell.pair_id}:${signal}`}
                                className="rounded-full border border-white/20 bg-white/10 px-2 py-1 text-[10px] font-medium uppercase tracking-[0.14em]"
                              >
                                {signalLabel(signal)}
                              </span>
                            ))}
                          </div>
                          <p className="mt-auto text-xs leading-5 opacity-85">
                            Delta {cell.max_rate_delta == null
                              ? "—"
                              : `${cell.max_rate_delta > 0 ? "+" : ""}${(cell.max_rate_delta * 100).toFixed(1)}pp`}
                          </p>
                        </div>
                      ) : (
                        <div className="flex h-full min-h-[110px] items-center justify-center text-center text-xs font-medium uppercase tracking-[0.16em]">
                          Sem par no top 20
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
