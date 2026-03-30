import { AlertTriangle, ArrowDown, ArrowUp } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import {
  CardGrid,
  ExpandableCard,
  RedFlagPill,
} from "@/components/dashboard/cross-ref-card";
import { emptyStateMessage, velocityFlagLabel, velocityFlagColor } from "@/lib/ui-copy";
import { getDecisionVelocityPageData, getDecisionVelocityFlags } from "@/lib/decision-velocity-data";
import Link from "next/link";
import { readSearchParam } from "@/lib/filter-context";

export default async function VelocidadePage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = (await searchParams) ?? {};
  const minister = readSearchParam(query.minister);
  const flagOnly = readSearchParam(query.flag_only) === "true";
  const velocityFlag = readSearchParam(query.velocity_flag);
  const page = Number(readSearchParam(query.page) ?? "1");

  const filterQuery: Record<string, string | undefined> = {
    minister: minister ?? undefined,
    flag_only: flagOnly ? "true" : undefined,
    velocity_flag: velocityFlag ?? undefined,
  };

  const [data, flags] = await Promise.all([
    getDecisionVelocityPageData({
      page,
      minister: minister ?? undefined,
      flagOnly,
      velocityFlag: velocityFlag ?? undefined,
    }),
    getDecisionVelocityFlags(),
  ]);

  const queueJumpCount = flags.items.filter((i) => i.velocity_flag === "queue_jump").length;
  const stalledCount = flags.items.filter((i) => i.velocity_flag === "stalled").length;

  return (
    <AppShell
      currentPath="/velocidade"
      eyebrow="Atlas STF · velocidade decisória"
      title="Velocidade de decisão"
      description="Processos com tempo de tramitação anômalo em relação ao grupo comparável."
      guidance={{
        title: "Como interpretar esta tela",
        summary:
          "Compara o tempo entre autuação e decisão de cada processo com processos similares (mesma classe, tema e ano).",
        bullets: [
          "Fura-fila: processo decidido mais rápido que 95% dos similares (abaixo do percentil 5).",
          "Parado: processo decidido mais devagar que 95% dos similares (acima do percentil 95).",
          "O z-score indica quantos desvios o processo está da mediana do grupo.",
          "Anomalia de velocidade não implica irregularidade -- pode refletir urgência legítima ou complexidade processual.",
        ],
      }}
    >
      {/* KPI cards */}
      <section className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Decisões analisadas</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{data.total.toLocaleString("pt-BR")}</p>
        </div>
        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 shadow-sm">
          <p className="text-sm text-red-600">Fura-fila</p>
          <p className="mt-1 text-3xl font-semibold text-red-700">{queueJumpCount.toLocaleString("pt-BR")}</p>
        </div>
        <div className="rounded-2xl border border-amber-200 bg-amber-50 p-5 shadow-sm">
          <p className="text-sm text-amber-600">Parado</p>
          <p className="mt-1 text-3xl font-semibold text-amber-700">{stalledCount.toLocaleString("pt-BR")}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Total anomalias</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{flags.total.toLocaleString("pt-BR")}</p>
        </div>
      </section>

      {/* Filters */}
      <section className="flex flex-wrap gap-3">
        <Link
          href="/velocidade"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            !flagOnly && !velocityFlag
              ? "border-verde-600 bg-verde-50 text-verde-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Todos
        </Link>
        <Link
          href="/velocidade?flag_only=true"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            flagOnly && !velocityFlag
              ? "border-red-500 bg-red-50 text-red-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Apenas anomalias
        </Link>
        <Link
          href="/velocidade?velocity_flag=queue_jump"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            velocityFlag === "queue_jump"
              ? "border-red-500 bg-red-50 text-red-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Fura-fila
        </Link>
        <Link
          href="/velocidade?velocity_flag=stalled"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            velocityFlag === "stalled"
              ? "border-amber-500 bg-amber-50 text-amber-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Parado
        </Link>
      </section>

      <PaginationControls
        pathname="/velocidade"
        query={filterQuery}
        page={data.page}
        pageSize={data.pageSize}
        total={data.total}
        orderingLabel="decisões"
        pageSizeOptions={[12, 24, 48]}
      />

      {data.items.length === 0 ? (
        <section className="flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
          <AlertTriangle className="h-5 w-5 text-amber-600" />
          <p className="text-sm text-amber-800">{emptyStateMessage("velocity")}</p>
        </section>
      ) : (
        <section>
          <CardGrid columns={1}>
            {data.items.map((v) => (
              <ExpandableCard
                key={v.velocity_id}
                summary={
                  <div className="flex flex-1 flex-wrap items-center gap-3">
                    <Link
                      href={`/caso/${encodeURIComponent(v.process_id)}`}
                      className="font-medium text-verde-700 hover:underline"
                    >
                      {v.process_id}
                    </Link>
                    <span className="text-sm text-slate-500">{v.current_rapporteur ?? "---"}</span>
                    <span className="text-2xl font-semibold text-slate-900">{v.days_to_decision ?? "---"}d</span>
                    <span className="text-sm text-slate-400">
                      mediana {v.median_days != null ? `${Math.round(v.median_days)}d` : "---"}
                    </span>
                    {v.velocity_flag && (
                      <span className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-xs font-medium ${velocityFlagColor(v.velocity_flag)}`}>
                        {v.velocity_flag === "queue_jump" ? <ArrowDown className="h-3 w-3" /> : <ArrowUp className="h-3 w-3" />}
                        {velocityFlagLabel(v.velocity_flag)}
                      </span>
                    )}
                    <RedFlagPill show={v.velocity_flag === "queue_jump"} />
                  </div>
                }
              >
                <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Autuação</p>
                    <p className="mt-1 text-sm text-slate-900">{v.filing_date ?? "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Decisão</p>
                    <p className="mt-1 text-sm text-slate-900">{v.decision_date ?? "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Classe / Tema</p>
                    <p className="mt-1 text-sm text-slate-900">{v.process_class ?? "---"} / {v.thematic_key ?? "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Z-score</p>
                    <p className="mt-1 text-sm text-slate-900">{v.velocity_z_score != null ? v.velocity_z_score.toFixed(2) : "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">P5 do grupo</p>
                    <p className="mt-1 text-sm text-slate-900">{v.p5_days != null ? `${Math.round(v.p5_days)}d` : "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">P10 do grupo</p>
                    <p className="mt-1 text-sm text-slate-900">{v.p10_days != null ? `${Math.round(v.p10_days)}d` : "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Mediana do grupo</p>
                    <p className="mt-1 text-sm text-slate-900">{v.median_days != null ? `${Math.round(v.median_days)}d` : "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">P90 do grupo</p>
                    <p className="mt-1 text-sm text-slate-900">{v.p90_days != null ? `${Math.round(v.p90_days)}d` : "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">P95 do grupo</p>
                    <p className="mt-1 text-sm text-slate-900">{v.p95_days != null ? `${Math.round(v.p95_days)}d` : "---"}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Tamanho do grupo</p>
                    <p className="mt-1 text-sm text-slate-900">{v.group_size ?? "---"}</p>
                  </div>
                </div>
              </ExpandableCard>
            ))}
          </CardGrid>
        </section>
      )}
    </AppShell>
  );
}
