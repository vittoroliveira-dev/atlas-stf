import Link from "next/link";
import { ArrowRight } from "lucide-react";
import type { CaseRow, EntitySummary, MinisterCorrelation } from "@/lib/dashboard-data";
import { relationHelperText, relationLabelHuman } from "@/lib/ui-copy";

function relationTone(level: "process_level" | "decision_derived" | "incerto") {
  if (level === "decision_derived") return "bg-verde-100 text-verde-800 border-verde-200";
  if (level === "incerto") return "bg-amber-100 text-amber-800 border-amber-200";
  return "bg-slate-100 text-slate-800 border-slate-200";
}

function relationLabel(level: "process_level" | "decision_derived" | "incerto") {
  return relationLabelHuman(level);
}

export function EntityIndexGrid({
  title,
  subtitle,
  items,
  detailBasePath,
  contextQuery,
  emptyMessage,
}: {
  title: string;
  subtitle: string;
  items: EntitySummary[];
  detailBasePath: string;
  contextQuery: string;
  emptyMessage: string;
}) {
  return (
    <section className="rounded-card border border-slate-200 bg-white p-6 shadow-elevation-1">
      <div className="max-w-3xl">
        <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Lista de nomes relacionados</p>
        <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">{title}</h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">{subtitle}</p>
      </div>

      {items.length === 0 ? (
        <div className="mt-6 rounded-inset border border-slate-200 bg-slate-50 px-5 py-4 text-sm leading-6 text-slate-600">
          {emptyMessage}
        </div>
      ) : (
        <div className="mt-6 grid gap-4 md:grid-cols-2">
          {items.map((item) => (
            <article key={item.id} className="rounded-inset border border-slate-200 bg-slate-50/70 p-5">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${relationTone(item.relation_level)}`}>
                      {relationLabel(item.relation_level)}
                    </span>
                  </div>
                  <p className="mt-3 text-lg font-semibold text-slate-950">{item.name_raw}</p>
                  <p className="mt-2 text-sm text-slate-600">
                    {relationHelperText(item.relation_level)}
                  </p>
                </div>
                <div className="grid min-w-[220px] gap-3 sm:grid-cols-2">
                  <div className="rounded-inset bg-white px-4 py-3 text-right shadow-sm">
                    <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Ocorrências</p>
                    <p className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">{item.associated_event_count}</p>
                  </div>
                  <div className="rounded-inset bg-white px-4 py-3 text-right shadow-sm">
                    <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Casos</p>
                    <p className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">{item.distinct_process_count}</p>
                  </div>
                </div>
              </div>

              {item.role_labels.length > 0 ? (
                <div className="mt-4 flex flex-wrap gap-2">
                  {item.role_labels.map((label) => (
                    <span key={`${item.id}:${label}`} className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-medium text-slate-700">
                      {label}
                    </span>
                  ))}
                </div>
              ) : null}

              <div className="mt-5 flex justify-end">
                <Link
                  href={`${detailBasePath}/${encodeURIComponent(item.id)}${contextQuery}`}
                  className="inline-flex h-11 items-center justify-center gap-2 rounded-2xl bg-slate-950 px-4 text-sm font-semibold text-white transition hover:bg-slate-800"
                >
                  Ver detalhes
                  <ArrowRight className="h-4 w-4" aria-hidden="true" focusable="false" />
                </Link>
              </div>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

export function EntityDetailPanels({
  entity,
  ministers,
  cases,
  entityLabel,
  contextQuery,
}: {
  entity: EntitySummary;
  ministers: MinisterCorrelation[];
  cases: CaseRow[];
  entityLabel: string;
  contextQuery: string;
}) {
  return (
    <>
      <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1">
          <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Ocorrências</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">{entity.associated_event_count}</p>
        </div>
        <div className="rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1">
          <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Casos</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">{entity.distinct_process_count}</p>
        </div>
        <div className="rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1">
          <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Tipo de ligação</p>
          <p className="mt-2 text-lg font-semibold tracking-tight text-slate-950">{relationLabel(entity.relation_level)}</p>
        </div>
        <div className="rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1">
          <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Papéis</p>
          <p className="mt-2 text-sm font-semibold text-slate-950">{entity.role_labels.join(" · ") || "Sem papel identificado neste período."}</p>
        </div>
      </section>

      <section className="grid gap-5 xl:grid-cols-[0.95fr_1.05fr]">
        <article className="rounded-card border border-slate-200 bg-white p-6 shadow-elevation-1">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Quem aparece junto</p>
            <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">
              Ministros mais ligados a est{entityLabel === "parte" ? "a" : "e"} {entityLabel}
            </h2>
            <p className="mt-2 text-sm leading-6 text-slate-600">
              Esta lista mostra em quais contextos est{entityLabel === "parte" ? "a" : "e"} {entityLabel} aparece com mais frequência.
            </p>
          </div>

          {ministers.length === 0 ? (
            <div className="mt-6 rounded-inset border border-slate-200 bg-slate-50 px-5 py-4 text-sm leading-6 text-slate-600">
              Não encontramos ministros ligados a este nome dentro do período selecionado.
            </div>
          ) : (
            <div className="mt-6 grid gap-4">
              {ministers.map((item) => (
                <article key={item.minister} className="rounded-inset border border-slate-200 bg-slate-50/70 p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <p className="text-base font-semibold text-slate-950">{item.minister}</p>
                      <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-700">
                        <span className="rounded-full border border-slate-200 bg-white px-3 py-1">ocorrências: {item.associated_event_count}</span>
                        <span className="rounded-full border border-slate-200 bg-white px-3 py-1">casos: {item.distinct_process_count}</span>
                      </div>
                    </div>
                    <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${relationTone(item.relation_level)}`}>
                      {relationLabel(item.relation_level)}
                    </span>
                  </div>
                  {item.role_labels.length > 0 ? (
                    <div className="mt-3 flex flex-wrap gap-2">
                      {item.role_labels.map((label) => (
                        <span key={`${item.minister}:${label}`} className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-medium text-slate-700">
                          {label}
                        </span>
                      ))}
                    </div>
                  ) : null}
                </article>
              ))}
            </div>
          )}
        </article>

        <article className="rounded-card border border-slate-200 bg-white p-6 shadow-elevation-1">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Casos relacionados</p>
            <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">
              Casos em que est{entityLabel === "parte" ? "a" : "e"} {entityLabel} aparece
            </h2>
            <p className="mt-2 text-sm leading-6 text-slate-600">
              Estes são os casos ligados a est{entityLabel === "parte" ? "a" : "e"} {entityLabel} dentro do filtro atual.
            </p>
          </div>

          {cases.length === 0 ? (
            <div className="mt-6 rounded-inset border border-slate-200 bg-slate-50 px-5 py-4 text-sm leading-6 text-slate-600">
              Não há casos ligados a est{entityLabel === "parte" ? "a" : "e"} {entityLabel} dentro do filtro atual.
            </div>
          ) : (
            <div className="mt-6 grid gap-4">
              {cases.map((item) => (
                <article key={item.decisionEventId} className="rounded-inset border border-slate-200 bg-slate-50/70 p-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <p className="text-base font-semibold text-slate-950">{item.processNumber}</p>
                    <p className="mt-1 text-sm text-slate-600">{item.processClass} · {item.decisionDate} · {item.judgingBody}</p>
                  </div>
                    <Link
                      href={`/caso/${encodeURIComponent(item.decisionEventId)}${contextQuery}`}
                      className="inline-flex h-10 items-center justify-center gap-2 rounded-2xl bg-verde-700 px-4 text-sm font-semibold text-white transition hover:bg-verde-800"
                    >
                      Ver caso
                      <ArrowRight className="h-4 w-4" aria-hidden="true" focusable="false" />
                    </Link>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-700">
                    <span className="rounded-full border border-slate-200 bg-white px-3 py-1">tipo: {item.decisionType}</span>
                    <span className="rounded-full border border-slate-200 bg-white px-3 py-1">andamento: {item.decisionProgress}</span>
                  </div>
                </article>
              ))}
            </div>
          )}
        </article>
      </section>
    </>
  );
}
