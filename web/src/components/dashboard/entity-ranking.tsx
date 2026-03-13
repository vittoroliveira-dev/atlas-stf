import { relationHelperText, relationLabelHuman } from "@/lib/ui-copy";

function relationTone(level: "process_level" | "decision_derived" | "incerto") {
  if (level === "decision_derived") {
    return "bg-verde-100 text-verde-800 border-verde-200";
  }
  if (level === "incerto") {
    return "bg-amber-100 text-amber-800 border-amber-200";
  }
  return "bg-slate-100 text-slate-800 border-slate-200";
}

function relationLabel(level: "process_level" | "decision_derived" | "incerto") {
  return relationLabelHuman(level);
}

type EntitySummary = {
  id: string;
  name_raw: string;
  name_normalized: string;
  associated_event_count: number;
  distinct_process_count: number;
  relation_level: "process_level" | "decision_derived" | "incerto";
  role_labels: string[];
};

export function EntityRanking({
  title,
  subtitle,
  items,
  emptyMessage,
}: {
  title: string;
  subtitle: string;
  items: EntitySummary[];
  emptyMessage: string;
}) {
  return (
    <section className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
      <div className="max-w-3xl">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Relações observadas</p>
        <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">{title}</h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">{subtitle}</p>
      </div>

      {items.length === 0 ? (
        <div className="mt-6 rounded-[24px] border border-slate-200 bg-slate-50 px-5 py-4 text-sm leading-6 text-slate-600">
          {emptyMessage}
        </div>
      ) : (
        <div className="mt-6 grid gap-4">
          {items.map((item, index) => (
            <article
              key={`${item.id}:${index}`}
              className="rounded-[24px] border border-slate-200 bg-slate-50/70 p-4"
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="rounded-full bg-slate-950 px-3 py-1 font-mono text-xs text-white">
                      #{index + 1}
                    </span>
                    <span
                      className={`rounded-full border px-3 py-1 text-xs font-semibold ${relationTone(item.relation_level)}`}
                    >
                      {relationLabel(item.relation_level)}
                    </span>
                  </div>
                  <p className="mt-3 text-lg font-semibold text-slate-950">{item.name_raw}</p>
                  <p className="mt-2 text-sm text-slate-600">{relationHelperText(item.relation_level)}</p>
                </div>
                <div className="grid min-w-[220px] gap-3 sm:grid-cols-2">
                  <div className="rounded-[20px] bg-white px-4 py-3 text-right shadow-sm">
                    <p className="font-mono text-[11px] uppercase tracking-[0.2em] text-slate-500">Ocorrências</p>
                    <p className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">
                      {item.associated_event_count}
                    </p>
                  </div>
                  <div className="rounded-[20px] bg-white px-4 py-3 text-right shadow-sm">
                    <p className="font-mono text-[11px] uppercase tracking-[0.2em] text-slate-500">Casos</p>
                    <p className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">
                      {item.distinct_process_count}
                    </p>
                  </div>
                </div>
              </div>

              {item.role_labels.length > 0 ? (
                <div className="mt-4 flex flex-wrap gap-2">
                  {item.role_labels.map((label) => (
                    <span
                      key={`${item.id}:${label}`}
                      className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-medium text-slate-700"
                    >
                      {label}
                    </span>
                  ))}
                </div>
              ) : null}
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

export function CaseEntities({
  title,
  subtitle,
  counsels,
  parties,
}: {
  title: string;
  subtitle: string;
  counsels: EntitySummary[];
  parties: EntitySummary[];
}) {
  return (
    <section className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
      <div className="max-w-3xl">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Pessoas ligadas ao caso</p>
        <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">{title}</h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">{subtitle}</p>
      </div>

      <div className="mt-6 grid gap-5 xl:grid-cols-2">
        <div className="grid gap-4">
          <div>
            <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Advogados</p>
          </div>
          {counsels.length === 0 ? (
            <div className="rounded-[24px] border border-slate-200 bg-slate-50 px-5 py-4 text-sm leading-6 text-slate-600">
              Nenhum representante apareceu neste caso dentro do período selecionado.
            </div>
          ) : (
            counsels.map((item) => (
              <article key={item.id} className="rounded-[24px] border border-slate-200 bg-slate-50/70 p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <p className="text-base font-semibold text-slate-950">{item.name_raw}</p>
                    <p className="mt-1 text-sm text-slate-600">{relationHelperText(item.relation_level)}</p>
                  </div>
                  <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${relationTone(item.relation_level)}`}>
                    {relationLabel(item.relation_level)}
                  </span>
                </div>
                <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-700">
                  <span className="rounded-full border border-slate-200 bg-white px-3 py-1">casos: {item.distinct_process_count}</span>
                  {item.role_labels.map((label) => (
                    <span key={`${item.id}:${label}`} className="rounded-full border border-slate-200 bg-white px-3 py-1">{label}</span>
                  ))}
                </div>
              </article>
            ))
          )}
        </div>

        <div className="grid gap-4">
          <div>
            <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-slate-500">Partes</p>
          </div>
          {parties.length === 0 ? (
            <div className="rounded-[24px] border border-slate-200 bg-slate-50 px-5 py-4 text-sm leading-6 text-slate-600">
              Nenhuma parte apareceu neste caso dentro do período selecionado.
            </div>
          ) : (
            parties.map((item) => (
              <article key={item.id} className="rounded-[24px] border border-slate-200 bg-slate-50/70 p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <p className="text-base font-semibold text-slate-950">{item.name_raw}</p>
                    <p className="mt-1 text-sm text-slate-600">{relationHelperText(item.relation_level)}</p>
                  </div>
                  <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${relationTone(item.relation_level)}`}>
                    {relationLabel(item.relation_level)}
                  </span>
                </div>
                <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-700">
                  <span className="rounded-full border border-slate-200 bg-white px-3 py-1">casos: {item.distinct_process_count}</span>
                  {item.role_labels.map((label) => (
                    <span key={`${item.id}:${label}`} className="rounded-full border border-slate-200 bg-white px-3 py-1">{label}</span>
                  ))}
                </div>
              </article>
            ))
          )}
        </div>
      </div>
    </section>
  );
}
