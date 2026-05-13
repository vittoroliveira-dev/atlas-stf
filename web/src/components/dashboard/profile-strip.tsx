export function ProfileStrip({
  profiles,
}: {
  profiles: Array<{
    minister: string;
    period: string;
    collegiate: string;
    eventCount: number;
    historicalAverage: number;
    linkedAlertCount: number;
    processClasses: string[];
    themes: string[];
  }>;
}) {
  return (
    <section className="grid gap-4 xl:grid-cols-3">
      {profiles.map((profile) => (
        <article
          key={`${profile.minister}-${profile.period}-${profile.collegiate}`}
          className="rounded-card border border-slate-200 bg-gradient-to-br from-white via-slate-50 to-verde-50 p-5 shadow-elevation-1"
        >
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Resumo de comparação</p>
              <h3 className="mt-2 text-xl font-semibold tracking-tight text-slate-950">{profile.minister}</h3>
              <p className="mt-1 text-sm text-slate-600">{profile.period} · {profile.collegiate}</p>
            </div>
            <div className="rounded-2xl bg-slate-950 px-3 py-1 text-sm font-semibold text-white">{profile.eventCount} ocorrências</div>
          </div>

          <dl className="mt-5 grid grid-cols-2 gap-3 text-sm">
            <div className="rounded-2xl bg-white p-3">
              <dt className="text-slate-500">Média histórica</dt>
              <dd className="mt-1 text-lg font-semibold text-slate-950">{profile.historicalAverage.toFixed(3)}</dd>
            </div>
            <div className="rounded-2xl bg-white p-3">
              <dt className="text-slate-500">Pontos de atenção</dt>
              <dd className="mt-1 text-lg font-semibold text-slate-950">{profile.linkedAlertCount}</dd>
            </div>
          </dl>

          <div className="mt-4 grid gap-3 text-sm text-slate-600 md:grid-cols-2">
            <div>
              <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Tipos de ação</p>
              <p className="mt-2 leading-6">{profile.processClasses.join(" · ") || "Sem tipo de ação predominante neste período"}</p>
            </div>
            <div>
              <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Temas</p>
              <p className="mt-2 leading-6">{profile.themes.join(" · ") || "Sem tema predominante neste período"}</p>
            </div>
          </div>
        </article>
      ))}
    </section>
  );
}
