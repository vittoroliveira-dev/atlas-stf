/**
 * Skeleton institucional genérico para loading.tsx.
 * Usa a mesma gramática visual do AppShell (hero escuro + cards)
 * para que a transição loading → página real seja mínima.
 *
 * Server Component — zero JS no cliente.
 */
export function PageSkeleton() {
  return (
    <main
      id="main-content"
      aria-busy="true"
      aria-label="Carregando página do painel Atlas STF"
      className="mx-auto flex min-h-screen w-full max-w-[1600px] scroll-mt-20 flex-col gap-8 px-4 py-6 sm:px-6 lg:px-8"
    >
      {/* Hero skeleton — mesma silhueta do AppShell hero */}
      <section
        aria-hidden="true"
        className="overflow-hidden rounded-hero border border-marinho-950 bg-marinho-900 p-6 text-white shadow-elevation-hero sm:p-8"
      >
        <div className="max-w-4xl space-y-6">
          <div className="h-7 w-44 animate-pulse rounded-full bg-white/10" />
          <div className="space-y-3">
            <div className="h-10 w-3/4 animate-pulse rounded bg-white/15 sm:h-12" />
            <div className="h-10 w-2/3 animate-pulse rounded bg-white/10 sm:h-12" />
          </div>
          <div className="space-y-2">
            <div className="h-4 w-5/6 animate-pulse rounded bg-white/10" />
            <div className="h-4 w-3/5 animate-pulse rounded bg-white/10" />
          </div>
        </div>
      </section>

      {/* Filter + leitura skeleton */}
      <section aria-hidden="true" className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <div className="rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1">
          <div className="space-y-3">
            <div className="h-3 w-32 animate-pulse rounded bg-slate-200" />
            <div className="h-4 w-2/3 animate-pulse rounded bg-slate-100" />
          </div>
          <div className="mt-5 grid gap-4 md:grid-cols-3">
            <div className="h-12 animate-pulse rounded-2xl bg-slate-100" />
            <div className="h-12 animate-pulse rounded-2xl bg-slate-100" />
            <div className="h-12 animate-pulse rounded-2xl bg-slate-100" />
          </div>
        </div>
        <div className="rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1">
          <div className="space-y-3">
            <div className="h-3 w-28 animate-pulse rounded bg-slate-200" />
            <div className="h-6 w-4/5 animate-pulse rounded bg-slate-100" />
            <div className="h-4 w-3/4 animate-pulse rounded bg-slate-100" />
            <div className="h-4 w-2/3 animate-pulse rounded bg-slate-100" />
          </div>
        </div>
      </section>

      {/* Stat cards skeleton */}
      <section aria-hidden="true" className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <div
            key={index}
            className="rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1"
          >
            <div className="flex items-center justify-between gap-4">
              <div className="space-y-3">
                <div className="h-3 w-24 animate-pulse rounded bg-slate-200" />
                <div className="h-8 w-16 animate-pulse rounded bg-slate-100" />
              </div>
              <div className="h-12 w-12 animate-pulse rounded-2xl bg-verde-100" />
            </div>
            <div className="mt-4 h-3 w-3/4 animate-pulse rounded bg-slate-100" />
          </div>
        ))}
      </section>

      {/* Content block skeleton — proxy para tabelas/cards principais */}
      <section aria-hidden="true" className="rounded-card border border-slate-200 bg-white p-6 shadow-elevation-1">
        <div className="space-y-3">
          <div className="h-3 w-36 animate-pulse rounded bg-slate-200" />
          <div className="h-7 w-1/2 animate-pulse rounded bg-slate-100" />
          <div className="h-4 w-3/4 animate-pulse rounded bg-slate-100" />
        </div>
        <div className="mt-6 space-y-3">
          {Array.from({ length: 4 }).map((_, index) => (
            <div key={index} className="h-14 animate-pulse rounded-inset bg-slate-50" />
          ))}
        </div>
      </section>

      <p className="sr-only" role="status">
        Carregando dados do painel. O conteúdo aparece automaticamente assim que a API responde.
      </p>
    </main>
  );
}
