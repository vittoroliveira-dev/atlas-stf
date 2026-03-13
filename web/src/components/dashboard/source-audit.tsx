import { sourceDescriptionHuman, sourceLabelHuman } from "@/lib/ui-copy";

export function SourceAudit({
  sourceFiles,
}: {
  sourceFiles: Array<{ label: string; path: string; checksum: string; updatedAt: string }>;
}) {
  return (
    <section className="rounded-[30px] border border-slate-200/80 bg-white/90 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
      <div className="max-w-3xl">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">
          Como este resultado foi montado
        </p>
        <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">
          Bases consultadas nesta tela
        </h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">
          Aqui você vê, em linguagem simples, quais bases ajudaram a compor esta página e quando
          elas foram atualizadas pela última vez.
        </p>
      </div>

      <div className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {sourceFiles.map((file) => (
          <article
            key={`${file.label}:${file.updatedAt}`}
            className="rounded-[24px] border border-slate-200 bg-white p-5"
          >
            <p className="text-base font-semibold text-slate-950">
              {sourceLabelHuman(file.label)}
            </p>
            <p className="mt-2 text-sm leading-6 text-slate-600">
              {sourceDescriptionHuman(file.label)}
            </p>
            <dl className="mt-4 grid gap-3 text-sm">
              <div>
                <dt className="text-slate-500">Última atualização</dt>
                <dd className="mt-1 font-medium text-slate-900">
                  {new Date(file.updatedAt).toLocaleString("pt-BR")}
                </dd>
              </div>
              <div>
                <dt className="text-slate-500">Conferência</dt>
                <dd className="mt-1 text-slate-700">Detalhes completos disponíveis na trilha técnica do projeto.</dd>
              </div>
            </dl>
          </article>
        ))}
      </div>
    </section>
  );
}
