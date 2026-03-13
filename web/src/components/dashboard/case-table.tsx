import Link from "next/link";
import { getSafeExternalHref } from "@/lib/safe-external-url";

export function CaseTable({
  rows,
  contextQuery,
}: {
  rows: Array<{
    processId: string;
    processNumber: string;
    processClass: string;
    decisionEventId: string;
    decisionDate: string;
    decisionType: string;
    decisionProgress: string;
    judgingBody: string;
    collegiateLabel: string;
    branchOfLaw: string;
    firstSubject: string;
    inteiroTeorUrl: string | null;
    docCountLabel: string;
    acordaoLabel: string;
    monocraticDecisionLabel: string;
    originDescription: string;
    decisionNoteSnippet: string;
  }>;
  contextQuery?: string;
}) {
  return (
    <section className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
      <div className="max-w-3xl">
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Casos para explorar</p>
        <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">
          Casos ligados ao período selecionado
        </h2>
        <p className="mt-2 text-sm leading-6 text-slate-600">
          Use esta área para abrir o caso, entender a decisão e verificar se há documentação disponível para leitura.
        </p>
      </div>

      <div className="mt-6 grid gap-4 lg:hidden">
        {rows.map((row) => (
          <article key={row.decisionEventId} className="rounded-[24px] border border-slate-200 bg-slate-50/70 p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-base font-semibold text-slate-950">{row.processNumber}</p>
                <p className="mt-1 text-sm text-slate-600">
                  {row.decisionDate} · {row.decisionType}
                </p>
              </div>
              <Link
                href={`/caso/${encodeURIComponent(row.decisionEventId)}${contextQuery ?? ""}`}
                className="inline-flex h-10 items-center justify-center rounded-2xl bg-slate-950 px-4 text-sm font-semibold text-white transition hover:bg-slate-800"
              >
                Ver detalhes
              </Link>
            </div>
            <dl className="mt-4 grid gap-3 text-sm text-slate-700">
              <div>
                <dt className="text-slate-500">Tipo de ação</dt>
                <dd className="mt-1">{row.processClass}</dd>
              </div>
              <div>
                <dt className="text-slate-500">Onde foi decidido</dt>
                <dd className="mt-1">{row.judgingBody}</dd>
              </div>
              <div>
                <dt className="text-slate-500">Tema principal</dt>
                <dd className="mt-1">{row.firstSubject !== "INCERTO" ? row.firstSubject : row.branchOfLaw}</dd>
              </div>
              <div>
                <dt className="text-slate-500">Documentos</dt>
                <dd className="mt-1">{row.docCountLabel} disponíveis</dd>
              </div>
            </dl>
          </article>
        ))}
      </div>

      <div className="mt-6 hidden overflow-x-auto rounded-[24px] border border-slate-200 lg:block">
        <table className="min-w-[1320px] divide-y divide-slate-200 text-left text-sm">
          <thead className="bg-slate-50 text-slate-500">
            <tr>
              <th className="px-4 py-3 font-medium">Caso</th>
              <th className="px-4 py-3 font-medium">Detalhes</th>
              <th className="px-4 py-3 font-medium">Data</th>
              <th className="px-4 py-3 font-medium">Decisão</th>
              <th className="px-4 py-3 font-medium">Situação</th>
              <th className="px-4 py-3 font-medium">Onde foi decidido</th>
              <th className="px-4 py-3 font-medium">Tipo / tema</th>
              <th className="px-4 py-3 font-medium">Documentos</th>
              <th className="px-4 py-3 font-medium">Origem</th>
              <th className="px-4 py-3 font-medium">Resumo</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100 bg-white">
            {rows.map((row) => (
              <tr key={row.decisionEventId} className="align-top hover:bg-slate-50">
                <td className="px-4 py-3">
                  <p className="font-semibold text-slate-950">{row.processNumber}</p>
                  <p className="mt-1 text-xs text-slate-500">{row.processClass}</p>
                </td>
                <td className="px-4 py-3 text-slate-600">
                  <Link
                    href={`/caso/${encodeURIComponent(row.decisionEventId)}${contextQuery ?? ""}`}
                    className="cursor-pointer rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-900 transition hover:border-verde-600 hover:text-verde-700"
                  >
                    Ver detalhes
                  </Link>
                </td>
                <td className="px-4 py-3 text-slate-700">{row.decisionDate}</td>
                <td className="px-4 py-3 text-slate-700">{row.decisionType}</td>
                <td className="px-4 py-3 text-slate-700">{row.decisionProgress}</td>
                <td className="px-4 py-3 text-slate-700">
                  <p>{row.judgingBody}</p>
                  <p className="mt-1 text-xs text-slate-500">{row.collegiateLabel}</p>
                </td>
                <td className="px-4 py-3 text-slate-700">
                  <p>{row.processClass}</p>
                  <p className="mt-1 text-xs text-slate-500">{row.firstSubject !== "INCERTO" ? row.firstSubject : row.branchOfLaw}</p>
                </td>
                <td className="px-4 py-3 text-slate-700">
                  <p>{row.docCountLabel} documentos</p>
                  <p className="mt-1 text-xs text-slate-500">{row.acordaoLabel} · {row.monocraticDecisionLabel}</p>
                  {getSafeExternalHref(row.inteiroTeorUrl) ? (
                    <a
                      href={getSafeExternalHref(row.inteiroTeorUrl) ?? undefined}
                      target="_blank"
                      rel="noreferrer"
                      className="mt-2 inline-flex cursor-pointer rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-900 transition hover:border-verde-600 hover:text-verde-700"
                    >
                      Abrir inteiro teor
                    </a>
                  ) : null}
                </td>
                <td className="px-4 py-3 text-slate-700">{row.originDescription}</td>
                <td className="max-w-sm px-4 py-3 text-slate-600">{row.decisionNoteSnippet}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
