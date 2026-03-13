import Link from "next/link";
import { ArrowLeft, ArrowRight, ArrowUpDown } from "lucide-react";

type QueryValue = string | number | undefined;

function buildHref(pathname: string, query: Record<string, QueryValue>) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== "") {
      params.set(key, String(value));
    }
  }
  const serialized = params.toString();
  return serialized ? `${pathname}?${serialized}` : pathname;
}

export function PaginationControls({
  pathname,
  query,
  page,
  pageSize,
  total,
  orderingLabel,
  pageSizeOptions = [12, 24, 48],
  pageParam = "page",
  pageSizeParam = "page_size",
}: {
  pathname: string;
  query: Record<string, QueryValue>;
  page: number;
  pageSize: number;
  total: number;
  orderingLabel: string;
  pageSizeOptions?: number[];
  pageParam?: string;
  pageSizeParam?: string;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(Math.max(page, 1), totalPages);
  const hasPrevious = safePage > 1;
  const hasNext = safePage < totalPages;
  const startItem = total === 0 ? 0 : (safePage - 1) * pageSize + 1;
  const endItem = Math.min(safePage * pageSize, total);

  return (
    <section className="rounded-[30px] border border-slate-200/80 bg-white/95 p-6 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="max-w-3xl">
          <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Paginação e ordenação</p>
          <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">Janela atual da listagem</h2>
          <p className="mt-2 text-sm leading-6 text-slate-600">
            Exibindo {startItem} a {endItem} de {total} registros no recorte atual.
          </p>
        </div>

        <div className="flex flex-col gap-3 lg:items-end">
          <div className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-700">
            <ArrowUpDown className="h-4 w-4 text-slate-500" />
            <span className="font-medium">Ordenação ativa:</span>
            <span>{orderingLabel}</span>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
            <span className="font-mono uppercase tracking-[0.18em] text-slate-500">Itens por página</span>
            {pageSizeOptions.map((option) => {
              const active = option === pageSize;
              return (
                <Link
                  key={option}
                  href={buildHref(pathname, { ...query, [pageParam]: 1, [pageSizeParam]: option })}
                  className={`rounded-full border px-3 py-1.5 font-semibold transition ${
                    active
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-slate-200 bg-white text-slate-700 hover:border-slate-400"
                  }`}
                >
                  {option}
                </Link>
              );
            })}
          </div>
        </div>
      </div>

      <div className="mt-6 flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-700">
          Página <span className="font-semibold text-slate-950">{safePage}</span> de <span className="font-semibold text-slate-950">{totalPages}</span>
        </div>

        <div className="flex items-center gap-3">
          <Link
            href={buildHref(pathname, { ...query, [pageParam]: Math.max(1, safePage - 1), [pageSizeParam]: pageSize })}
            aria-disabled={!hasPrevious}
            aria-label="Ir para pagina anterior"
            className={`inline-flex h-11 items-center justify-center gap-2 rounded-2xl px-4 text-sm font-semibold transition ${
              hasPrevious
                ? "border border-slate-200 bg-white text-slate-900 hover:border-slate-400"
                : "pointer-events-none border border-slate-200 bg-slate-100 text-slate-400"
            }`}
          >
            <ArrowLeft className="h-4 w-4" />
            Anterior
          </Link>
          <Link
            href={buildHref(pathname, { ...query, [pageParam]: Math.min(totalPages, safePage + 1), [pageSizeParam]: pageSize })}
            aria-disabled={!hasNext}
            aria-label="Ir para proxima pagina"
            className={`inline-flex h-11 items-center justify-center gap-2 rounded-2xl px-4 text-sm font-semibold transition ${
              hasNext
                ? "bg-slate-950 text-white hover:bg-slate-800"
                : "pointer-events-none bg-slate-200 text-slate-400"
            }`}
          >
            Próxima
            <ArrowRight className="h-4 w-4" />
          </Link>
        </div>
      </div>
    </section>
  );
}
