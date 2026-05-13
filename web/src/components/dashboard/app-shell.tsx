import Link from "next/link";
import { AlertTriangle, ShieldAlert, ShieldCheck } from "lucide-react";
import { formatDataFreshness } from "@/lib/data-freshness";
import {
  buildFilterHref,
  labelCollegiateFilter,
  type FilterContext,
} from "@/lib/filter-context";

type NavGroup = {
  title: string;
  items: Array<{ href: string; label: string }>;
};

const NAV_GROUPS: NavGroup[] = [
  {
    title: "Panorama",
    items: [
      { href: "/", label: "Resumo" },
      { href: "/ministros", label: "Comparar período" },
      { href: "/temporal", label: "Análise temporal" },
      { href: "/velocidade", label: "Velocidade decisória" },
    ],
  },
  {
    title: "Casos e decisões",
    items: [
      { href: "/caso", label: "Casos" },
      { href: "/alertas", label: "Pontos de atenção" },
      { href: "/convergencia", label: "Sinais combinados" },
      { href: "/redistribuicao", label: "Redistribuição" },
      { href: "/origem", label: "Tribunais de origem" },
    ],
  },
  {
    title: "Entidades",
    items: [
      { href: "/advogados", label: "Representantes" },
      { href: "/partes", label: "Partes envolvidas" },
      { href: "/representacao", label: "Representação processual" },
      { href: "/rede-advogados", label: "Rede de advogados" },
      { href: "/afinidade", label: "Afinidade min.-adv." },
    ],
  },
  {
    title: "Fontes externas",
    items: [
      { href: "/sancoes", label: "Sanções administrativas" },
      { href: "/doacoes", label: "Doações eleitorais" },
      { href: "/vinculos", label: "Vínculos empresariais" },
      { href: "/agenda", label: "Agenda ministerial" },
    ],
  },
  {
    title: "Método e auditoria",
    items: [
      { href: "/auditoria", label: "Auditoria" },
      { href: "/investigacao", label: "Investigação" },
      { href: "/revisao", label: "Revisão" },
    ],
  },
];

export function AppShell({
  currentPath,
  filterContext,
  heroState,
  eyebrow,
  title,
  description,
  children,
  guidance,
  lastUpdate,
}: {
  currentPath: string;
  filterContext?: FilterContext;
  heroState?: {
    status: "ok" | "empty" | "inconclusivo" | "error";
    title: string;
    description: string;
  };
  eyebrow: string;
  title: string;
  description: string;
  children: React.ReactNode;
  guidance?: {
    title: string;
    summary: string;
    bullets: string[];
  };
  /**
   * Timestamp ISO da última atualização dos dados exibidos. Derivado
   * tipicamente do `sourceFiles` da página via `pickLatestUpdate`.
   * Quando presente, renderiza rodapé discreto no hero:
   *   "Dados atualizados até DD/MM/YYYY às HH:MM"
   */
  lastUpdate?: string | null;
}) {
  const lastUpdateLabel = formatDataFreshness(lastUpdate);
  const breadcrumbItems = [
    filterContext?.minister,
    filterContext?.period,
    labelCollegiateFilter(filterContext?.collegiate),
  ].filter((item): item is string => Boolean(item));

  return (
    <main id="main-content" className="mx-auto flex min-h-screen w-full max-w-[1600px] scroll-mt-20 flex-col gap-8 px-4 py-6 sm:px-6 lg:px-8">
      <section className="overflow-hidden rounded-hero border border-marinho-950 bg-marinho-900 bg-[radial-gradient(circle_at_top_right,_rgba(0,125,48,0.22),_transparent_55%)] p-6 text-white shadow-elevation-hero sm:p-8">
        <div className="max-w-4xl">
          <div className="inline-flex rounded-full border border-white/15 bg-white/10 px-4 py-2 text-xs font-semibold tracking-[0.02em] text-verde-100">
            {eyebrow}
          </div>
          {breadcrumbItems.length > 0 ? (
            <div className="mt-4 flex flex-wrap items-center gap-2 text-sm text-verde-50/90">
              <span className="text-xs font-semibold tracking-[0.02em] text-verde-100/80">
                Filtro atual
              </span>
              {breadcrumbItems.map((item, index) => (
                <div key={`${item}:${index}`} className="inline-flex items-center gap-2">
                  {index > 0 ? <span className="text-white/40">/</span> : null}
                  <span className="rounded-full border border-white/10 bg-white/10 px-3 py-1.5 text-sm text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]">
                    {item}
                  </span>
                </div>
              ))}
            </div>
          ) : null}
          <h1 className="mt-6 text-4xl font-semibold tracking-tight sm:text-5xl">{title}</h1>
          <p className="mt-5 max-w-3xl text-base leading-7 text-slate-200 sm:text-lg">{description}</p>
          {lastUpdateLabel ? (
            <p className="mt-4 text-xs text-verde-100/85">
              <span className="font-semibold">Dados atualizados até</span>{" "}
              <time dateTime={lastUpdate ?? undefined}>{lastUpdateLabel}</time>
              <span className="ml-2 text-verde-100/60">
                · corte do recorte atual
              </span>
            </p>
          ) : null}
        </div>
        {heroState ? (
          <div
            className={`mt-6 flex flex-col gap-3 rounded-card border px-5 py-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.08)] sm:flex-row sm:items-start sm:justify-between ${
              heroState.status === "empty"
                ? "border-amber-300/30 bg-amber-400/12"
                : heroState.status === "error"
                  ? "border-red-300/30 bg-red-400/12"
                : heroState.status === "inconclusivo"
                  ? "border-marinho-300/30 bg-marinho-400/12"
                  : "border-verde-300/20 bg-verde-400/10"
            }`}
          >
            <div className="flex items-start gap-3">
              {heroState.status === "empty" ? (
                <AlertTriangle className="mt-0.5 h-5 w-5 text-amber-200" aria-hidden="true" focusable="false" />
              ) : heroState.status === "error" ? (
                <AlertTriangle className="mt-0.5 h-5 w-5 text-red-200" aria-hidden="true" focusable="false" />
              ) : heroState.status === "inconclusivo" ? (
                <ShieldAlert className="mt-0.5 h-5 w-5 text-marinho-200" aria-hidden="true" focusable="false" />
              ) : (
                <ShieldCheck className="mt-0.5 h-5 w-5 text-verde-200" aria-hidden="true" focusable="false" />
              )}
              <div>
                <p className="text-xs font-semibold tracking-[0.02em] text-white/85">
                  Leitura rápida
                </p>
                <p className="mt-1 text-lg font-semibold text-white">{heroState.title}</p>
                <p className="mt-1 max-w-3xl text-sm leading-6 text-white/90">
                  {heroState.description}
                </p>
              </div>
            </div>
            <span
              className={`inline-flex rounded-full px-3 py-1.5 text-xs font-semibold tracking-[0.02em] ${
                heroState.status === "empty"
                  ? "bg-amber-100 text-amber-900"
                  : heroState.status === "error"
                    ? "bg-red-100 text-red-900"
                  : heroState.status === "inconclusivo"
                    ? "bg-marinho-100 text-marinho-900"
                    : "bg-verde-100 text-verde-900"
              }`}
            >
              {heroState.status === "empty"
                ? "sem resultados"
                : heroState.status === "error"
                  ? "erro de carga"
                : heroState.status === "inconclusivo"
                  ? "mais contexto"
                  : "pronto para análise"}
            </span>
          </div>
        ) : null}
      </section>
      {guidance ? (
        <details className="group rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1">
          <summary className="flex cursor-pointer list-none items-start justify-between gap-4">
            <div>
              <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Ajuda rápida</p>
              <h2 className="mt-2 text-xl font-semibold text-slate-950">{guidance.title}</h2>
              <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">{guidance.summary}</p>
            </div>
            <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700">
              Ver orientações
            </span>
          </summary>
          <div className="mt-4 grid gap-3 md:grid-cols-3">
            {guidance.bullets.map((item) => (
              <div key={item} className="rounded-2xl bg-slate-50 p-4 text-sm leading-6 text-slate-700">
                {item}
              </div>
            ))}
          </div>
        </details>
      ) : null}
      {children}
      <nav
        id="site-navigation"
        aria-label="Áreas do painel"
        className="mt-4 scroll-mt-20 rounded-card border border-slate-200 bg-white p-6 shadow-elevation-1"
      >
        <p className="text-xs font-semibold tracking-[0.02em] text-slate-500">Navegação geral</p>
        <h2 className="mt-2 text-xl font-semibold text-slate-950">Mapa completo das áreas</h2>
        <div className="mt-5 grid gap-6 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
          {NAV_GROUPS.map((group) => (
            <div key={group.title}>
              <p className="text-xs font-semibold tracking-[0.02em] text-marinho-700">{group.title}</p>
              <ul className="mt-3 space-y-1.5 text-sm">
                {group.items.map((item) => {
                  const active = currentPath === item.href;
                  const href = buildFilterHref(item.href, filterContext ?? {});
                  return (
                    <li key={item.href}>
                      <Link
                        href={href}
                        aria-current={active ? "page" : undefined}
                        className={`block rounded-sm px-1.5 py-2 transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-verde-600 ${
                          active
                            ? "font-semibold text-verde-800"
                            : "text-slate-700 hover:text-verde-700"
                        }`}
                      >
                        {item.label}
                      </Link>
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </div>
      </nav>
    </main>
  );
}
