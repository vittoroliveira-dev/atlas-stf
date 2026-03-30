import Link from "next/link";
import {
  AlertTriangle,
  ArrowRightLeft,
  Banknote,
  BarChart3,
  Building2,
  Calendar,
  ClipboardCheck,
  Clock3,
  FileSearch,
  Flame,
  Gauge,
  LayoutDashboard,
  Link2,
  Network,
  Search,
  ShieldAlert,
  ShieldCheck,
  UserRoundSearch,
  Users,
} from "lucide-react";
import {
  buildFilterHref,
  labelCollegiateFilter,
  type FilterContext,
} from "@/lib/filter-context";

const NAV_ITEMS = [
  { href: "/", label: "Resumo", icon: LayoutDashboard },
  { href: "/ministros", label: "Comparar período", icon: UserRoundSearch },
  { href: "/alertas", label: "Pontos de atenção", icon: BarChart3 },
  { href: "/caso", label: "Casos", icon: FileSearch },
  { href: "/advogados", label: "Representantes", icon: UserRoundSearch },
  { href: "/partes", label: "Partes envolvidas", icon: UserRoundSearch },
  { href: "/sancoes", label: "Sanções administrativas", icon: AlertTriangle },
  { href: "/doacoes", label: "Doações eleitorais", icon: Banknote },
  { href: "/vinculos", label: "Vínculos empresariais", icon: Link2 },
  { href: "/afinidade", label: "Afinidade min.-adv.", icon: Users },
  { href: "/convergencia", label: "Sinais combinados", icon: Flame },
  { href: "/velocidade", label: "Velocidade decisória", icon: Gauge },
  { href: "/redistribuicao", label: "Redistribuição", icon: ArrowRightLeft },
  { href: "/representacao", label: "Representação processual", icon: ShieldCheck },
  { href: "/rede-advogados", label: "Rede de advogados", icon: Network },
  { href: "/agenda", label: "Agenda ministerial", icon: Calendar },
  { href: "/temporal", label: "Análise temporal", icon: Clock3 },
  { href: "/origem", label: "Tribunais de origem", icon: Building2 },
  { href: "/auditoria", label: "Auditoria", icon: ShieldAlert },
  { href: "/investigacao", label: "Investigação", icon: Search },
  { href: "/revisao", label: "Revisão", icon: ClipboardCheck },
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
}) {
  const breadcrumbItems = [
    filterContext?.minister,
    filterContext?.period,
    labelCollegiateFilter(filterContext?.collegiate),
  ].filter((item): item is string => Boolean(item));

  return (
    <main className="mx-auto flex min-h-screen w-full max-w-[1600px] flex-col gap-8 px-4 py-6 sm:px-6 lg:px-8">
      <section className="overflow-hidden rounded-[36px] border border-white/60 bg-[linear-gradient(135deg,rgba(0,19,64,0.96),rgba(0,39,118,0.90),rgba(0,99,40,0.82))] p-6 text-white shadow-[0_24px_90px_rgba(15,23,42,0.28)] sm:p-8">
        <div className="flex flex-col gap-8 xl:flex-row xl:items-end xl:justify-between">
          <div className="max-w-4xl">
            <div className="inline-flex rounded-full border border-white/15 bg-white/10 px-4 py-2 font-mono text-xs uppercase tracking-[0.24em] text-verde-100">
              {eyebrow}
            </div>
            {breadcrumbItems.length > 0 ? (
              <div className="mt-4 flex flex-wrap items-center gap-2 text-sm text-verde-50/90">
                  <span className="font-mono text-[11px] uppercase tracking-[0.2em] text-verde-100/80">
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
            <p className="mt-5 text-base leading-7 text-slate-200 sm:text-lg">{description}</p>
          </div>
          <div className="rounded-[28px] border border-white/10 bg-white/10 p-4 backdrop-blur-xl xl:min-w-[360px]">
            <div className="mb-4 flex items-center gap-2 font-mono text-xs uppercase tracking-[0.24em] text-verde-100">
              <ShieldCheck className="h-4 w-4" />
              Caminhos rapidos
            </div>
            <nav className="grid gap-2 sm:grid-cols-2">
              {NAV_ITEMS.map((item) => {
                const Icon = item.icon;
                const active = currentPath === item.href;
                const href = buildFilterHref(item.href, filterContext ?? {});
                return (
                  <Link
                    key={item.href}
                    href={href}
                    className={`inline-flex cursor-pointer items-center gap-3 rounded-2xl border px-4 py-3 text-sm font-medium transition duration-200 ${
                      active
                        ? "border-white/30 bg-white text-slate-950"
                        : "border-white/10 bg-white/5 text-white hover:border-white/20 hover:bg-white/10"
                    }`}
                  >
                    <Icon className="h-4 w-4" />
                    {item.label}
                  </Link>
                );
              })}
            </nav>
          </div>
        </div>
        {heroState ? (
          <div
            className={`mt-6 flex flex-col gap-3 rounded-[28px] border px-5 py-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.08)] sm:flex-row sm:items-start sm:justify-between ${
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
                <AlertTriangle className="mt-0.5 h-5 w-5 text-amber-200" />
              ) : heroState.status === "error" ? (
                <AlertTriangle className="mt-0.5 h-5 w-5 text-red-200" />
              ) : heroState.status === "inconclusivo" ? (
                <ShieldAlert className="mt-0.5 h-5 w-5 text-marinho-200" />
              ) : (
                <ShieldCheck className="mt-0.5 h-5 w-5 text-verde-200" />
              )}
              <div>
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-white/70">
                  Leitura rapida
                </p>
                <p className="mt-1 text-lg font-semibold text-white">{heroState.title}</p>
                <p className="mt-1 max-w-3xl text-sm leading-6 text-white/80">
                  {heroState.description}
                </p>
              </div>
            </div>
            <span
              className={`inline-flex rounded-full px-3 py-1.5 font-mono text-[11px] uppercase tracking-[0.2em] ${
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
                  : "pronto para analise"}
            </span>
          </div>
        ) : null}
      </section>
      {guidance ? (
        <details className="group rounded-[28px] border border-slate-200/80 bg-white/90 p-5 shadow-[0_20px_70px_rgba(15,23,42,0.08)]">
          <summary className="flex cursor-pointer list-none items-start justify-between gap-4">
            <div>
              <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Ajuda rapida</p>
              <h2 className="mt-2 text-xl font-semibold text-slate-950">{guidance.title}</h2>
              <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">{guidance.summary}</p>
            </div>
            <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700">
              Ver orientacoes
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
    </main>
  );
}
