"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

type TopCategory = {
  title: string;
  href: string;
  routePrefixes: string[];
};

/**
 * 5 categorias principais do painel. A ordem e os grupos espelham
 * NAV_GROUPS em app-shell.tsx — mantenha os dois em sincronia.
 * routePrefixes define quais paths acendem o estado "active" da categoria.
 */
const TOP_CATEGORIES: readonly TopCategory[] = [
  {
    title: "Panorama",
    href: "/",
    routePrefixes: ["/", "/ministros", "/temporal", "/velocidade"],
  },
  {
    title: "Casos e decisões",
    href: "/caso",
    routePrefixes: ["/caso", "/alertas", "/convergencia", "/redistribuicao", "/origem"],
  },
  {
    title: "Entidades",
    href: "/advogados",
    routePrefixes: ["/advogados", "/partes", "/representacao", "/rede-advogados", "/afinidade"],
  },
  {
    title: "Fontes externas",
    href: "/sancoes",
    routePrefixes: ["/sancoes", "/doacoes", "/vinculos", "/agenda"],
  },
  {
    title: "Método e auditoria",
    href: "/auditoria",
    routePrefixes: ["/auditoria", "/investigacao", "/revisao"],
  },
];

function isActiveCategory(currentPath: string, prefixes: readonly string[]): boolean {
  for (const prefix of prefixes) {
    if (prefix === "/") {
      if (currentPath === "/") return true;
      continue;
    }
    if (currentPath === prefix || currentPath.startsWith(`${prefix}/`)) {
      return true;
    }
  }
  return false;
}

export function TopBar() {
  const pathname = usePathname() ?? "/";

  return (
    <header
      aria-label="Navegação principal"
      className="sticky top-0 z-20 border-b border-slate-200 bg-white"
    >
      <div className="mx-auto flex h-14 max-w-[1600px] items-center justify-between gap-4 px-4 sm:px-6 lg:px-8">
        <Link
          href="/"
          className="inline-flex items-baseline gap-2 text-sm font-semibold text-slate-900 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-verde-600"
        >
          <span className="text-verde-700">Atlas STF</span>
          <span className="hidden text-xs font-medium tracking-[0.02em] text-slate-500 sm:inline">
            · Painel analítico
          </span>
        </Link>

        <nav aria-label="Categorias" className="hidden lg:flex lg:items-center lg:gap-1">
          {TOP_CATEGORIES.map((category) => {
            const active = isActiveCategory(pathname, category.routePrefixes);
            return (
              <Link
                key={category.href}
                href={category.href}
                aria-current={active ? "page" : undefined}
                className={`rounded-md px-3 py-2 text-sm transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-verde-600 ${
                  active
                    ? "font-semibold text-verde-800"
                    : "font-medium text-slate-700 hover:text-verde-700"
                }`}
              >
                {category.title}
              </Link>
            );
          })}
        </nav>

        <a
          href="#site-navigation"
          className="inline-flex h-9 items-center rounded-md border border-slate-200 bg-white px-3 text-xs font-medium text-slate-700 transition hover:border-slate-400 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-verde-600 lg:hidden"
        >
          Ver áreas
        </a>
      </div>
    </header>
  );
}
