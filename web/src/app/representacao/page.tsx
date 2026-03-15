import { Building2, Network, ShieldCheck, Users } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import { StatCard } from "@/components/dashboard/stat-card";
import { getRepresentationPageData } from "@/lib/representation-data";
import { readSearchParam } from "@/lib/filter-context";
import Link from "next/link";

function confidenceBadge(oabStatus: string | null) {
  if (oabStatus === "ativo") return { label: "Confirmado", color: "bg-verde-50 text-verde-700 border-verde-200" };
  if (oabStatus === "format_valid") return { label: "Provavel", color: "bg-ouro-50 text-ouro-700 border-ouro-200" };
  return { label: "Nao confirmado", color: "bg-slate-100 text-slate-600 border-slate-200" };
}

export default async function RepresentacaoPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = (await searchParams) ?? {};
  const page = Number(readSearchParam(query.page) ?? "1");
  const search = readSearchParam(query.search);
  const tab = (readSearchParam(query.tab) ?? "advogados") as "advogados" | "escritorios";

  const data = await getRepresentationPageData({ page, search, tab });

  return (
    <AppShell
      currentPath="/representacao"
      eyebrow="Atlas STF · rede de representacao"
      title="Rede de representacao processual"
      description="Advogados, escritorios e vinculos de representacao extraidos do portal STF."
      guidance={{
        title: "Como usar esta pagina",
        summary: "Veja os advogados com identidade profissional (OAB) e seus vinculos com escritorios e partes.",
        bullets: [
          "O badge de confianca mostra se a OAB foi confirmada, provavel ou nao confirmada.",
          "Clique em um advogado para ver seus vinculos e eventos.",
          "Use a aba Escritorios para ver as bancas identificadas.",
        ],
      }}
    >
      <section className="grid gap-4 md:grid-cols-4">
        <StatCard icon={Users} label="Advogados" value={String(data.summary.total_lawyers)} help="Total de advogados identificados." />
        <StatCard icon={Building2} label="Escritorios" value={String(data.summary.total_firms)} help="Total de escritorios identificados." />
        <StatCard icon={Network} label="Vinculos" value={String(data.summary.total_edges)} help="Total de arestas de representacao." />
        <StatCard icon={ShieldCheck} label="Com OAB" value={String(data.summary.lawyers_with_oab)} help="Advogados com numero OAB identificado." />
      </section>

      <div className="flex gap-2">
        <Link
          href={`/representacao?tab=advogados${search ? `&search=${search}` : ""}`}
          className={`rounded-full px-4 py-2 text-sm font-medium transition ${tab === "advogados" ? "bg-verde-600 text-white" : "bg-slate-100 text-slate-700 hover:bg-slate-200"}`}
        >
          Advogados ({data.lawyerTotal})
        </Link>
        <Link
          href={`/representacao?tab=escritorios${search ? `&search=${search}` : ""}`}
          className={`rounded-full px-4 py-2 text-sm font-medium transition ${tab === "escritorios" ? "bg-verde-600 text-white" : "bg-slate-100 text-slate-700 hover:bg-slate-200"}`}
        >
          Escritorios ({data.firmTotal})
        </Link>
      </div>

      {tab === "advogados" ? (
        <>
          <PaginationControls pathname="/representacao" query={{ tab: "advogados", search }} page={data.page} pageSize={data.pageSize} total={data.lawyerTotal} orderingLabel="processos" />
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {data.lawyers.map((lawyer) => {
              const badge = confidenceBadge(lawyer.oab_status);
              return (
                <Link
                  key={lawyer.lawyer_id}
                  href={`/representacao/advogados/${lawyer.lawyer_id}`}
                  className="group rounded-2xl border border-slate-200 bg-white p-4 shadow-sm transition hover:border-verde-300 hover:shadow-md"
                >
                  <div className="flex items-start justify-between gap-2">
                    <h3 className="text-sm font-semibold text-slate-900 group-hover:text-verde-700 line-clamp-2">
                      {lawyer.lawyer_name_normalized ?? lawyer.lawyer_name_raw}
                    </h3>
                    <span className={`shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-medium ${badge.color}`}>
                      {badge.label}
                    </span>
                  </div>
                  {lawyer.oab_number ? (
                    <p className="mt-1 text-xs text-slate-500">OAB {lawyer.oab_number}</p>
                  ) : null}
                  <div className="mt-3 flex gap-4 text-xs text-slate-500">
                    <span>{lawyer.process_count} processos</span>
                    <span>{lawyer.event_count} eventos</span>
                  </div>
                </Link>
              );
            })}
          </div>
          {data.lawyers.length === 0 ? (
            <p className="text-center text-sm text-slate-500 py-8">Nenhum advogado encontrado.</p>
          ) : null}
        </>
      ) : (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {data.firms.map((firm) => (
            <Link
              key={firm.firm_id}
              href={`/representacao/escritorios/${firm.firm_id}`}
              className="group rounded-2xl border border-slate-200 bg-white p-4 shadow-sm transition hover:border-verde-300 hover:shadow-md"
            >
              <h3 className="text-sm font-semibold text-slate-900 group-hover:text-verde-700 line-clamp-2">
                {firm.firm_name_normalized ?? firm.firm_name_raw}
              </h3>
              {firm.cnpj ? (
                <p className="mt-1 text-xs text-slate-500">CNPJ {firm.cnpj}</p>
              ) : null}
              <div className="mt-3 flex gap-4 text-xs text-slate-500">
                <span>{firm.member_count} advogados</span>
                <span>{firm.process_count} processos</span>
              </div>
            </Link>
          ))}
          {data.firms.length === 0 ? (
            <p className="text-center text-sm text-slate-500 py-8 col-span-full">Nenhum escritorio encontrado.</p>
          ) : null}
        </div>
      )}
    </AppShell>
  );
}
