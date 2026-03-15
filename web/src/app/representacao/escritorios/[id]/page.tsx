import { ArrowRight, Building2, Users } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { StatCard } from "@/components/dashboard/stat-card";
import { getFirmDetail } from "@/lib/representation-data";
import Link from "next/link";

export default async function FirmDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const route = await params;
  const data = await getFirmDetail(route.id);

  if (!data) {
    return (
      <AppShell
        currentPath="/representacao"
        eyebrow="Atlas STF · escritorio"
        title="Escritorio nao encontrado"
        description="O identificador informado nao corresponde a nenhum escritorio registrado."
      >
        <Link href="/representacao?tab=escritorios" className="inline-flex items-center gap-2 rounded-2xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 hover:border-verde-600">
          Voltar para escritorios <ArrowRight className="h-4 w-4" />
        </Link>
      </AppShell>
    );
  }

  const firm = data.firm;

  return (
    <AppShell
      currentPath="/representacao"
      eyebrow="Atlas STF · detalhe do escritorio"
      title={firm.firm_name_normalized ?? firm.firm_name_raw}
      description="Advogados vinculados a este escritorio."
    >
      <section className="grid gap-4 md:grid-cols-3">
        <StatCard icon={Users} label="Advogados" value={String(data.lawyers.length)} help="Advogados vinculados a este escritorio." />
        <StatCard icon={Building2} label="Processos" value={String(firm.process_count)} help="Processos em que este escritorio aparece." />
        <StatCard
          icon={Building2}
          label="CNPJ"
          value={firm.cnpj ?? "—"}
          help={firm.cnsa_number ? `CNSA: ${firm.cnsa_number}` : "Sem CNSA"}
        />
      </section>

      <Link href="/representacao?tab=escritorios" className="inline-flex items-center gap-2 rounded-2xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 hover:border-verde-600">
        Voltar para escritorios <ArrowRight className="h-4 w-4" />
      </Link>

      {data.lawyers.length > 0 ? (
        <section>
          <h2 className="mb-3 text-lg font-semibold text-slate-900">Advogados vinculados</h2>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {data.lawyers.map((lawyer) => (
              <Link
                key={lawyer.lawyer_id}
                href={`/representacao/advogados/${lawyer.lawyer_id}`}
                className="group rounded-2xl border border-slate-200 bg-white p-4 shadow-sm transition hover:border-verde-300 hover:shadow-md"
              >
                <h3 className="text-sm font-semibold text-slate-900 group-hover:text-verde-700 line-clamp-2">
                  {lawyer.lawyer_name_normalized ?? lawyer.lawyer_name_raw}
                </h3>
                {lawyer.oab_number ? (
                  <p className="mt-1 text-xs text-slate-500">OAB {lawyer.oab_number}</p>
                ) : null}
                <div className="mt-2 flex gap-4 text-xs text-slate-500">
                  <span>{lawyer.process_count} processos</span>
                  <span>{lawyer.event_count} eventos</span>
                </div>
              </Link>
            ))}
          </div>
        </section>
      ) : (
        <p className="text-center text-sm text-slate-500 py-8">Nenhum advogado vinculado a este escritorio.</p>
      )}
    </AppShell>
  );
}
