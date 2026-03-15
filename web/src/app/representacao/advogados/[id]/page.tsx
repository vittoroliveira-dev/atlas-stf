import { ArrowRight, Gavel, Network, Users } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { StatCard } from "@/components/dashboard/stat-card";
import { getLawyerDetail } from "@/lib/representation-data";
import Link from "next/link";

const ROLE_LABELS: Record<string, string> = {
  counsel_of_record: "Advogado constituido",
  amicus_representative: "Representante de amicus",
  oral_argument_speaker: "Sustentacao oral",
  memorial_submitter: "Memorial",
  power_of_attorney_signatory: "Procurador",
  substitute_counsel: "Substabelecido",
  public_attorney: "Procurador publico",
};

const EVENT_LABELS: Record<string, string> = {
  petition: "Peticao",
  oral_argument: "Sustentacao oral",
  memorial: "Memorial",
  amicus_brief: "Amicus",
  procuracao: "Procuracao",
  substabelecimento: "Substabelecimento",
  withdrawal: "Desistencia",
  other: "Outro",
};

export default async function LawyerDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const route = await params;
  const data = await getLawyerDetail(route.id);

  if (!data) {
    return (
      <AppShell
        currentPath="/representacao"
        eyebrow="Atlas STF · advogado"
        title="Advogado nao encontrado"
        description="O identificador informado nao corresponde a nenhum advogado registrado."
      >
        <Link href="/representacao" className="inline-flex items-center gap-2 rounded-2xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 hover:border-verde-600">
          Voltar para representacao <ArrowRight className="h-4 w-4" />
        </Link>
      </AppShell>
    );
  }

  const lawyer = data.lawyer;
  const uniqueProcessIds = new Set(data.edges.map((e) => e.process_id));

  return (
    <AppShell
      currentPath="/representacao"
      eyebrow="Atlas STF · detalhe do advogado"
      title={lawyer.lawyer_name_normalized ?? lawyer.lawyer_name_raw}
      description="Vinculos de representacao e eventos deste advogado."
    >
      <section className="grid gap-4 md:grid-cols-4">
        <StatCard icon={Users} label="Processos" value={String(uniqueProcessIds.size)} help="Processos em que este advogado aparece." />
        <StatCard icon={Network} label="Vinculos" value={String(data.edges.length)} help="Arestas de representacao." />
        <StatCard icon={Gavel} label="Eventos" value={String(data.events.length)} help="Eventos processuais vinculados." />
        <StatCard
          icon={Users}
          label="OAB"
          value={lawyer.oab_number ?? "—"}
          help={lawyer.oab_status ? `Status: ${lawyer.oab_status}` : "Sem validacao OAB"}
        />
      </section>

      <div className="flex gap-2">
        <Link href="/representacao" className="inline-flex items-center gap-2 rounded-2xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 hover:border-verde-600">
          Voltar para representacao <ArrowRight className="h-4 w-4" />
        </Link>
        {lawyer.firm_id ? (
          <Link href={`/representacao/escritorios/${lawyer.firm_id}`} className="inline-flex items-center gap-2 rounded-2xl border border-verde-300 bg-verde-50 px-4 py-2 text-sm font-semibold text-verde-700 hover:bg-verde-100">
            Ver escritorio
          </Link>
        ) : null}
      </div>

      {data.edges.length > 0 ? (
        <section>
          <h2 className="mb-3 text-lg font-semibold text-slate-900">Vinculos de representacao</h2>
          <div className="overflow-x-auto rounded-2xl border border-slate-200">
            <table className="w-full text-sm">
              <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                <tr>
                  <th className="px-4 py-3">Processo</th>
                  <th className="px-4 py-3">Tipo</th>
                  <th className="px-4 py-3">Papel</th>
                  <th className="px-4 py-3">Eventos</th>
                  <th className="px-4 py-3">Confianca</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {data.edges.map((edge) => (
                  <tr key={edge.edge_id} className="hover:bg-slate-50">
                    <td className="px-4 py-2.5 font-mono text-xs">{edge.process_id.slice(0, 12)}...</td>
                    <td className="px-4 py-2.5">{edge.representative_kind ?? "—"}</td>
                    <td className="px-4 py-2.5">{edge.role_type ? (ROLE_LABELS[edge.role_type] ?? edge.role_type) : "—"}</td>
                    <td className="px-4 py-2.5">{edge.event_count}</td>
                    <td className="px-4 py-2.5">
                      {edge.confidence != null ? `${(edge.confidence * 100).toFixed(0)}%` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {data.events.length > 0 ? (
        <section>
          <h2 className="mb-3 text-lg font-semibold text-slate-900">Eventos processuais</h2>
          <div className="overflow-x-auto rounded-2xl border border-slate-200">
            <table className="w-full text-sm">
              <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                <tr>
                  <th className="px-4 py-3">Data</th>
                  <th className="px-4 py-3">Tipo</th>
                  <th className="px-4 py-3">Descricao</th>
                  <th className="px-4 py-3">Processo</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {data.events.slice(0, 50).map((evt) => (
                  <tr key={evt.event_id} className="hover:bg-slate-50">
                    <td className="px-4 py-2.5 whitespace-nowrap">{evt.event_date ?? "—"}</td>
                    <td className="px-4 py-2.5">{evt.event_type ? (EVENT_LABELS[evt.event_type] ?? evt.event_type) : "—"}</td>
                    <td className="px-4 py-2.5 max-w-xs truncate">{evt.event_description ?? "—"}</td>
                    <td className="px-4 py-2.5 font-mono text-xs">{evt.process_id.slice(0, 12)}...</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}
    </AppShell>
  );
}
