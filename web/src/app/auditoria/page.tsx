import { AppShell } from "@/components/dashboard/app-shell";
import { StatCard } from "@/components/dashboard/stat-card";
import { AlertTriangle, CheckCircle, Layers, ShieldCheck } from "lucide-react";
import { getAssignmentAuditData, type AssignmentAudit } from "@/lib/analytics-data";

function AuditTable({ rows }: { rows: AssignmentAudit[] }) {
  if (rows.length === 0) {
    return <p className="text-sm text-slate-500">Nenhum registro de auditoria encontrado.</p>;
  }
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">Distribuição por classe e ano</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-3 py-2">Classe</th>
              <th className="px-3 py-2">Ano</th>
              <th className="px-3 py-2">Relatores</th>
              <th className="px-3 py-2">Eventos</th>
              <th className="px-3 py-2">Chi²</th>
              <th className="px-3 py-2">p-value</th>
              <th className="px-3 py-2">Uniforme</th>
              <th className="px-3 py-2">Mais representado</th>
              <th className="px-3 py-2">Menos representado</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr
                key={i}
                className={`border-b border-slate-100 ${row.uniformity_flag ? "" : "bg-red-50"}`}
              >
                <td className="px-3 py-2 font-mono text-xs">{row.process_class}</td>
                <td className="px-3 py-2">{row.decision_year}</td>
                <td className="px-3 py-2">{row.rapporteur_count}</td>
                <td className="px-3 py-2">{row.event_count}</td>
                <td className="px-3 py-2">{row.chi2_statistic.toFixed(2)}</td>
                <td className="px-3 py-2">{row.p_value_approx}</td>
                <td className="px-3 py-2">
                  {row.uniformity_flag ? (
                    <span className="inline-flex items-center gap-1 text-verde-700">
                      <CheckCircle className="h-3.5 w-3.5" />
                      sim
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-1 text-red-700">
                      <AlertTriangle className="h-3.5 w-3.5" />
                      não
                    </span>
                  )}
                </td>
                <td className="px-3 py-2 text-xs">{row.most_overrepresented_rapporteur ?? "—"}</td>
                <td className="px-3 py-2 text-xs">{row.most_underrepresented_rapporteur ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default async function AuditoriaPage() {
  const data = await getAssignmentAuditData();

  const totalPairs = data.length;
  const uniformCount = data.filter((d) => d.uniformity_flag).length;
  const nonUniformCount = totalPairs - uniformCount;
  const uniformPercent = totalPairs > 0 ? ((uniformCount / totalPairs) * 100).toFixed(1) : "0";

  return (
    <AppShell
      currentPath="/auditoria"
      eyebrow="Atlas STF · auditoria de distribuição"
      title="Auditoria de distribuição de relatores"
      description="Analisa se a distribuição de relatores por classe processual e ano segue um padrão uniforme, usando teste chi-quadrado."
      heroState={
        nonUniformCount === 0
          ? {
              status: "ok",
              title: "Distribuição uniforme em todos os pares analisados",
              description: "Nenhum desvio significativo foi detectado na distribuição de relatores.",
            }
          : {
              status: "inconclusivo",
              title: `${nonUniformCount} par(es) com distribuição não-uniforme`,
              description: "Alguns pares de classe/ano mostram distribuição de relatores que diverge do esperado.",
            }
      }
      guidance={{
        title: "Como interpretar esta auditoria",
        summary: "O teste chi-quadrado compara a distribuição observada de relatores com uma distribuição uniforme esperada.",
        bullets: [
          "Linhas verdes indicam que a distribuição está dentro do esperado.",
          "Linhas vermelhas indicam desvio significativo — pode haver concentração incomum.",
          "O p-value indica a probabilidade de o desvio observado ser aleatório.",
        ],
      }}
    >
      <section className="grid gap-4 md:grid-cols-4">
        <StatCard icon={Layers} label="Pares analisados" value={String(totalPairs)} help="Total de combinações (classe, ano) avaliadas." />
        <StatCard icon={ShieldCheck} label="Uniformes" value={String(uniformCount)} help="Pares com distribuição uniforme de relatores." />
        <StatCard icon={AlertTriangle} label="Não-uniformes" value={String(nonUniformCount)} help="Pares com desvio estatístico na distribuição." />
        <StatCard icon={CheckCircle} label="% uniforme" value={`${uniformPercent}%`} help="Percentual de pares com distribuição uniforme." />
      </section>

      <AuditTable rows={data} />
    </AppShell>
  );
}
