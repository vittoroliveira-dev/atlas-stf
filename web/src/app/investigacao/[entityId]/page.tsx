import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft, Shield } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import {
  fetchInvestigationDetail,
  nodeTypeLabel,
} from "@/lib/grafo-data";

function strengthBadge(s: string | null): { text: string; cls: string } {
  if (s === "strong") return { text: "Forte", cls: "bg-verde-100 text-verde-700" };
  if (s === "moderate") return { text: "Moderada", cls: "bg-ouro-100 text-ouro-700" };
  if (s === "weak") return { text: "Fraca", cls: "bg-red-100 text-red-700" };
  return { text: s ?? "---", cls: "bg-slate-100 text-slate-600" };
}

export default async function InvestigacaoDetalhe({
  params,
}: {
  params: Promise<{ entityId: string }>;
}) {
  const { entityId } = await params;
  const data = await fetchInvestigationDetail(entityId);
  if (!data) notFound();

  const { node, score, bundles, edges } = data;

  return (
    <AppShell currentPath="/investigacao" eyebrow="Investigação" title={node.canonical_label ?? entityId} description={`Detalhe — ${nodeTypeLabel(node.node_type)}`}>
      <Link href="/investigacao" className="inline-flex items-center gap-1 text-sm text-marinho-600 hover:underline mb-6">
        <ArrowLeft className="h-4 w-4" aria-hidden="true" /> Voltar
      </Link>

      {/* Score cards */}
      {score && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4 mb-8">
          <div className="rounded-lg border border-slate-200 p-4">
            <p className="text-xs text-slate-500">Score calibrado</p>
            <p className="text-xl font-semibold tabular-nums">{score.calibrated_score.toFixed(3)}</p>
          </div>
          <div className="rounded-lg border border-slate-200 p-4">
            <p className="text-xs text-slate-500">Prioridade operacional</p>
            <p className="text-xl font-semibold tabular-nums">{score.operational_priority.toFixed(3)}</p>
          </div>
          <div className="rounded-lg border border-slate-200 p-4">
            <p className="text-xs text-slate-500">Score documental</p>
            <p className="text-xl font-semibold tabular-nums">{score.documentary_score.toFixed(3)}</p>
          </div>
          <div className="rounded-lg border border-slate-200 p-4">
            <p className="text-xs text-slate-500">Score de rede</p>
            <p className="text-xl font-semibold tabular-nums">{score.network_score.toFixed(3)}</p>
          </div>
        </div>
      )}

      {/* Identidade */}
      <section className="mb-8 rounded-lg border border-slate-200 p-4">
        <h2 className="text-base font-semibold text-slate-800 mb-3">Identidade</h2>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
          <dt className="text-slate-500">Tipo</dt>
          <dd>{nodeTypeLabel(node.node_type)}</dd>
          <dt className="text-slate-500">Identificador</dt>
          <dd className="font-mono">{node.entity_identifier ?? "---"}</dd>
          <dt className="text-slate-500">Tipo do identificador</dt>
          <dd>{node.entity_identifier_type ?? "---"}</dd>
          <dt className="text-slate-500">Qualidade</dt>
          <dd>{node.entity_identifier_quality ?? "---"}</dd>
        </dl>
      </section>

      {/* Evidências */}
      {bundles.length > 0 && (
        <section className="mb-8">
          <h2 className="text-base font-semibold text-slate-800 mb-3">Evidências ({bundles.length})</h2>
          <div className="space-y-3">
            {bundles.map((b) => (
              <div key={b.bundle_id} className="rounded-lg border border-slate-200 p-4">
                <div className="flex items-center gap-2 mb-1">
                  <Shield className="h-4 w-4 text-slate-400" aria-hidden="true" />
                  <span className="text-sm font-medium">{b.bundle_type ?? "Bundle"}</span>
                  <span className="ml-auto text-xs text-slate-500">{b.signal_count} sinais</span>
                </div>
                {b.summary_text && <p className="text-sm text-slate-600 mt-1">{b.summary_text}</p>}
                {b.signal_types.length > 0 && (
                  <div className="flex flex-wrap gap-1 mt-2">
                    {b.signal_types.map((st) => (
                      <span key={st} className="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-600">{st}</span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Conexões */}
      {edges.length > 0 && (
        <section className="mb-8">
          <h2 className="text-base font-semibold text-slate-800 mb-3">Conexões ({edges.length})</h2>
          <div className="overflow-x-auto rounded-lg border border-slate-200">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Tipo</th>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Destino</th>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Força</th>
                  <th className="px-4 py-2 text-right font-medium text-slate-600">Confiança</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {edges.slice(0, 50).map((e) => {
                  const badge = strengthBadge(e.evidence_strength);
                  return (
                    <tr key={e.edge_id} className="hover:bg-slate-50">
                      <td className="px-4 py-2">{e.edge_type.replace(/_/g, " ")}</td>
                      <td className="px-4 py-2 font-mono text-xs">
                        {e.dst_node_id === node.node_id ? e.src_node_id : e.dst_node_id}
                      </td>
                      <td className="px-4 py-2">
                        <span className={`inline-block rounded-full border px-2 py-0.5 text-xs ${badge.cls}`}>{badge.text}</span>
                      </td>
                      <td className="px-4 py-2 text-right tabular-nums">{e.confidence_score?.toFixed(2) ?? "---"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {edges.length > 50 && <p className="mt-2 text-xs text-slate-500">Exibindo 50 de {edges.length} conexões.</p>}
        </section>
      )}
    </AppShell>
  );
}
