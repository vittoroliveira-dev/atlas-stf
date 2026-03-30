import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import { fetchGraphScores } from "@/lib/grafo-data";

function readParam(v: string | string[] | undefined): string | undefined {
  return Array.isArray(v) ? v[0] : v;
}

export default async function ScoresPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) ?? {};
  const page = Math.max(1, Number(readParam(params.page)) || 1);
  const mode = readParam(params.mode) ?? "broad";
  const minSignals = Number(readParam(params.min_signals)) || 2;

  const data = await fetchGraphScores({ mode, minSignals, page, pageSize: 20 });

  return (
    <AppShell currentPath="/investigacao" eyebrow="Grafo" title="Scores de risco" description="Ranking de entidades por score calibrado">
      <Link href="/investigacao" className="inline-flex items-center gap-1 text-sm text-marinho-600 hover:underline mb-6">
        <ArrowLeft className="h-4 w-4" aria-hidden="true" /> Voltar
      </Link>

      {data.items.length === 0 ? (
        <p className="text-sm text-slate-500">Nenhum score calculado para os filtros selecionados.</p>
      ) : (
        <>
          <div className="overflow-x-auto rounded-lg border border-slate-200">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Entidade</th>
                  <th className="px-4 py-2 text-right font-medium text-slate-600">Calibrado</th>
                  <th className="px-4 py-2 text-right font-medium text-slate-600">Prioridade</th>
                  <th className="px-4 py-2 text-right font-medium text-slate-600">Documental</th>
                  <th className="px-4 py-2 text-right font-medium text-slate-600">Estatístico</th>
                  <th className="px-4 py-2 text-right font-medium text-slate-600">Rede</th>
                  <th className="px-4 py-2 text-right font-medium text-slate-600">Temporal</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {data.items.map((s) => (
                  <tr key={s.score_id} className="hover:bg-slate-50">
                    <td className="px-4 py-2">
                      {s.entity_id ? (
                        <Link href={`/investigacao/${encodeURIComponent(s.entity_id)}`} className="text-marinho-600 hover:underline">
                          {s.entity_id}
                        </Link>
                      ) : (
                        <span className="text-slate-400">---</span>
                      )}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums font-semibold">{s.calibrated_score.toFixed(3)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{s.operational_priority.toFixed(3)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{s.documentary_score.toFixed(3)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{s.statistical_score.toFixed(3)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{s.network_score.toFixed(3)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{s.temporal_score.toFixed(3)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="mt-4">
            <PaginationControls pathname="/investigacao/scores" query={{ mode, min_signals: minSignals }} page={data.page} pageSize={data.pageSize} total={data.total} orderingLabel="Score calibrado" />
          </div>
        </>
      )}
    </AppShell>
  );
}
