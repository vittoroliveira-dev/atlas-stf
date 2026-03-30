import Link from "next/link";
import { Search, TrendingUp } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import {
  fetchTopInvestigations,
  fetchGraphMetrics,
  fetchGraphSearch,
  nodeTypeLabel,
} from "@/lib/grafo-data";

function readParam(v: string | string[] | undefined): string | undefined {
  return Array.isArray(v) ? v[0] : v;
}

export default async function InvestigacaoPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) ?? {};
  const page = Math.max(1, Number(readParam(params.page)) || 1);
  const query = readParam(params.query) ?? "";
  const mode = readParam(params.mode) ?? "broad";

  const [investigations, metrics, searchResults] = await Promise.all([
    fetchTopInvestigations({ mode, minSignals: 2, page }),
    fetchGraphMetrics(),
    query ? fetchGraphSearch({ query, page: 1, pageSize: 10 }) : null,
  ]);

  return (
    <AppShell currentPath="/investigacao" eyebrow="Grafo" title="Investigação" description="Grafo de relações e investigações priorizadas">
      {/* Métricas */}
      {metrics && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4 mb-8">
          <div className="rounded-lg border border-slate-200 p-4">
            <p className="text-xs text-slate-500">Nós</p>
            <p className="text-xl font-semibold tabular-nums">{metrics.total_nodes.toLocaleString("pt-BR")}</p>
          </div>
          <div className="rounded-lg border border-slate-200 p-4">
            <p className="text-xs text-slate-500">Arestas</p>
            <p className="text-xl font-semibold tabular-nums">{metrics.total_edges.toLocaleString("pt-BR")}</p>
          </div>
          <div className="rounded-lg border border-slate-200 p-4">
            <p className="text-xs text-slate-500">Scores</p>
            <p className="text-xl font-semibold tabular-nums">{metrics.total_scores.toLocaleString("pt-BR")}</p>
          </div>
          <div className="rounded-lg border border-slate-200 p-4">
            <p className="text-xs text-slate-500">Arestas determinísticas</p>
            <p className="text-xl font-semibold tabular-nums">{(metrics.pct_deterministic_edges * 100).toFixed(1)}%</p>
          </div>
        </div>
      )}

      {/* Busca */}
      <form className="mb-8">
        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" aria-hidden="true" />
            <input
              type="text"
              name="query"
              defaultValue={query}
              placeholder="Buscar entidade por nome, CPF, CNPJ..."
              className="w-full rounded-lg border border-slate-200 bg-white py-2 pl-10 pr-4 text-sm focus:border-marinho-400 focus:outline-none"
            />
          </div>
          <button type="submit" className="rounded-lg bg-marinho-600 px-4 py-2 text-sm font-medium text-white hover:bg-marinho-700">
            Buscar
          </button>
        </div>
      </form>

      {/* Resultados de busca */}
      {searchResults && searchResults.items.length > 0 && (
        <section className="mb-8">
          <h2 className="text-lg font-semibold text-slate-800 mb-3">Resultados da busca</h2>
          <div className="overflow-x-auto rounded-lg border border-slate-200">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Entidade</th>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Tipo</th>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Identificador</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {searchResults.items.map((n) => (
                  <tr key={n.node_id} className="hover:bg-slate-50">
                    <td className="px-4 py-2">
                      <Link href={`/investigacao/${encodeURIComponent(n.entity_id ?? n.node_id)}`} className="text-marinho-600 hover:underline">
                        {n.canonical_label ?? n.node_id}
                      </Link>
                    </td>
                    <td className="px-4 py-2 text-slate-500">{nodeTypeLabel(n.node_type)}</td>
                    <td className="px-4 py-2 font-mono text-xs text-slate-400">{n.entity_identifier ?? "---"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {searchResults && searchResults.items.length === 0 && query && (
        <p className="mb-8 text-sm text-slate-500">Nenhum resultado para &ldquo;{query}&rdquo;.</p>
      )}

      {/* Top investigações */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-slate-800">Investigações priorizadas</h2>
          <Link href="/investigacao/scores" className="text-sm text-marinho-600 hover:underline flex items-center gap-1">
            <TrendingUp className="h-4 w-4" aria-hidden="true" /> Ver scores
          </Link>
        </div>
        {investigations.items.length === 0 ? (
          <p className="text-sm text-slate-500">Nenhuma investigação encontrada.</p>
        ) : (
          <>
            <div className="overflow-x-auto rounded-lg border border-slate-200">
              <table className="min-w-full text-sm">
                <thead className="bg-slate-50">
                  <tr>
                    <th className="px-4 py-2 text-left font-medium text-slate-600">Entidade</th>
                    <th className="px-4 py-2 text-left font-medium text-slate-600">Tipo</th>
                    <th className="px-4 py-2 text-right font-medium text-slate-600">Sinais</th>
                    <th className="px-4 py-2 text-right font-medium text-slate-600">Arestas</th>
                    <th className="px-4 py-2 text-right font-medium text-slate-600">Score</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {investigations.items.map((inv) => (
                    <tr key={inv.entity_id} className="hover:bg-slate-50">
                      <td className="px-4 py-2">
                        <Link href={`/investigacao/${encodeURIComponent(inv.entity_id)}`} className="text-marinho-600 hover:underline">
                          {inv.entity_label ?? inv.entity_id}
                        </Link>
                      </td>
                      <td className="px-4 py-2 text-slate-500">{nodeTypeLabel(inv.node_type ?? "")}</td>
                      <td className="px-4 py-2 text-right tabular-nums">{inv.signal_count}</td>
                      <td className="px-4 py-2 text-right tabular-nums">{inv.edge_count}</td>
                      <td className="px-4 py-2 text-right tabular-nums font-medium">
                        {inv.score?.calibrated_score.toFixed(2) ?? "---"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-4">
              <PaginationControls pathname="/investigacao" query={{ mode }} page={investigations.page} pageSize={investigations.pageSize} total={investigations.total} orderingLabel="Score" />
            </div>
          </>
        )}
      </section>
    </AppShell>
  );
}
