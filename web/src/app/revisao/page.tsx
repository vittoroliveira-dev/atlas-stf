import Link from "next/link";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import {
  fetchReviewQueue,
  reviewStatusLabel,
  priorityTierLabel,
} from "@/lib/grafo-data";

function readParam(v: string | string[] | undefined): string | undefined {
  return Array.isArray(v) ? v[0] : v;
}

function statusBadge(status: string): string {
  switch (status) {
    case "pending":
      return "bg-ouro-100 text-ouro-700 border-ouro-200";
    case "confirmed_relevant":
      return "bg-red-100 text-red-700 border-red-200";
    case "false_positive":
      return "bg-verde-100 text-verde-700 border-verde-200";
    case "needs_more_data":
      return "bg-blue-100 text-blue-700 border-blue-200";
    default:
      return "bg-slate-100 text-slate-600 border-slate-200";
  }
}

function tierBadge(tier: string | null): string {
  if (tier === "high") return "bg-red-100 text-red-700 border-red-200";
  if (tier === "medium") return "bg-ouro-100 text-ouro-700 border-ouro-200";
  return "bg-slate-100 text-slate-600 border-slate-200";
}

export default async function RevisaoPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) ?? {};
  const page = Math.max(1, Number(readParam(params.page)) || 1);
  const status = readParam(params.status) ?? "pending";
  const tier = readParam(params.tier);

  const data = await fetchReviewQueue({ status, tier, page, pageSize: 20 });

  return (
    <AppShell currentPath="/revisao" eyebrow="Revisão" title="Fila de revisão" description="Itens pendentes de análise humana">
      {/* Filtros */}
      <div className="flex flex-wrap gap-2 mb-6">
        {["pending", "confirmed_relevant", "false_positive", "needs_more_data", "deferred"].map((s) => (
          <Link
            key={s}
            href={`/revisao?status=${s}${tier ? `&tier=${tier}` : ""}`}
            className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${
              status === s ? "bg-marinho-600 text-white border-marinho-600" : "bg-white text-slate-600 border-slate-200 hover:border-slate-400"
            }`}
          >
            {reviewStatusLabel(s)}
          </Link>
        ))}
      </div>

      {data.items.length === 0 ? (
        <p className="text-sm text-slate-500">Nenhum item com status &ldquo;{reviewStatusLabel(status)}&rdquo;.</p>
      ) : (
        <>
          <div className="overflow-x-auto rounded-lg border border-slate-200">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Entidade</th>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Status</th>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Prioridade</th>
                  <th className="px-4 py-2 text-left font-medium text-slate-600">Motivo</th>
                  <th className="px-4 py-2 text-right font-medium text-slate-600">Score</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {data.items.map((item) => (
                  <tr key={item.item_id} className="hover:bg-slate-50">
                    <td className="px-4 py-2">
                      {item.entity_id ? (
                        <Link href={`/investigacao/${encodeURIComponent(item.entity_id)}`} className="text-marinho-600 hover:underline">
                          {item.entity_id}
                        </Link>
                      ) : (
                        <span className="font-mono text-xs text-slate-400">{item.item_id}</span>
                      )}
                    </td>
                    <td className="px-4 py-2">
                      <span className={`inline-block rounded-full border px-2 py-0.5 text-xs ${statusBadge(item.status)}`}>
                        {reviewStatusLabel(item.status)}
                      </span>
                    </td>
                    <td className="px-4 py-2">
                      <span className={`inline-block rounded-full border px-2 py-0.5 text-xs ${tierBadge(item.priority_tier)}`}>
                        {priorityTierLabel(item.priority_tier)}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-slate-500 max-w-xs truncate">{item.review_reason ?? "---"}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{item.priority_score.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="mt-4">
            <PaginationControls
              pathname="/revisao"
              query={{ status, ...(tier ? { tier } : {}) }}
              page={data.page}
              pageSize={data.pageSize}
              total={data.total}
              orderingLabel="Prioridade"
            />
          </div>
        </>
      )}
    </AppShell>
  );
}
