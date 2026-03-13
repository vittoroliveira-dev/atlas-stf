import { Building2 } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import {
  fetchOriginContext,
  type OriginContextItem,
} from "@/lib/origin-context-data";

function formatNumber(n: number): string {
  return n.toLocaleString("pt-BR");
}

function sharePctColor(pct: number): string {
  if (pct >= 5) return "bg-red-100 text-red-800";
  if (pct >= 2) return "bg-amber-100 text-amber-800";
  if (pct >= 1) return "bg-yellow-100 text-yellow-800";
  return "bg-slate-100 text-slate-700";
}

function KpiCard({
  label,
  value,
}: {
  label: string;
  value: string | number;
}) {
  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <p className="font-mono text-xs uppercase tracking-widest text-slate-500">
        {label}
      </p>
      <p className="mt-2 text-3xl font-semibold text-slate-900">{value}</p>
    </div>
  );
}

function TopList({
  title,
  items,
}: {
  title: string;
  items: { nome: string; count: number }[];
}) {
  if (!items.length) return null;
  return (
    <div>
      <p className="mb-2 font-mono text-xs uppercase tracking-widest text-slate-500">
        {title}
      </p>
      <ul className="space-y-1">
        {items.slice(0, 5).map((item) => (
          <li
            key={item.nome}
            className="flex items-center justify-between text-sm"
          >
            <span className="truncate text-slate-700">{item.nome}</span>
            <span className="ml-2 text-slate-500">
              {formatNumber(item.count)}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function OriginRow({ item }: { item: OriginContextItem }) {
  return (
    <details className="group rounded-2xl border border-slate-200 bg-white shadow-sm">
      <summary className="flex cursor-pointer list-none items-center gap-4 px-5 py-4">
        <Building2 className="h-5 w-5 text-teal-600" />
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-3">
            <span className="text-lg font-semibold text-slate-900">
              {item.tribunal_label}
            </span>
            <span className="rounded-full bg-slate-100 px-2.5 py-0.5 text-xs font-medium text-slate-600">
              {item.state}
            </span>
          </div>
          <p className="mt-0.5 text-sm text-slate-500">
            {formatNumber(item.datajud_total_processes)} processos DataJud
            {" / "}
            {formatNumber(item.stf_process_count)} chegaram ao STF
          </p>
        </div>
        <span
          className={`rounded-full px-3 py-1 text-xs font-semibold ${sharePctColor(item.stf_share_pct)}`}
        >
          {item.stf_share_pct.toFixed(2)}%
        </span>
      </summary>
      <div className="grid gap-6 border-t border-slate-100 px-5 py-4 md:grid-cols-3">
        <TopList title="Principais assuntos" items={item.top_assuntos} />
        <TopList
          title="Orgaos julgadores"
          items={item.top_orgaos_julgadores}
        />
        <TopList title="Classes processuais" items={item.class_distribution} />
      </div>
    </details>
  );
}

export default async function OrigemPage({
  searchParams,
}: {
  searchParams: Promise<{ state?: string }>;
}) {
  const params = await searchParams;
  const data = await fetchOriginContext(params.state);

  const totalDatajud = data.items.reduce(
    (sum, item) => sum + item.datajud_total_processes,
    0,
  );
  const totalStf = data.items.reduce(
    (sum, item) => sum + item.stf_process_count,
    0,
  );

  return (
    <AppShell
      currentPath="/origem"
      eyebrow="Atlas STF"
      title="Tribunais de origem"
      description="Contexto agregado dos tribunais que enviam processos ao STF, via dados do CNJ DataJud."
    >
      <div className="grid gap-4 sm:grid-cols-3">
        <KpiCard label="Tribunais mapeados" value={data.total} />
        <KpiCard
          label="Total processos DataJud"
          value={formatNumber(totalDatajud)}
        />
        <KpiCard
          label="Processos que chegaram ao STF"
          value={formatNumber(totalStf)}
        />
      </div>

      <div className="mt-6 space-y-3">
        {data.items.map((item) => (
          <OriginRow key={item.origin_index} item={item} />
        ))}
        {data.items.length === 0 && (
          <div className="rounded-2xl border border-slate-200 bg-white p-8 text-center text-slate-500">
            Nenhum dado de origem disponivel. Execute{" "}
            <code className="rounded bg-slate-100 px-1.5 py-0.5 text-xs">
              atlas-stf datajud fetch
            </code>{" "}
            e{" "}
            <code className="rounded bg-slate-100 px-1.5 py-0.5 text-xs">
              atlas-stf datajud build-context
            </code>{" "}
            para popular os dados.
          </div>
        )}
      </div>
    </AppShell>
  );
}
