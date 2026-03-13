import Link from "next/link";
import {
  ArrowUpRight,
  Banknote,
  Building2,
  Flame,
  Link2,
  Users,
} from "lucide-react";
import type { CompoundRiskItem } from "@/lib/compound-risk-data";

function entityTypeLabel(entityType: "party" | "counsel") {
  return entityType === "party" ? "Parte" : "Advogado";
}

function signalLabel(signal: string) {
  switch (signal) {
    case "sanction":
      return "Sancao";
    case "donation":
      return "Doacao";
    case "corporate":
      return "Vinculo";
    case "affinity":
      return "Afinidade";
    case "alert":
      return "Alerta";
    default:
      return signal;
  }
}

function signalTone(signal: string) {
  switch (signal) {
    case "sanction":
      return "border-amber-200 bg-amber-50 text-amber-800";
    case "donation":
      return "border-ouro-200 bg-ouro-50 text-ouro-800";
    case "corporate":
      return "border-marinho-200 bg-marinho-50 text-marinho-800";
    case "affinity":
      return "border-verde-200 bg-verde-50 text-verde-800";
    case "alert":
      return "border-rose-200 bg-rose-50 text-rose-800";
    default:
      return "border-slate-200 bg-slate-50 text-slate-700";
  }
}

function formatDelta(value: number | null) {
  if (value == null) {
    return "—";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(1)}pp`;
}

function formatCurrency(value: number | null) {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 0,
  }).format(value);
}

function signalSummary(item: CompoundRiskItem) {
  const segments: string[] = [];
  if (item.sanction_match_count > 0) {
    segments.push(`${item.sanction_match_count} sancao(oes)`);
  }
  if (item.donation_match_count > 0) {
    segments.push(`${item.donation_match_count} doacao(oes)`);
  }
  if (item.corporate_conflict_count > 0) {
    segments.push(`${item.corporate_conflict_count} vinculo(s)`);
  }
  if (item.affinity_count > 0) {
    segments.push(`${item.affinity_count} afinidade(s)`);
  }
  if (item.alert_count > 0) {
    segments.push(`${item.alert_count} alerta(s)`);
  }
  return segments.join(" · ") || "Sem detalhamento adicional";
}

function supportingContext(item: CompoundRiskItem) {
  if (item.entity_type === "counsel" && item.supporting_party_names.length > 0) {
    return `Clientes relacionados: ${item.supporting_party_names.slice(0, 3).join(", ")}`;
  }
  if (item.corporate_companies.length > 0) {
    return `Empresas ligadas: ${item.corporate_companies
      .slice(0, 2)
      .map((company) => company.company_name)
      .join(", ")}`;
  }
  if (item.top_process_classes.length > 0) {
    return `Classes recorrentes: ${item.top_process_classes.slice(0, 3).join(", ")}`;
  }
  return "Sem contexto adicional no recorte atual";
}

export function CompoundRiskRanking({
  items,
}: {
  items: CompoundRiskItem[];
}) {
  if (items.length === 0) {
    return (
      <section className="flex items-center gap-3 rounded-[28px] border border-amber-200 bg-amber-50 p-6">
        <Flame className="h-5 w-5 text-amber-700" />
        <p className="text-sm text-amber-800">
          Nenhum par foi encontrado com os filtros atuais.
        </p>
      </section>
    );
  }

  return (
    <section className="grid gap-5">
      {items.map((item) => {
        const detailHref =
          item.entity_type === "party"
            ? `/partes/${encodeURIComponent(item.entity_id)}`
            : `/advogados/${encodeURIComponent(item.entity_id)}`;

        return (
          <article
            key={item.pair_id}
            className="overflow-hidden rounded-[32px] border border-slate-200/80 bg-white/95 shadow-[0_20px_70px_rgba(15,23,42,0.08)]"
          >
            <div className="grid gap-0 xl:grid-cols-[1.2fr_0.8fr]">
              <div className="p-6">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                  <div>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="inline-flex rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-slate-700">
                        {item.minister_name}
                      </span>
                      <span className="inline-flex rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-slate-700">
                        {entityTypeLabel(item.entity_type)}
                      </span>
                      {item.red_flag ? (
                        <span className="inline-flex items-center gap-1 rounded-full border border-rose-200 bg-rose-50 px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-rose-700">
                          <Flame className="h-3.5 w-3.5" />
                          Ponto critico composto
                        </span>
                      ) : null}
                    </div>
                    <h2 className="mt-4 text-3xl font-semibold tracking-tight text-slate-950">
                      {item.entity_name}
                    </h2>
                    <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
                      {signalSummary(item)}
                    </p>
                  </div>

                  <Link
                    href={detailHref}
                    className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-semibold text-slate-900 transition hover:border-slate-400"
                  >
                    Abrir entidade
                    <ArrowUpRight className="h-4 w-4" />
                  </Link>
                </div>

                <div className="mt-5 flex flex-wrap gap-2">
                  {item.signals.map((signal) => (
                    <span
                      key={`${item.pair_id}:${signal}`}
                      className={`inline-flex rounded-full border px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] ${signalTone(signal)}`}
                    >
                      {signalLabel(signal)}
                    </span>
                  ))}
                </div>

                <div className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                  <div className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-sm text-slate-500">Sinais convergentes</p>
                    <p className="mt-2 text-3xl font-semibold text-slate-950">
                      {item.signal_count}
                    </p>
                  </div>
                  <div className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-sm text-slate-500">Casos compartilhados</p>
                    <p className="mt-2 text-3xl font-semibold text-slate-950">
                      {item.shared_process_count}
                    </p>
                  </div>
                  <div className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-sm text-slate-500">Maior delta</p>
                    <p className="mt-2 text-3xl font-semibold text-slate-950">
                      {formatDelta(item.max_rate_delta)}
                    </p>
                  </div>
                  <div className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-sm text-slate-500">Doacoes somadas</p>
                    <p className="mt-2 text-2xl font-semibold text-slate-950">
                      {formatCurrency(item.donation_total_brl)}
                    </p>
                  </div>
                </div>
              </div>

              <div className="border-t border-slate-200/80 bg-slate-50/80 p-6 xl:border-l xl:border-t-0">
                <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">
                  Contexto do par
                </p>
                <div className="mt-4 space-y-4">
                  <div className="rounded-2xl border border-slate-200 bg-white p-4">
                    <div className="flex items-start gap-3">
                      {item.corporate_conflict_count > 0 ? (
                        <Building2 className="mt-0.5 h-4 w-4 text-marinho-700" />
                      ) : item.donation_match_count > 0 ? (
                        <Banknote className="mt-0.5 h-4 w-4 text-ouro-700" />
                      ) : item.affinity_count > 0 ? (
                        <Users className="mt-0.5 h-4 w-4 text-verde-700" />
                      ) : (
                        <Link2 className="mt-0.5 h-4 w-4 text-slate-500" />
                      )}
                      <div>
                        <p className="text-sm font-semibold text-slate-950">
                          {supportingContext(item)}
                        </p>
                        <p className="mt-1 text-sm leading-6 text-slate-600">
                          IDs de processos: {item.shared_process_ids.slice(0, 4).join(", ") || "—"}
                        </p>
                      </div>
                    </div>
                  </div>

                  {item.corporate_companies.length > 0 ? (
                    <div className="rounded-2xl border border-slate-200 bg-white p-4">
                      <p className="text-sm font-semibold text-slate-950">
                        Empresas relacionadas
                      </p>
                      <div className="mt-3 flex flex-wrap gap-2">
                        {item.corporate_companies.map((company) => (
                          <span
                            key={`${item.pair_id}:${company.company_cnpj_basico}`}
                            className="inline-flex rounded-full border border-marinho-200 bg-marinho-50 px-3 py-1.5 text-xs font-medium text-marinho-800"
                          >
                            {company.company_name} · grau {company.link_degree}
                          </span>
                        ))}
                      </div>
                    </div>
                  ) : null}

                  {item.supporting_party_names.length > 0 ? (
                    <div className="rounded-2xl border border-slate-200 bg-white p-4">
                      <p className="text-sm font-semibold text-slate-950">
                        Partes de apoio ao sinal
                      </p>
                      <div className="mt-3 flex flex-wrap gap-2">
                        {item.supporting_party_names.map((name) => (
                          <span
                            key={`${item.pair_id}:${name}`}
                            className="inline-flex rounded-full border border-ouro-200 bg-ouro-50 px-3 py-1.5 text-xs font-medium text-ouro-800"
                          >
                            {name}
                          </span>
                        ))}
                      </div>
                    </div>
                  ) : null}
                </div>
              </div>
            </div>
          </article>
        );
      })}
    </section>
  );
}
