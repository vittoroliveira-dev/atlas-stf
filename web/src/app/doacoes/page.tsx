import { AlertTriangle } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { PaginationControls } from "@/components/dashboard/pagination-controls";
import {
  CardGrid,
  ExpandableCard,
  RateComparisonBar,
  RedFlagPill,
} from "@/components/dashboard/cross-ref-card";
import { emptyStateMessage } from "@/lib/ui-copy";
import { getPartyDonationsPageData, getCounselDonationsPageData, getDonationRedFlags } from "@/lib/donations-data";
import Link from "next/link";
import { readSearchParam } from "@/lib/filter-context";

const formatBrl = (value: number) =>
  new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(value);

export default async function DoacoesPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = (await searchParams) ?? {};
  const redFlagOnly = readSearchParam(query.red_flag_only) === "true";
  const page = Number(readSearchParam(query.page) ?? "1");
  const counselPage = Number(readSearchParam(query.counsel_page) ?? "1");

  const filterQuery: Record<string, string | undefined> = {
    red_flag_only: redFlagOnly ? "true" : undefined,
  };

  const [partyData, counselData, redFlags] = await Promise.all([
    getPartyDonationsPageData({ page, redFlagOnly }),
    getCounselDonationsPageData({ page: counselPage, redFlagOnly }),
    getDonationRedFlags(),
  ]);

  const partyTotalDonated = partyData.donations.reduce((sum, d) => sum + d.total_donated_brl, 0);
  const counselTotalDonated = counselData.donations.reduce((sum, d) => sum + d.total_donated_brl, 0);

  return (
    <AppShell
      currentPath="/doacoes"
      eyebrow="Atlas STF · doadores de campanha"
      title="Doacoes eleitorais"
      description="Doadores de campanha que tambem litigam no STF."
      guidance={{
        title: "Como interpretar esta tela",
        summary:
          "Mostra partes e advogados que constam como doadores de campanha no TSE e que litigam no STF.",
        bullets: [
          "Ponto critico indica taxa de exito significativamente acima da media para a classe processual.",
          "O matching e feito por nome normalizado entre doador TSE e entidade processual STF.",
          "Doacoes de PJ foram proibidas a partir de 2015 -- dados anteriores podem incluir empresas.",
          "A secao de advogados identifica profissionais que constam como doadores e atuam no STF.",
        ],
      }}
    >
      {/* KPI cards */}
      <section className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Partes doadoras</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{partyData.total}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <p className="text-sm text-slate-500">Advogados doadores</p>
          <p className="mt-1 text-3xl font-semibold text-slate-900">{counselData.total}</p>
        </div>
        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 shadow-sm">
          <p className="text-sm text-red-600">Pontos criticos (partes)</p>
          <p className="mt-1 text-3xl font-semibold text-red-700">{redFlags.totalPartyFlags}</p>
        </div>
        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 shadow-sm">
          <p className="text-sm text-red-600">Pontos criticos (advogados)</p>
          <p className="mt-1 text-3xl font-semibold text-red-700">{redFlags.totalCounselFlags}</p>
        </div>
      </section>

      {/* Filters */}
      <section className="flex flex-wrap gap-3">
        <Link
          href="/doacoes"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            !redFlagOnly
              ? "border-verde-600 bg-verde-50 text-verde-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Todos
        </Link>
        <Link
          href="/doacoes?red_flag_only=true"
          className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
            redFlagOnly
              ? "border-red-500 bg-red-50 text-red-700"
              : "border-slate-200 text-slate-600 hover:border-slate-400"
          }`}
        >
          Apenas pontos criticos
        </Link>
      </section>

      {/* Partes doadoras */}
      <section>
        <h2 className="mb-4 text-lg font-semibold text-slate-900">Partes doadoras</h2>

        <PaginationControls
          pathname="/doacoes"
          query={{ ...filterQuery, counsel_page: counselPage !== 1 ? String(counselPage) : undefined }}
          page={partyData.page}
          pageSize={partyData.pageSize}
          total={partyData.total}
          orderingLabel="cruzamentos de doacoes (partes)"
          pageSizeOptions={[8, 16, 24]}
        />

        {partyTotalDonated > 0 && (
          <div className="mt-4 rounded-2xl border border-ouro-200 bg-ouro-50 p-4">
            <p className="text-sm text-ouro-700">Total doado nesta pagina</p>
            <p className="mt-1 text-2xl font-semibold text-ouro-800">{formatBrl(partyTotalDonated)}</p>
          </div>
        )}

        {partyData.donations.length === 0 ? (
          <div className="mt-4 flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
            <AlertTriangle className="h-5 w-5 text-amber-600" />
            <p className="text-sm text-amber-800">{emptyStateMessage("donations")}</p>
          </div>
        ) : (
          <div className="mt-4">
            <CardGrid columns={2}>
              {partyData.donations.map((d) => (
                <ExpandableCard
                  key={d.match_id}
                  summary={
                    <div className="flex flex-1 flex-wrap items-center gap-3">
                      <Link
                        href={`/partes/${encodeURIComponent(d.party_id)}`}
                        className="font-medium text-verde-700 hover:underline"
                      >
                        {d.party_name_normalized}
                      </Link>
                      <span className="text-2xl font-semibold text-slate-900">{formatBrl(d.total_donated_brl)}</span>
                      <span className="text-sm text-slate-500">
                        {d.election_years.length} eleic{d.election_years.length === 1 ? "ao" : "oes"}
                      </span>
                      <RedFlagPill show={d.red_flag} />
                    </div>
                  }
                >
                  <div className="space-y-4">
                    <RateComparisonBar
                      rate={d.favorable_rate}
                      baseline={d.baseline_favorable_rate}
                      rateLabel="Taxa favoravel"
                      baselineLabel="media"
                    />
                    <dl className="grid gap-2 text-sm sm:grid-cols-2">
                      <div>
                        <dt className="text-slate-500">CPF/CNPJ</dt>
                        <dd className="font-mono text-xs font-medium text-slate-900">{d.donor_cpf_cnpj || "---"}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-500">Casos STF</dt>
                        <dd className="font-medium text-slate-900">{d.stf_case_count}</dd>
                      </div>
                    </dl>
                    <div>
                      <p className="text-sm text-slate-500">Eleicoes</p>
                      <div className="mt-1 flex flex-wrap gap-1.5">
                        {d.election_years.length > 0 ? d.election_years.map((y) => (
                          <span key={y} className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-medium text-slate-700">
                            {y}
                          </span>
                        )) : <span className="text-sm text-slate-400">---</span>}
                      </div>
                    </div>
                    <div>
                      <p className="text-sm text-slate-500">Partidos</p>
                      <div className="mt-1 flex flex-wrap gap-1.5">
                        {d.parties_donated_to.length > 0 ? d.parties_donated_to.map((p) => (
                          <span key={p} className="rounded-full border border-ouro-200 bg-ouro-50 px-2.5 py-0.5 text-xs font-medium text-ouro-800">
                            {p}
                          </span>
                        )) : <span className="text-sm text-slate-400">---</span>}
                      </div>
                    </div>
                  </div>
                </ExpandableCard>
              ))}
            </CardGrid>
          </div>
        )}
      </section>

      {/* Advogados doadores */}
      <section>
        <h2 className="mb-4 text-lg font-semibold text-slate-900">Advogados doadores</h2>

        <PaginationControls
          pathname="/doacoes"
          query={{ ...filterQuery, page: page !== 1 ? String(page) : undefined }}
          page={counselData.page}
          pageSize={counselData.pageSize}
          total={counselData.total}
          orderingLabel="cruzamentos de doacoes (advogados)"
          pageParam="counsel_page"
          pageSizeParam="counsel_page_size"
          pageSizeOptions={[8, 16, 24]}
        />

        {counselTotalDonated > 0 && (
          <div className="mt-4 rounded-2xl border border-ouro-200 bg-ouro-50 p-4">
            <p className="text-sm text-ouro-700">Total doado nesta pagina</p>
            <p className="mt-1 text-2xl font-semibold text-ouro-800">{formatBrl(counselTotalDonated)}</p>
          </div>
        )}

        {counselData.donations.length === 0 ? (
          <div className="mt-4 flex items-center gap-3 rounded-2xl border border-amber-200 bg-amber-50 p-6">
            <AlertTriangle className="h-5 w-5 text-amber-600" />
            <p className="text-sm text-amber-800">{emptyStateMessage("donations")}</p>
          </div>
        ) : (
          <div className="mt-4">
            <CardGrid columns={2}>
              {counselData.donations.map((d) => (
                <ExpandableCard
                  key={d.match_id}
                  summary={
                    <div className="flex flex-1 flex-wrap items-center gap-3">
                      <Link
                        href={`/advogados/${encodeURIComponent(d.counsel_id ?? d.party_id)}`}
                        className="font-medium text-verde-700 hover:underline"
                      >
                        {d.party_name_normalized}
                      </Link>
                      <span className="text-2xl font-semibold text-slate-900">{formatBrl(d.total_donated_brl)}</span>
                      <span className="text-sm text-slate-500">
                        {d.election_years.length} eleic{d.election_years.length === 1 ? "ao" : "oes"}
                      </span>
                      <RedFlagPill show={d.red_flag} />
                    </div>
                  }
                >
                  <div className="space-y-4">
                    <RateComparisonBar
                      rate={d.favorable_rate}
                      baseline={d.baseline_favorable_rate}
                      rateLabel="Taxa favoravel"
                      baselineLabel="media"
                    />
                    <dl className="grid gap-2 text-sm sm:grid-cols-2">
                      <div>
                        <dt className="text-slate-500">CPF/CNPJ</dt>
                        <dd className="font-mono text-xs font-medium text-slate-900">{d.donor_cpf_cnpj || "---"}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-500">Casos STF</dt>
                        <dd className="font-medium text-slate-900">{d.stf_case_count}</dd>
                      </div>
                    </dl>
                    <div>
                      <p className="text-sm text-slate-500">Eleicoes</p>
                      <div className="mt-1 flex flex-wrap gap-1.5">
                        {d.election_years.length > 0 ? d.election_years.map((y) => (
                          <span key={y} className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-xs font-medium text-slate-700">
                            {y}
                          </span>
                        )) : <span className="text-sm text-slate-400">---</span>}
                      </div>
                    </div>
                    <div>
                      <p className="text-sm text-slate-500">Partidos</p>
                      <div className="mt-1 flex flex-wrap gap-1.5">
                        {d.parties_donated_to.length > 0 ? d.parties_donated_to.map((p) => (
                          <span key={p} className="rounded-full border border-ouro-200 bg-ouro-50 px-2.5 py-0.5 text-xs font-medium text-ouro-800">
                            {p}
                          </span>
                        )) : <span className="text-sm text-slate-400">---</span>}
                      </div>
                    </div>
                  </div>
                </ExpandableCard>
              ))}
            </CardGrid>
          </div>
        )}
      </section>
    </AppShell>
  );
}
