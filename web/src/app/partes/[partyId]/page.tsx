import { AlertTriangle, ArrowRight, Gavel, Network, Users } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { EntityDetailPanels } from "@/components/dashboard/entity-pages";
import { FilterBar } from "@/components/dashboard/filter-bar";
import { RedFlagBadge, SanctionBadge } from "@/components/dashboard/sanction-badge";
import { SourceAudit } from "@/components/dashboard/source-audit";
import { StatCard } from "@/components/dashboard/stat-card";
import { DonationBadge, DonationRedFlagBadge } from "@/components/dashboard/donation-badge";
import { getEntityDetailData } from "@/lib/dashboard-data";
import { getPartyDonations } from "@/lib/donations-data";
import { buildFilterHref, buildFilterQuery, readSearchParam } from "@/lib/filter-context";
import { getPartySanctions } from "@/lib/sanctions-data";
import Link from "next/link";

export default async function PartyDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ partyId: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const route = await params;
  const query = (await searchParams) ?? {};
  const judgingBody = readSearchParam(query.judging_body);
  const processClass = readSearchParam(query.process_class);
  const [data, partySanctions, partyDonations] = await Promise.all([
    getEntityDetailData("party", route.partyId, {
    minister: readSearchParam(query.minister),
    period: readSearchParam(query.period),
    collegiate: readSearchParam(query.collegiate),
    judgingBody,
    processClass,
  }),
    getPartySanctions(route.partyId),
    getPartyDonations(route.partyId),
  ]);
  const filterContext = {
    minister: data.selectedSnapshot.minister,
    period: data.selectedSnapshot.period,
    collegiate: data.selectedSnapshot.data.collegiate_filter,
    judgingBody,
    processClass,
  };
  const contextQuery = buildFilterQuery(filterContext);

  return (
    <AppShell
      currentPath="/partes"
      filterContext={filterContext}
      heroState={
        data.loadError
            ? {
                status: "error",
                title: "Não foi possível carregar esta parte agora",
                description:
                  "A API de detalhe não respondeu como esperado. Tente recarregar a página ou voltar mais tarde.",
            }
          : data.selectedEntity == null
            ? {
                status: "empty",
                title: "Não encontramos esta parte com os filtros atuais",
                description:
                  "Tente voltar para a lista de partes ou ajustar o período selecionado.",
            }
          : data.selectedSnapshot.data.thematic_flow_interpretation_status === "inconclusivo"
            ? {
                status: "inconclusivo",
                title: "Esta parte foi encontrada, mas ainda pede contexto adicional",
                description:
                  "Há informação suficiente para leitura, mas ainda não para interpretações mais fortes.",
              }
            : {
                status: "ok",
                title: "Parte localizada",
                description:
                  "Veja abaixo os casos e ministros ligados a esta parte dentro do período selecionado.",
              }
      }
      eyebrow="Atlas STF · detalhe da parte"
      title={data.selectedEntity ? data.selectedEntity.name_raw : "Parte não encontrada"}
      description="Esta página reúne os principais vínculos desta parte no filtro atual."
      guidance={{
        title: "Como ler este detalhe",
        summary: "Aqui você entende com quem esta parte aparece e em quais casos ela volta a surgir.",
        bullets: [
          "Comece pelos números principais para ver a frequência desta parte.",
          "Depois veja os ministros ligados a ela no período.",
          "Abra os casos relacionados para entender o contexto de cada aparição.",
        ],
      }}
    >
      <FilterBar
        ministers={data.ministers}
        periods={data.periods}
        judgingBodies={data.judgingBodies}
        processClasses={data.processClasses}
        selectedMinister={data.selectedSnapshot.minister}
        selectedPeriod={data.selectedSnapshot.period}
        selectedCollegiate={data.selectedSnapshot.data.collegiate_filter}
        selectedJudgingBody={judgingBody}
        selectedProcessClass={processClass}
        action="/partes"
      />

      {data.selectedEntity ? (
        <>
          <section className="grid gap-4 md:grid-cols-3">
            <StatCard icon={Users} label="Ocorrências" value={String(data.selectedEntity.associated_event_count)} help="Quantidade de vezes em que esta parte aparece no período selecionado." />
            <StatCard icon={Gavel} label="Casos diferentes" value={String(data.selectedEntity.distinct_process_count)} help="Número de casos em que esta parte aparece." />
            <StatCard icon={Network} label="Ministros ligados" value={String(data.relatedMinisters.length)} help="Quantidade de ministros que aparecem ligados a esta parte no filtro atual." />
          </section>

          <section className="flex justify-start">
            <Link
              href={buildFilterHref("/partes", filterContext)}
              className="inline-flex h-11 items-center justify-center gap-2 rounded-2xl border border-slate-300 px-4 text-sm font-semibold text-slate-900 transition hover:border-verde-600 hover:text-verde-700"
            >
              Voltar para a lista de partes
              <ArrowRight className="h-4 w-4" />
            </Link>
          </section>

          {partySanctions.length > 0 ? (
            <section className="rounded-2xl border border-red-200 bg-red-50/50 p-5">
              <div className="flex items-center gap-2 mb-4">
                <AlertTriangle className="h-5 w-5 text-red-600" />
                <h2 className="text-lg font-semibold text-red-800">Registros em bases publicas de sancoes</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="border-b border-red-200">
                    <tr>
                      <th className="px-3 py-2 font-semibold text-red-700">Fonte</th>
                      <th className="px-3 py-2 font-semibold text-red-700">Tipo</th>
                      <th className="px-3 py-2 font-semibold text-red-700">Orgao</th>
                      <th className="px-3 py-2 font-semibold text-red-700">Inicio</th>
                      <th className="px-3 py-2 font-semibold text-red-700">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-red-100">
                    {partySanctions.map((s) => (
                      <tr key={s.match_id}>
                        <td className="px-3 py-2"><SanctionBadge source={s.sanction_source} /></td>
                        <td className="px-3 py-2 text-slate-700">{s.sanction_type ?? "—"}</td>
                        <td className="px-3 py-2 text-slate-700">{s.sanctioning_body ?? "—"}</td>
                        <td className="px-3 py-2 text-slate-600">{s.sanction_start_date ?? "—"}</td>
                        <td className="px-3 py-2">{s.red_flag ? <RedFlagBadge /> : <span className="text-slate-400">—</span>}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : (
            <section className="rounded-2xl border border-verde-200 bg-verde-50/50 p-4">
              <p className="text-sm text-verde-700">Sem registros em CEIS/CNEP para esta parte.</p>
            </section>
          )}

          {partyDonations.length > 0 ? (
            <section className="rounded-2xl border border-ouro-200 bg-ouro-50/50 p-5">
              <div className="flex items-center gap-2 mb-4">
                <AlertTriangle className="h-5 w-5 text-ouro-600" />
                <h2 className="text-lg font-semibold text-ouro-800">Doacoes eleitorais</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="border-b border-ouro-200">
                    <tr>
                      <th className="px-3 py-2 font-semibold text-ouro-700">CPF/CNPJ</th>
                      <th className="px-3 py-2 font-semibold text-ouro-700" title="Total dos eventos vinculados a este match">Total do match</th>
                      <th className="px-3 py-2 font-semibold text-ouro-700">Eleicoes</th>
                      <th className="px-3 py-2 font-semibold text-ouro-700">Partidos</th>
                      <th className="px-3 py-2 font-semibold text-ouro-700">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-ouro-100">
                    {partyDonations.map((d) => (
                      <tr key={d.match_id}>
                        <td className="px-3 py-2 font-mono text-xs text-slate-700">{d.donor_cpf_cnpj || "—"}</td>
                        <td className="px-3 py-2 text-slate-700">
                          {new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(d.matched_events_total_brl ?? d.total_donated_brl)}
                        </td>
                        <td className="px-3 py-2 text-slate-600">{d.election_years.join(", ") || "—"}</td>
                        <td className="px-3 py-2 text-slate-600">{d.parties_donated_to.join(", ") || "—"}</td>
                        <td className="px-3 py-2">{d.red_flag ? <DonationRedFlagBadge /> : <DonationBadge />}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          <EntityDetailPanels
            entity={data.selectedEntity}
            ministers={data.relatedMinisters}
            cases={data.relatedCases}
            entityLabel="parte"
            contextQuery={contextQuery}
          />
        </>
      ) : null}

      <SourceAudit sourceFiles={data.sourceFiles} />
    </AppShell>
  );
}
