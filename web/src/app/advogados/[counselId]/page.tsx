import { AlertTriangle, ArrowRight, Gavel, Network, Users } from "lucide-react";
import { AppShell } from "@/components/dashboard/app-shell";
import { EntityDetailPanels } from "@/components/dashboard/entity-pages";
import { FilterBar } from "@/components/dashboard/filter-bar";
import { RedFlagBadge } from "@/components/dashboard/sanction-badge";
import { SourceAudit } from "@/components/dashboard/source-audit";
import { StatCard } from "@/components/dashboard/stat-card";
import { getEntityDetailData } from "@/lib/dashboard-data";
import { getCounselDonationProfile } from "@/lib/donations-data";
import { buildFilterHref, buildFilterQuery, readSearchParam } from "@/lib/filter-context";
import { getCounselSanctionProfile } from "@/lib/sanctions-data";
import Link from "next/link";

export default async function CounselDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ counselId: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const route = await params;
  const query = (await searchParams) ?? {};
  const judgingBody = readSearchParam(query.judging_body);
  const processClass = readSearchParam(query.process_class);
  const [data, sanctionProfile, donationProfile] = await Promise.all([
    getEntityDetailData("counsel", route.counselId, {
    minister: readSearchParam(query.minister),
    period: readSearchParam(query.period),
    collegiate: readSearchParam(query.collegiate),
    judgingBody,
    processClass,
  }),
    getCounselSanctionProfile(route.counselId),
    getCounselDonationProfile(route.counselId),
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
      currentPath="/advogados"
      filterContext={filterContext}
      heroState={
        data.loadError
            ? {
                status: "error",
                title: "Não foi possível carregar este representante agora",
                description:
                  "A API de detalhe não respondeu como esperado. Tente recarregar a página ou voltar mais tarde.",
            }
          : data.selectedEntity == null
            ? {
                status: "empty",
                title: "Não encontramos este representante com os filtros atuais",
                description:
                  "Tente voltar para a lista de representantes ou ajustar o período selecionado.",
            }
          : data.selectedSnapshot.data.thematic_flow_interpretation_status === "inconclusivo"
            ? {
                status: "inconclusivo",
                title: "Este representante foi encontrado, mas ainda pede contexto adicional",
                description:
                  "Há informação suficiente para leitura, mas ainda não para interpretações mais fortes.",
              }
            : {
                status: "ok",
                title: "Representante localizado",
                description:
                  "Veja abaixo os casos e ministros ligados a este nome dentro do período selecionado.",
              }
      }
      eyebrow="Atlas STF · detalhe do representante"
      title={data.selectedEntity ? data.selectedEntity.name_raw : "Representante não encontrado"}
      description="Esta página reúne os principais vínculos deste representante no filtro atual."
      guidance={{
        title: "Como ler este detalhe",
        summary: "Aqui você entende com quem este nome aparece e em quais casos ele volta a surgir.",
        bullets: [
          "Comece pelos números principais para ver a frequência deste nome.",
          "Depois veja os ministros ligados a ele no período.",
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
        action="/advogados"
      />

      {data.selectedEntity ? (
        <>
          <section className="grid gap-4 md:grid-cols-3">
            <StatCard icon={Users} label="Ocorrências" value={String(data.selectedEntity.associated_event_count)} help="Quantidade de vezes em que este nome aparece no período selecionado." />
            <StatCard icon={Gavel} label="Casos diferentes" value={String(data.selectedEntity.distinct_process_count)} help="Número de casos em que este nome aparece." />
            <StatCard icon={Network} label="Ministros ligados" value={String(data.relatedMinisters.length)} help="Quantidade de ministros que aparecem ligados a este nome no filtro atual." />
          </section>

          <section className="flex justify-start">
            <Link
              href={buildFilterHref("/advogados", filterContext)}
              className="inline-flex h-11 items-center justify-center gap-2 rounded-2xl border border-slate-300 px-4 text-sm font-semibold text-slate-900 transition hover:border-verde-600 hover:text-verde-700"
            >
              Voltar para a lista de representantes
              <ArrowRight className="h-4 w-4" />
            </Link>
          </section>

          {sanctionProfile ? (
            <section className="rounded-2xl border border-amber-200 bg-amber-50/50 p-5">
              <div className="flex items-center gap-2 mb-4">
                <AlertTriangle className="h-5 w-5 text-amber-600" />
                <h2 className="text-lg font-semibold text-amber-800">Perfil de clientes sancionados</h2>
                {sanctionProfile.red_flag ? <RedFlagBadge /> : null}
              </div>
              <div className="grid gap-3 md:grid-cols-4 text-sm">
                <div className="rounded-xl bg-white p-3 border border-amber-100">
                  <p className="text-slate-500">Clientes sancionados</p>
                  <p className="text-2xl font-semibold text-slate-900">{sanctionProfile.sanctioned_client_count}</p>
                </div>
                <div className="rounded-xl bg-white p-3 border border-amber-100">
                  <p className="text-slate-500">Total de clientes</p>
                  <p className="text-2xl font-semibold text-slate-900">{sanctionProfile.total_client_count}</p>
                </div>
                <div className="rounded-xl bg-white p-3 border border-amber-100">
                  <p className="text-slate-500">Taxa fav. (sancionados)</p>
                  <p className="text-2xl font-semibold text-slate-900">
                    {sanctionProfile.sanctioned_favorable_rate != null
                      ? `${(sanctionProfile.sanctioned_favorable_rate * 100).toFixed(1)}%`
                      : "—"}
                  </p>
                </div>
                <div className="rounded-xl bg-white p-3 border border-amber-100">
                  <p className="text-slate-500">Taxa fav. (geral)</p>
                  <p className="text-2xl font-semibold text-slate-900">
                    {sanctionProfile.overall_favorable_rate != null
                      ? `${(sanctionProfile.overall_favorable_rate * 100).toFixed(1)}%`
                      : "—"}
                  </p>
                </div>
              </div>
            </section>
          ) : null}

          {donationProfile ? (
            <section className="rounded-2xl border border-ouro-200 bg-ouro-50/50 p-5">
              <div className="flex items-center gap-2 mb-4">
                <AlertTriangle className="h-5 w-5 text-ouro-600" />
                <h2 className="text-lg font-semibold text-ouro-800">Perfil de clientes doadores</h2>
                {donationProfile.red_flag ? (
                  <span className="inline-flex rounded-full border border-red-300 bg-red-50 px-2.5 py-0.5 text-xs font-semibold text-red-700">Ponto critico</span>
                ) : null}
              </div>
              <div className="grid gap-3 md:grid-cols-4 text-sm">
                <div className="rounded-xl bg-white p-3 border border-ouro-100">
                  <p className="text-slate-500">Clientes doadores</p>
                  <p className="text-2xl font-semibold text-slate-900">{donationProfile.donor_client_count}</p>
                </div>
                <div className="rounded-xl bg-white p-3 border border-ouro-100">
                  <p className="text-slate-500">Total de clientes</p>
                  <p className="text-2xl font-semibold text-slate-900">{donationProfile.total_client_count}</p>
                </div>
                <div className="rounded-xl bg-white p-3 border border-ouro-100">
                  <p className="text-slate-500">Taxa fav. (doadores)</p>
                  <p className="text-2xl font-semibold text-slate-900">
                    {donationProfile.donor_client_favorable_rate != null
                      ? `${(donationProfile.donor_client_favorable_rate * 100).toFixed(1)}%`
                      : "—"}
                  </p>
                </div>
                <div className="rounded-xl bg-white p-3 border border-ouro-100">
                  <p className="text-slate-500">Taxa fav. (geral)</p>
                  <p className="text-2xl font-semibold text-slate-900">
                    {donationProfile.overall_favorable_rate != null
                      ? `${(donationProfile.overall_favorable_rate * 100).toFixed(1)}%`
                      : "—"}
                  </p>
                </div>
              </div>
            </section>
          ) : null}

          <EntityDetailPanels
            entity={data.selectedEntity}
            ministers={data.relatedMinisters}
            cases={data.relatedCases}
            entityLabel="representante"
            contextQuery={contextQuery}
          />
        </>
      ) : null}

      <SourceAudit sourceFiles={data.sourceFiles} />
    </AppShell>
  );
}
