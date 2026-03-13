import { AppShell } from "@/components/dashboard/app-shell";
import { StatCard } from "@/components/dashboard/stat-card";
import { Activity, AlertTriangle, BarChart3, Link2, User, Users } from "lucide-react";
import {
  getMinisterBioData,
  getMinisterProfileData,
  getMinisterSequentialData,
  type MinisterBio,
  type RapporteurProfile,
  type SequentialAnalysis,
} from "@/lib/analytics-data";
import { getMinisterCorporateConflicts, type CorporateConflict } from "@/lib/corporate-network-data";
import { getMinisterCounselAffinities, type CounselAffinity } from "@/lib/counsel-affinity-data";
import Link from "next/link";

function safeDecodePathSegment(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function BioSection({ bio }: { bio: MinisterBio }) {
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">Dados biográficos</h2>
      <div className="grid gap-3 text-sm text-slate-700 sm:grid-cols-2">
        {bio.appointment_date ? <p><span className="font-medium text-slate-900">Nomeação:</span> {bio.appointment_date}</p> : null}
        {bio.appointing_president ? <p><span className="font-medium text-slate-900">Presidente:</span> {bio.appointing_president}</p> : null}
        {bio.birth_date ? <p><span className="font-medium text-slate-900">Nascimento:</span> {bio.birth_date}</p> : null}
        {bio.birth_state ? <p><span className="font-medium text-slate-900">Estado:</span> {bio.birth_state}</p> : null}
        {bio.career_summary ? <p className="sm:col-span-2"><span className="font-medium text-slate-900">Carreira:</span> {bio.career_summary}</p> : null}
      </div>
    </section>
  );
}

function ProfileTable({ profiles }: { profiles: RapporteurProfile[] }) {
  if (profiles.length === 0) {
    return <p className="text-sm text-slate-500">Nenhum perfil de desvio encontrado.</p>;
  }
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">Perfil estatístico de desvio</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-3 py-2">Classe</th>
              <th className="px-3 py-2">Tema</th>
              <th className="px-3 py-2">Ano</th>
              <th className="px-3 py-2">Eventos</th>
              <th className="px-3 py-2">Chi²</th>
              <th className="px-3 py-2">p-value</th>
              <th className="px-3 py-2">Desvio</th>
            </tr>
          </thead>
          <tbody>
            {profiles.map((p, i) => (
              <tr key={i} className={`border-b border-slate-100 ${p.deviation_flag ? "bg-red-50" : ""}`}>
                <td className="px-3 py-2 font-mono text-xs">{p.process_class}</td>
                <td className="px-3 py-2">{p.thematic_key}</td>
                <td className="px-3 py-2">{p.decision_year}</td>
                <td className="px-3 py-2">{p.event_count}</td>
                <td className="px-3 py-2">{p.chi2_statistic?.toFixed(2) ?? "—"}</td>
                <td className="px-3 py-2">{p.p_value_approx ?? "—"}</td>
                <td className="px-3 py-2">
                  {p.deviation_flag ? (
                    <span className="inline-flex items-center gap-1 rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800">
                      <AlertTriangle className="h-3 w-3" />
                      {p.deviation_direction ?? "sim"}
                    </span>
                  ) : (
                    <span className="text-slate-400">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SequentialTable({ rows }: { rows: SequentialAnalysis[] }) {
  if (rows.length === 0) {
    return <p className="text-sm text-slate-500">Nenhuma análise sequencial encontrada.</p>;
  }
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">Análise sequencial</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-3 py-2">Ano</th>
              <th className="px-3 py-2">Decisões</th>
              <th className="px-3 py-2">Autocorrelação</th>
              <th className="px-3 py-2">Streak 3</th>
              <th className="px-3 py-2">Streak 5</th>
              <th className="px-3 py-2">Taxa favorável</th>
              <th className="px-3 py-2">Bias</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((s, i) => (
              <tr key={i} className={`border-b border-slate-100 ${s.sequential_bias_flag ? "bg-amber-50" : ""}`}>
                <td className="px-3 py-2">{s.decision_year}</td>
                <td className="px-3 py-2">{s.n_decisions}</td>
                <td className="px-3 py-2">{s.autocorrelation_lag1.toFixed(4)}</td>
                <td className="px-3 py-2">{s.streak_effect_3?.toFixed(4) ?? "—"}</td>
                <td className="px-3 py-2">{s.streak_effect_5?.toFixed(4) ?? "—"}</td>
                <td className="px-3 py-2">{(s.base_favorable_rate * 100).toFixed(1)}%</td>
                <td className="px-3 py-2">
                  {s.sequential_bias_flag ? (
                    <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800">
                      <AlertTriangle className="h-3 w-3" />
                      sim
                    </span>
                  ) : (
                    <span className="text-slate-400">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function CorporateConflictsSection({ conflicts }: { conflicts: CorporateConflict[] }) {
  if (conflicts.length === 0) {
    return <p className="text-sm text-slate-500">Nenhum vinculo societario encontrado.</p>;
  }
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">Vinculos societarios</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-3 py-2">Empresa</th>
              <th className="px-3 py-2">CNPJ</th>
              <th className="px-3 py-2">Vinculado</th>
              <th className="px-3 py-2">Tipo</th>
              <th className="px-3 py-2">Grau</th>
              <th className="px-3 py-2">Casos</th>
              <th className="px-3 py-2">Delta</th>
              <th className="px-3 py-2">Status</th>
            </tr>
          </thead>
          <tbody>
            {conflicts.map((c) => (
              <tr key={c.conflict_id} className={`border-b border-slate-100 ${c.red_flag ? "bg-red-50" : ""}`}>
                <td className="px-3 py-2">{c.company_name || "—"}</td>
                <td className="px-3 py-2 font-mono text-xs">{c.company_cnpj_basico}</td>
                <td className="px-3 py-2">
                  <Link
                    href={`/${c.linked_entity_type === "party" ? "partes" : "advogados"}/${encodeURIComponent(c.linked_entity_id)}`}
                    className="text-verde-700 hover:underline"
                  >
                    {c.linked_entity_name}
                  </Link>
                </td>
                <td className="px-3 py-2">{c.linked_entity_type === "party" ? "Parte" : "Advogado"}</td>
                <td className="px-3 py-2">
                  <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                    c.link_degree >= 3
                      ? "bg-red-100 text-red-800"
                      : c.link_degree === 2
                        ? "bg-amber-100 text-amber-800"
                        : "bg-slate-100 text-slate-600"
                  }`}>
                    {c.link_degree}
                  </span>
                </td>
                <td className="px-3 py-2">{c.shared_process_count}</td>
                <td className="px-3 py-2">
                  {c.favorable_rate_delta != null
                    ? `${c.favorable_rate_delta > 0 ? "+" : ""}${(c.favorable_rate_delta * 100).toFixed(1)}pp`
                    : "—"}
                </td>
                <td className="px-3 py-2">
                  {c.red_flag ? (
                    <span className="inline-flex items-center gap-1 rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800">
                      <AlertTriangle className="h-3 w-3" />
                      Ponto critico
                    </span>
                  ) : (
                    <span className="text-slate-400">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function CounselAffinitySection({ affinities }: { affinities: CounselAffinity[] }) {
  if (affinities.length === 0) {
    return <p className="text-sm text-slate-500">Nenhuma afinidade atipica encontrada.</p>;
  }
  return (
    <section className="rounded-[28px] border border-slate-200/80 bg-white/90 p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-slate-950">Afinidade com advogados</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-3 py-2">Advogado</th>
              <th className="px-3 py-2">Casos</th>
              <th className="px-3 py-2">Taxa par</th>
              <th className="px-3 py-2">Delta min.</th>
              <th className="px-3 py-2">Delta adv.</th>
              <th className="px-3 py-2">Classes</th>
              <th className="px-3 py-2">Status</th>
            </tr>
          </thead>
          <tbody>
            {affinities.map((a) => (
              <tr key={a.affinity_id} className={`border-b border-slate-100 ${a.red_flag ? "bg-red-50" : ""}`}>
                <td className="px-3 py-2">
                  <Link
                    href={`/advogados/${encodeURIComponent(a.counsel_id)}`}
                    className="text-verde-700 hover:underline"
                  >
                    {a.counsel_name_normalized}
                  </Link>
                </td>
                <td className="px-3 py-2">{a.shared_case_count}</td>
                <td className="px-3 py-2">
                  {a.pair_favorable_rate != null ? `${(a.pair_favorable_rate * 100).toFixed(1)}%` : "—"}
                </td>
                <td className="px-3 py-2">
                  {a.pair_delta_vs_minister != null
                    ? `${a.pair_delta_vs_minister > 0 ? "+" : ""}${(a.pair_delta_vs_minister * 100).toFixed(1)}pp`
                    : "—"}
                </td>
                <td className="px-3 py-2">
                  {a.pair_delta_vs_counsel != null
                    ? `${a.pair_delta_vs_counsel > 0 ? "+" : ""}${(a.pair_delta_vs_counsel * 100).toFixed(1)}pp`
                    : "—"}
                </td>
                <td className="px-3 py-2">{a.top_process_classes.join(", ") || "—"}</td>
                <td className="px-3 py-2">
                  {a.red_flag ? (
                    <span className="inline-flex items-center gap-1 rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800">
                      <AlertTriangle className="h-3 w-3" />
                      Ponto critico
                    </span>
                  ) : (
                    <span className="text-slate-400">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default async function MinisterProfilePage({
  params,
}: {
  params: Promise<{ minister: string }>;
}) {
  const { minister } = await params;
  const decoded = safeDecodePathSegment(minister);

  const [profiles, sequential, bio, corporateConflicts, counselAffinities] = await Promise.all([
    getMinisterProfileData(decoded),
    getMinisterSequentialData(decoded),
    getMinisterBioData(decoded),
    getMinisterCorporateConflicts(decoded),
    getMinisterCounselAffinities(decoded),
  ]);

  const deviationCount = profiles.filter((p) => p.deviation_flag).length;
  const biasCount = sequential.filter((s) => s.sequential_bias_flag).length;
  const totalEvents = profiles.reduce((sum, p) => sum + p.event_count, 0);

  return (
    <AppShell
      currentPath="/ministros"
      eyebrow="Atlas STF · perfil do ministro"
      title={decoded}
      description="Perfil estatístico completo do ministro, incluindo desvios chi-square, análise sequencial e dados biográficos."
    >
      <section className="grid gap-4 md:grid-cols-4">
        <StatCard icon={User} label="Perfis analisados" value={String(profiles.length)} help="Combinações de (classe, tema, ano) analisadas para este ministro." />
        <StatCard icon={BarChart3} label="Eventos totais" value={String(totalEvents)} help="Total de decisões consideradas nos perfis." />
        <StatCard icon={AlertTriangle} label="Desvios detectados" value={String(deviationCount)} help="Perfis com desvio estatístico significativo (p < 0.05)." />
        <StatCard icon={Activity} label="Anos com bias sequencial" value={String(biasCount)} help="Anos em que foi detectado bias sequencial nas decisões." />
        <StatCard icon={Link2} label="Vinculos societarios" value={String(corporateConflicts.length)} help="Vinculos corporativos detectados via Receita Federal." />
        <StatCard icon={Users} label="Afinidades atipicas" value={String(counselAffinities.filter(a => a.red_flag).length)} help="Pares ministro-advogado com taxa de vitoria anomala." />
      </section>

      {bio ? <BioSection bio={bio} /> : null}
      <ProfileTable profiles={profiles} />
      <SequentialTable rows={sequential} />
      <CorporateConflictsSection conflicts={corporateConflicts} />
      <CounselAffinitySection affinities={counselAffinities} />
    </AppShell>
  );
}
