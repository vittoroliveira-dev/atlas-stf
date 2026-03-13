import { fetchApiJson } from "@/lib/api-client";

export type RapporteurProfile = {
  rapporteur: string;
  process_class: string;
  thematic_key: string;
  decision_year: number;
  event_count: number;
  chi2_statistic: number | null;
  p_value_approx: number | null;
  deviation_flag: boolean;
  deviation_direction: string | null;
  progress_distribution: Record<string, number>;
  group_progress_distribution: Record<string, number>;
};

export type SequentialAnalysis = {
  rapporteur: string;
  decision_year: number;
  n_decisions: number;
  autocorrelation_lag1: number;
  streak_effect_3: number | null;
  streak_effect_5: number | null;
  base_favorable_rate: number;
  post_streak_favorable_rate_3: number | null;
  post_streak_favorable_rate_5: number | null;
  sequential_bias_flag: boolean;
};

export type MinisterBio = {
  minister_name: string;
  appointment_date: string | null;
  appointing_president: string | null;
  birth_date: string | null;
  birth_state: string | null;
  career_summary: string | null;
  political_party_history: string[] | null;
  known_connections: string[] | null;
  news_references: string[] | null;
};

export type AssignmentAudit = {
  process_class: string;
  decision_year: number;
  rapporteur_count: number;
  event_count: number;
  chi2_statistic: number;
  p_value_approx: number;
  uniformity_flag: boolean;
  most_overrepresented_rapporteur: string | null;
  most_underrepresented_rapporteur: string | null;
  rapporteur_distribution: Record<string, number>;
};

export async function getMinisterProfileData(
  minister: string,
): Promise<RapporteurProfile[]> {
  return fetchApiJson<RapporteurProfile[]>(
    `/ministers/${encodeURIComponent(minister)}/profile`,
  );
}

export async function getMinisterSequentialData(
  minister: string,
): Promise<SequentialAnalysis[]> {
  return fetchApiJson<SequentialAnalysis[]>(
    `/ministers/${encodeURIComponent(minister)}/sequential`,
  );
}

export async function getMinisterBioData(
  minister: string,
): Promise<MinisterBio | null> {
  try {
    return await fetchApiJson<MinisterBio>(
      `/ministers/${encodeURIComponent(minister)}/bio`,
    );
  } catch {
    return null;
  }
}

export async function getAssignmentAuditData(): Promise<AssignmentAudit[]> {
  return fetchApiJson<AssignmentAudit[]>("/audit/assignment");
}
