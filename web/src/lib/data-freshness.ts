interface SourceFileLike {
  updatedAt: string;
}

/**
 * Retorna o timestamp ISO mais recente de uma lista de source files.
 * Usado para derivar a "data de corte" do recorte atual e mostrar no hero.
 * Retorna null quando a lista é vazia ou todas as datas são inválidas.
 */
export function pickLatestUpdate(sourceFiles: readonly SourceFileLike[] | undefined): string | null {
  if (!sourceFiles || sourceFiles.length === 0) return null;

  let latest: number | null = null;

  for (const file of sourceFiles) {
    const timestamp = Date.parse(file.updatedAt);
    if (!Number.isFinite(timestamp)) continue;
    if (latest == null || timestamp > latest) {
      latest = timestamp;
    }
  }

  return latest == null ? null : new Date(latest).toISOString();
}

const BRASILIA_FORMATTER = new Intl.DateTimeFormat("pt-BR", {
  timeZone: "America/Sao_Paulo",
  day: "2-digit",
  month: "2-digit",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

/**
 * Formata um timestamp ISO como "DD/MM/YYYY às HH:MM" em horário de Brasília.
 * Fixa a timezone para evitar drift entre servidor (UTC em produção) e cliente —
 * garante que o label exibido é sempre o que jornalistas/acadêmicos esperam citar.
 */
export function formatDataFreshness(isoTimestamp: string | null | undefined): string | null {
  if (!isoTimestamp) return null;
  const date = new Date(isoTimestamp);
  if (Number.isNaN(date.getTime())) return null;

  // Intl output pt-BR: "29/03/2026, 14:30" → trocamos a vírgula-espaço por " às ".
  return BRASILIA_FORMATTER.format(date).replace(", ", " às ");
}
