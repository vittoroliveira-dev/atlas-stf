export function SanctionBadge({ source }: { source: string }) {
  const label = source.toUpperCase();
  const color =
    source === "ceis"
      ? "bg-red-100 text-red-800 border-red-200"
      : source === "cnep"
        ? "bg-orange-100 text-orange-800 border-orange-200"
        : source === "leniencia"
          ? "bg-indigo-100 text-indigo-800 border-indigo-200"
          : "bg-purple-100 text-purple-800 border-purple-200";

  return (
    <span
      className={`inline-flex rounded-full border px-2.5 py-0.5 text-xs font-semibold ${color}`}
      aria-label={`Origem da sanção: ${label}`}
    >
      {label}
    </span>
  );
}

export function RedFlagBadge() {
  return (
    <span
      className="inline-flex rounded-full border border-red-300 bg-red-50 px-2.5 py-0.5 text-xs font-semibold text-red-700"
      aria-label="Ponto crítico: resultado fora do padrão esperado"
    >
      Ponto crítico
    </span>
  );
}
