export function RateComparisonBar({
  rate,
  baseline,
  rateLabel = "Taxa",
  baselineLabel = "media",
}: {
  rate: number | null;
  baseline: number | null;
  rateLabel?: string;
  baselineLabel?: string;
}) {
  if (rate == null) {
    return <span className="text-sm text-slate-400">Sem dados</span>;
  }
  const pct = Math.min(Math.max(rate * 100, 0), 100);
  const blPct = baseline != null ? Math.min(Math.max(baseline * 100, 0), 100) : null;

  return (
    <div className="w-full">
      <div className="flex items-center justify-between text-xs text-slate-600 mb-1.5">
        <span>
          <span className="font-semibold text-slate-900">{pct.toFixed(1)}%</span>{" "}
          <span className="text-slate-500">{rateLabel}</span>
        </span>
        {blPct != null && (
          <span className="text-slate-500">
            {baselineLabel} {blPct.toFixed(1)}%
          </span>
        )}
      </div>
      <div className="relative h-2.5 w-full rounded-full bg-slate-100 overflow-hidden">
        <div
          className="absolute inset-y-0 left-0 rounded-full bg-verde-600"
          style={{ width: `${pct}%` }}
        />
        {blPct != null && (
          <div
            className="absolute inset-y-0 w-0.5 bg-slate-500"
            style={{ left: `${blPct}%` }}
            aria-label={`Baseline: ${blPct.toFixed(1)}%`}
          />
        )}
      </div>
    </div>
  );
}

export function DeltaIndicator({
  value,
  compact = false,
  label,
}: {
  value: number | null;
  compact?: boolean;
  label?: string;
}) {
  if (value == null) {
    return compact ? (
      <span className="text-slate-400 text-xs">—</span>
    ) : null;
  }
  const pp = value * 100;
  const sign = pp > 0 ? "+" : "";
  const text = `${sign}${pp.toFixed(1)}pp`;

  let tone: string;
  if (pp >= 10) {
    tone = "border-red-200 bg-red-50 text-red-700";
  } else if (pp >= 5) {
    tone = "border-amber-200 bg-amber-50 text-amber-700";
  } else if (pp < 0) {
    tone = "border-verde-200 bg-verde-50 text-verde-700";
  } else {
    tone = "border-slate-200 bg-slate-50 text-slate-600";
  }

  const ariaLabel = `Diferenca da media: ${text}`;

  if (compact) {
    return (
      <span
        className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-semibold ${tone}`}
        aria-label={ariaLabel}
      >
        {text}
      </span>
    );
  }

  return (
    <div
      className={`rounded-xl border p-3 ${tone}`}
      aria-label={ariaLabel}
    >
      {label && (
        <p className="text-[11px] font-mono uppercase tracking-[0.18em] opacity-70 mb-1">
          {label}
        </p>
      )}
      <p className="text-lg font-semibold">{text}</p>
    </div>
  );
}

export function ExpandableCard({
  children,
  summary,
  defaultOpen = false,
}: {
  children: React.ReactNode;
  summary: React.ReactNode;
  defaultOpen?: boolean;
}) {
  return (
    <details
      className="group rounded-[28px] border border-slate-200/80 bg-white/95 shadow-[0_20px_70px_rgba(15,23,42,0.08)] overflow-hidden"
      open={defaultOpen || undefined}
    >
      <summary className="flex cursor-pointer list-none items-center gap-4 p-5 [&::-webkit-details-marker]:hidden">
        {summary}
        <svg
          className="ml-auto h-5 w-5 shrink-0 text-slate-400 transition-transform group-open:rotate-180"
          fill="none"
          viewBox="0 0 24 24"
          strokeWidth={2}
          stroke="currentColor"
          aria-hidden="true"
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </summary>
      <div className="border-t border-slate-100 p-5">
        {children}
      </div>
    </details>
  );
}

export function CardGrid({
  children,
  columns = 2,
}: {
  children: React.ReactNode;
  columns?: 1 | 2;
}) {
  return (
    <div
      className={`grid gap-4 ${
        columns === 2 ? "xl:grid-cols-2" : ""
      }`}
    >
      {children}
    </div>
  );
}

export function RedFlagPill({ show }: { show: boolean }) {
  if (!show) return null;
  return (
    <span
      className="inline-flex rounded-full border border-red-300 bg-red-50 px-2.5 py-0.5 text-xs font-semibold text-red-700"
      aria-label="Ponto critico: resultado fora do padrao esperado"
    >
      Ponto critico
    </span>
  );
}
