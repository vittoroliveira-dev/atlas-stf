export function ChartSkeleton() {
  return (
    <section
      aria-hidden="true"
      className="rounded-card border border-slate-200 bg-white p-5 shadow-elevation-1"
    >
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="space-y-2">
          <div className="h-4 w-32 animate-pulse rounded bg-slate-200" />
          <div className="h-3 w-56 animate-pulse rounded bg-slate-100" />
        </div>
      </div>
      <div className="h-72 min-h-[18rem] animate-pulse rounded-inset bg-slate-100" />
    </section>
  );
}
