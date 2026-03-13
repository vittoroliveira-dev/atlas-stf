import { Search } from "lucide-react";

export function FilterBar({
  ministers,
  periods,
  judgingBodies = [],
  processClasses = [],
  selectedMinister,
  selectedPeriod,
  selectedCollegiate,
  selectedJudgingBody,
  selectedProcessClass,
  action = "/",
}: {
  ministers: string[];
  periods: string[];
  judgingBodies?: string[];
  processClasses?: string[];
  selectedMinister: string;
  selectedPeriod: string;
  selectedCollegiate: string;
  selectedJudgingBody?: string;
  selectedProcessClass?: string;
  action?: string;
}) {
  const hasAdvancedFilters = judgingBodies.length > 0 || processClasses.length > 0;

  return (
    <form
      action={action}
      className={`grid gap-4 rounded-[30px] border border-slate-200/80 bg-white/90 p-5 shadow-[0_20px_70px_rgba(15,23,42,0.08)] backdrop-blur md:items-end ${
        hasAdvancedFilters
          ? "md:grid-cols-[1.1fr_0.8fr_0.8fr_0.9fr_0.9fr_auto]"
          : "md:grid-cols-[1.2fr_0.8fr_0.8fr_auto]"
      }`}
    >
      <div className={`md:col-span-full ${hasAdvancedFilters ? "" : ""}`}>
        <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">Refine esta visão</p>
        <p className="mt-2 text-sm leading-6 text-slate-600">
          Escolha quem, quando e em qual tipo de decisão você quer concentrar a leitura.
        </p>
      </div>
      <label className="grid gap-2 text-sm text-slate-600">
        Ministro analisado
        <select
          name="minister"
          defaultValue={selectedMinister}
          className="h-12 rounded-2xl border border-slate-200 bg-slate-50 px-4 text-base text-slate-900 outline-none transition focus:border-verde-600 focus:bg-white"
        >
          {ministers.map((minister) => (
            <option key={minister} value={minister}>
              {minister}
            </option>
          ))}
        </select>
      </label>

      <label className="grid gap-2 text-sm text-slate-600">
        Período analisado
        <select
          name="period"
          defaultValue={selectedPeriod}
          className="h-12 rounded-2xl border border-slate-200 bg-slate-50 px-4 text-base text-slate-900 outline-none transition focus:border-verde-600 focus:bg-white"
        >
          <option value="__all__">Todos os períodos</option>
          {periods.map((period) => (
            <option key={period} value={period}>
              {period}
            </option>
          ))}
        </select>
      </label>

      <label className="grid gap-2 text-sm text-slate-600">
        Tipo de decisão
        <select
          name="collegiate"
          defaultValue={selectedCollegiate}
          className="h-12 rounded-2xl border border-slate-200 bg-slate-50 px-4 text-base text-slate-900 outline-none transition focus:border-verde-600 focus:bg-white"
        >
          <option value="all">Todas as decisões</option>
          <option value="colegiado">Somente decisões colegiadas</option>
          <option value="monocratico">Somente decisões individuais</option>
        </select>
      </label>

      {judgingBodies.length > 0 ? (
        <label className="grid gap-2 text-sm text-slate-600">
          Onde foi decidido
          <select
            name="judging_body"
            defaultValue={selectedJudgingBody ?? ""}
            className="h-12 rounded-2xl border border-slate-200 bg-slate-50 px-4 text-base text-slate-900 outline-none transition focus:border-verde-600 focus:bg-white"
          >
            <option value="">Todos os contextos</option>
            {judgingBodies.map((judgingBody) => (
              <option key={judgingBody} value={judgingBody}>
                {judgingBody}
              </option>
            ))}
          </select>
        </label>
      ) : null}

      {processClasses.length > 0 ? (
        <label className="grid gap-2 text-sm text-slate-600">
          Tipo de ação
          <select
            name="process_class"
            defaultValue={selectedProcessClass ?? ""}
            className="h-12 rounded-2xl border border-slate-200 bg-slate-50 px-4 text-base text-slate-900 outline-none transition focus:border-verde-600 focus:bg-white"
          >
            <option value="">Todos os tipos</option>
            {processClasses.map((processClass) => (
              <option key={processClass} value={processClass}>
                {processClass}
              </option>
            ))}
          </select>
        </label>
      ) : null}

      <button
        type="submit"
        className="inline-flex h-12 cursor-pointer items-center justify-center gap-2 rounded-2xl bg-slate-950 px-5 text-sm font-semibold text-white transition duration-200 hover:bg-slate-800 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-verde-600"
      >
        <Search className="h-4 w-4" aria-hidden="true" focusable="false" />
        Atualizar resultados
      </button>
    </form>
  );
}
