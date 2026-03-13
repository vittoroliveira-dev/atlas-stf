export type FilterContext = {
  minister?: string;
  period?: string;
  collegiate?: string;
  judgingBody?: string;
  processClass?: string;
};

export function readSearchParam(
  value: string | string[] | undefined,
): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

export function buildFilterQuery(context: FilterContext): string {
  const params = new URLSearchParams();

  if (context.minister) params.set("minister", context.minister);
  if (context.period) params.set("period", context.period);
  if (context.collegiate) params.set("collegiate", context.collegiate);
  if (context.judgingBody) params.set("judging_body", context.judgingBody);
  if (context.processClass) params.set("process_class", context.processClass);

  const query = params.toString();
  return query ? `?${query}` : "";
}

export function buildFilterHref(
  pathname: string,
  context: FilterContext,
): string {
  return `${pathname}${buildFilterQuery(context)}`;
}

export function labelCollegiateFilter(value: string | undefined): string {
  if (value === "colegiado") return "Decisões colegiadas";
  if (value === "monocratico") return "Decisões individuais";
  return "Todas as decisões";
}
