export function DonationBadge() {
  return (
    <span
      className="inline-flex rounded-full border border-ouro-200 bg-ouro-100 px-2.5 py-0.5 text-xs font-semibold text-ouro-800"
      aria-label="Doador de campanha eleitoral"
    >
      Doador
    </span>
  );
}

export function DonationRedFlagBadge() {
  return (
    <span
      className="inline-flex rounded-full border border-red-300 bg-red-50 px-2.5 py-0.5 text-xs font-semibold text-red-700"
      aria-label="Ponto critico: resultado fora do padrao esperado"
    >
      Ponto critico
    </span>
  );
}
