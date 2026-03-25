"use client";

import { AlertTriangle, RotateCcw } from "lucide-react";

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_top,_rgba(14,165,233,0.14),_transparent_40%),linear-gradient(180deg,_#f8fafc_0%,_#e2e8f0_100%)] px-6 py-12 text-slate-950">
      <div className="mx-auto flex min-h-[70vh] max-w-3xl items-center justify-center">
        <section className="w-full rounded-[32px] border border-slate-200/80 bg-white/95 p-8 shadow-[0_30px_90px_rgba(15,23,42,0.12)] backdrop-blur">
          <div className="flex items-start gap-4">
            <div className="rounded-2xl bg-rose-100 p-3 text-rose-700">
              <AlertTriangle className="h-6 w-6" />
            </div>
            <div className="space-y-4">
              <div>
                <p className="font-mono text-xs uppercase tracking-[0.24em] text-slate-500">
                  Atlas STF · falha temporária
                </p>
                <h1 className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">
                  Não foi possível carregar esta página agora
                </h1>
              </div>
              <p className="max-w-2xl text-sm leading-7 text-slate-600">
                O painel depende da API e dos artefatos materializados. Se a API estiver indisponível
                ou a consulta falhar, tente novamente em instantes.
              </p>
              {process.env.NODE_ENV === "development" && error.message ? (
                <p className="rounded-2xl bg-slate-100 px-4 py-3 font-mono text-xs text-slate-600">
                  {error.message}
                </p>
              ) : null}
              <button
                type="button"
                onClick={() => reset()}
                className="inline-flex h-11 items-center gap-2 rounded-2xl bg-verde-700 px-4 text-sm font-semibold text-white transition hover:bg-verde-800"
              >
                <RotateCcw className="h-4 w-4" aria-hidden="true" focusable="false" />
                Tentar novamente
              </button>
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}
