import Link from "next/link";

export default function NotFound() {
  return (
    <div className="flex min-h-screen flex-col items-center justify-center gap-4 p-8 text-center">
      <h1 className="text-6xl font-bold text-slate-300">404</h1>
      <p className="text-lg text-slate-600">Página não encontrada</p>
      <Link href="/" className="rounded-lg bg-marinho-600 px-4 py-2 text-sm font-medium text-white hover:bg-marinho-700">
        Voltar ao início
      </Link>
    </div>
  );
}
