# Frontend SSR-only com Server Components

- **Status:** accepted
- **Data:** 2024-06-01

## Contexto

Dashboard analítico single-user. Dados mudam apenas quando o pipeline roda
(batch, não real-time). Não há necessidade de estado client-side para dados
da API — o servidor já tem acesso direto ao backend.

## Decisão

- 26 async Server Components (páginas SSR)
- Data fetch server-side via `fetchApiJson(cache: "no-store")`
- URL search params para filtros (minister, period, collegiate) — views bookmarkable
- Apenas 3 client components: `charts.tsx`, `corporate-network-graph.tsx`, `error.tsx`
  (interatividade Recharts/D3 que requer browser APIs)
- Padrão de dados: `lib/{feature}-data.ts` com types + fetcher functions

## Consequências

### Positivas

- Zero client fetch code — sem useEffect, sem loading states para dados
- Bundle mínimo — JS enviado ao cliente limitado a charts interativos
- Views bookmarkable — filtros na URL, compartilháveis
- SEO-friendly (embora não seja requisito primário)

### Negativas

- Toda navegação = server render + API call (aceitável para single-user)
- Charts interativos requerem "use client" boundary
- Sem atualizações real-time (irrelevante para dados batch)

## Evidência no código

- `web/src/app/*/page.tsx` — 26 Server Components async
- `web/src/lib/api-client.ts` — `fetchApiJson` com `cache: "no-store"`
- `web/src/components/dashboard/charts.tsx` — único chart client component
