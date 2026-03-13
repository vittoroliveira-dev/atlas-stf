# G6 — Superfície pública web

## Objetivo

Revisar a camada Next.js que consome a API, monta links e expõe o recorte auditável ao usuário final.

## Entradas

- `web/src/lib/api-client.ts`
- `web/src/lib/safe-external-url.ts`
- `web/src/lib/filter-context.ts`
- `web/src/lib/dashboard-data.ts`
- `web/src/app/caso/[decisionEventId]/page.tsx`
- `web/src/components/dashboard/case-table.tsx`

## Saída esperada

Confirmar se a camada web preserva contratos da API, codifica parâmetros e evita exposição insegura de links externos.

## Restrições

- Não concluir XSS sem sink real.
- Não tratar link externo como inseguro quando houver filtro explícito de protocolo e encoding.

## Critérios de validação

- O client de API monta URL com `URL` e `searchParams`.
- Parâmetros vindos de rota ou busca são codificados antes de uso em paths.
- Links externos só aceitam `http:` e `https:`.
- Erros 404 esperados são tratados sem vazar stack ou detalhe interno.

## Riscos ou incertezas

- Não houve teste E2E do dashboard nesta rodada.
- A revisão focou rotas e utilitários ligados ao detalhe de caso e listagens.

## Evidência revisada

- `web/src/lib/api-client.ts` usa `URL` e `searchParams` para montar chamadas.
- `web/src/lib/filter-context.ts` centraliza construção de query string sem concatenação manual de valores.
- `web/src/lib/safe-external-url.ts` rejeita protocolos fora de `http` e `https`.
- `web/src/app/caso/[decisionEventId]/page.tsx` e `web/src/components/dashboard/case-table.tsx` usam `encodeURIComponent` ou helper seguro antes de gerar links.
- `web/src/lib/dashboard-data.ts` trata `404` esperado para detalhe e ML outlier sem expor erro cru.

## Achados confirmados

- Nenhum achado confirmado na leitura focal deste grupo.

## Itens `INCERTO`

- Não foi feita verificação visual ou E2E sobre todos os caminhos de erro do App Router; portanto a cobertura UX completa permanece `INCERTO`.
