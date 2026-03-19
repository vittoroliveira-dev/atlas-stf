# API GET-only read-only

- **Status:** accepted
- **Data:** 2024-06-01

## Contexto

A API serve analytics pré-computados de um SQLite materializado. Não há input
de usuário que modifique dados — o pipeline é a única fonte de escrita.
Os dados são de tribunal público (STF).

## Decisão

- 73 endpoints GET-only — nenhum POST/PUT/DELETE
- CORS permite apenas método GET (`allow_methods=["GET"]`)
- Rate limiting in-memory, fail-closed (429) — configurável via env vars
- Sem autenticação — dados públicos de tribunal
- Request timeout (30s default) com 504 em timeout
- Security headers: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`

## Consequências

### Positivas

- Superfície de segurança simplificada — sem mutação, sem auth state
- Cacheável em toda camada (CDN, proxy, browser)
- Rate limiting simples — não precisa de auth token para identificar abuso

### Negativas

- Sem anotações de usuário via API (fora do escopo)
- CORS restritivo pode exigir configuração para novos frontends

## Evidência no código

- `api/app.py` — CORS config, rate limiter, security headers, timeout middleware
- `api/_routes_*.py` — 8 route registrars, todos `@app.get()`
