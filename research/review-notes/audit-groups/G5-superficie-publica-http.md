# G5 — Superfície pública HTTP

## Objetivo

Revisar a API FastAPI exposta publicamente, seus filtros, limites operacionais e a consistência entre contratos e implementação.

## Entradas

- `src/atlas_stf/api/app.py`
- `src/atlas_stf/api/_routes_core.py`
- `src/atlas_stf/api/_routes_risk.py`
- `src/atlas_stf/api/_filters.py`
- `src/atlas_stf/api/_service_alerts_cases.py`
- `src/atlas_stf/api/_service_entities.py`
- `src/atlas_stf/api/_service_flow.py`
- `tests/api/test_app.py`

## Saída esperada

Confirmar se a superfície HTTP valida inputs, limita paginação e aplica controles de proteção de forma efetiva.

## Restrições

- Não marcar ausência de autenticação como defeito sem prova de que a rota deva ser restrita.
- Não tratar uso de SQLAlchemy parametrizado como injeção sem interpolação real.

## Critérios de validação

- Inputs públicos usam `Query(...)` com limites ou padrões onde aplicável.
- Filtros textuais escapam curingas.
- As rotas usam consultas ORM/SQLAlchemy sem concatenação de SQL.
- Rate limiting e timeout realmente protegem a superfície.

## Riscos ou incertezas

- A revisão não cobriu benchmark de carga.
- Não houve inspeção de reverse proxy externo; o achado abaixo considera a aplicação isolada ou atrás de proxy sem saneamento de cabeçalho.

## Evidência revisada

- `api/_filters.py` escapa `%` e `_` em filtro textual.
- `api/_routes_core.py` e `api/_routes_risk.py` limitam paginação e alguns parâmetros por `Query`.
- `tests/api/test_app.py:295-319` cobre o caminho nominal do rate limit, CORS e timeout.
- Reproduções locais feitas nesta revisão:
  - três requisições para `/openapi.json` com mesmo `X-Forwarded-For` resultaram em `[200, 200, 429]`;
  - três requisições com `X-Forwarded-For` trocado a cada chamada resultaram em `[200, 200, 200]`.

## Achados confirmados

- **Médio** — `src/atlas_stf/api/app.py:74-84` usa `X-Forwarded-For` e `X-Real-IP` como identificador de cliente sem qualquer noção de proxy confiável.
  Evidência: o middleware de rate limit usa diretamente `_get_client_identifier` em `src/atlas_stf/api/app.py:124-145`. Os testes existentes validam apenas o cenário sem spoof (`tests/api/test_app.py:295-308`). A reprodução local desta revisão mostrou que a simples troca do cabeçalho a cada chamada evita o `429`.
  Revisão contextual: não há configuração de trusted proxy, assinatura de cabeçalho ou fallback restritivo quando a aplicação recebe esses cabeçalhos de origem arbitrária.
  Impacto: o rate limit pode ser contornado por qualquer cliente capaz de forjar cabeçalhos, reduzindo a eficácia da proteção contra abuso e rajadas.
  Correção sugerida: honrar cabeçalhos encaminhados apenas quando houver proxy confiável explicitamente configurado; no default, usar `request.client.host`.

## Itens `INCERTO`

- Não há achado confirmado de injeção SQL nas rotas revisadas; as consultas lidas usam SQLAlchemy sem concatenação direta.
