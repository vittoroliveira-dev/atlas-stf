# Endpoint POST para review de graph scoring

- **Status:** accepted
- **Data:** 2026-03-25

## Contexto

O ADR-004 estabelece a API como GET-only read-only. O módulo graph introduz um
workflow de investigação onde revisores humanos confirmam ou rejeitam scores
gerados automaticamente. Esse feedback precisa ser persistido para calibrar o
modelo e gerar audit trail.

## Decisão

Exceção pontual ao ADR-004: um único endpoint POST em `_routes_graph.py`:

```
POST /graph/review/decision
```

Aceita `{path_id, decision, reviewer_notes}` e persiste em `ServingReviewQueue`.

Restrições:
- Único endpoint de escrita no sistema inteiro
- Não altera dados analíticos — apenas registra decisão humana
- Rate limited como todos os outros endpoints
- Sem autenticação adicional (mesma política da API GET)

## Consequências

### Positivas

- Calibração humana sem sistema externo (planilha, email)
- Audit trail persistido no mesmo SQLite (consultável via API GET)
- Feedback loop: review → re-score → serving rebuild incorpora decisões

### Negativas

- Quebra a regra GET-only — requer disciplina para não proliferar
- Sem autenticação, qualquer cliente pode submeter reviews
- ServingReviewQueue cresce monotonicamente (sem TTL)

## Evidência no código

- `api/_routes_graph.py` — `@app.post("/graph/review/decision")`
- `api/_service_graph_review.py` — lógica de persistência
- `serving/_models_graph.py` — `ServingReviewQueue` model
