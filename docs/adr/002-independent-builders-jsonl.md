# Builders independentes com artefatos JSONL

- **Status:** accepted
- **Data:** 2024-06-01

## Contexto

O pipeline tem ~50 builders analíticos que precisam de boundaries claros,
paralelismo e execução opcional. Builders dependentes entre si criariam
acoplamento, dificultariam testes e impediriam execução parcial.

## Decisão

Cada builder é um módulo independente com assinatura padrão:

```python
def build_xxx(*, curated_dir, analytics_dir, output_dir, on_progress=None) -> Path
```

- Lê de `data/curated/` (e opcionalmente `data/analytics/` para convergência)
- Escreve 2 artefatos: `{nome}.jsonl` (registros) + `{nome}_summary.json` (metadados)
- Sem estado compartilhado entre builders — comunicam exclusivamente via JSONL
- Idempotentes — re-executar com mesmos inputs produz mesmos outputs

Ordem de dependência mínima: `groups → baseline → alerts → (demais independentes)`.

## Consequências

### Positivas

- Paralelizável — `make analytics -j6` executa 6 builders simultâneos
- Testável isolado — cada builder tem testes próprios com fixtures JSONL
- Novo builder = novo módulo — não altera código existente
- Execução parcial — rodar apenas `compound-risk` sem precisar dos demais

### Negativas

- I/O redundante — múltiplos builders releem mesmos curated (mitigado por SSD)
- Sem streaming inter-builder — cada um serializa/desserializa JSONL completo

## Evidência no código

- `analytics/compound_risk.py` — exemplo de builder com assinatura padrão
- `Makefile` target `analytics` — execução paralela com `-j`
- `.claude/rules/pipeline-contract.md` — contrato formal dos builders
