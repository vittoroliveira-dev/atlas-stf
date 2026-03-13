# G3 — Motor analítico e evidência

## Objetivo

Revisar a camada que combina sinais analíticos e monta bundles de evidência, verificando rastreabilidade e coerência entre artefatos.

## Entradas

- `src/atlas_stf/analytics/compound_risk.py`
- `src/atlas_stf/evidence/build_bundle.py`
- `tests/analytics/test_compound_risk.py`
- `tests/evidence/test_build_bundle.py`

## Saída esperada

Confirmar se agregações e bundles preservam referência a inputs materializados e falham de forma explícita quando insumos obrigatórios faltam.

## Restrições

- Não concluir defeito analítico apenas por complexidade do score.
- Não confundir decisão metodológica com vulnerabilidade técnica.

## Critérios de validação

- O analytics lê apenas artefatos materializados e identificáveis.
- O bundle falha quando chave obrigatória ou arquivo exigido está ausente.
- Há validação de schema antes de persistir bundle.
- Há testes adjacentes cobrindo build e serialização.

## Riscos ou incertezas

- Não foi feita auditoria matemática completa do score composto.
- Não houve reprocessamento em base real nesta rodada.

## Evidência revisada

- `compound_risk.py` depende de artefatos materializados e constrói evidência por pares com IDs estáveis.
- `build_bundle.py` valida `alert`, `baseline`, `comparison_group`, `decision_event` e `process` antes de escrever bundle.
- A suíte adjacente passou: `tests/analytics/test_compound_risk.py` e `tests/evidence/test_build_bundle.py`.

## Achados confirmados

- Nenhum achado confirmado na leitura focal deste grupo.

## Itens `INCERTO`

- O custo de memória de `_read_jsonl_map` em bases muito grandes não foi medido nesta revisão; sem medição, não há achado confirmado de DoS ou exaustão.
