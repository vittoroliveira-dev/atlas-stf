# G2 — Núcleo canônico de domínio

## Objetivo

Revisar a camada determinística que normaliza identidades, aplica regras centrais e constrói registros canônicos usados pelo restante do sistema.

## Entradas

- `src/atlas_stf/core/identity.py`
- `src/atlas_stf/core/stats.py`
- `src/atlas_stf/curated/build_decision_event.py`
- `src/atlas_stf/curated/build_process.py`
- `tests/core/test_stats.py`
- `tests/curated/test_build_decision_event.py`

## Saída esperada

Confirmar se a camada central mantém determinismo, validação mínima e fronteira limpa com fontes suplementares.

## Restrições

- Não escalar hipótese estatística para bug sem contradição observável.
- Não tratar aproximação estatística documentada como defeito por si só.

## Critérios de validação

- Identidade e normalização são determinísticas.
- Builders canônicos não pulam validação de schema.
- Enriquecimento suplementar não substitui dados primários sem rastreabilidade.
- Testes adjacentes cobrem os comportamentos revistos.

## Riscos ou incertezas

- A leitura concentrou-se em identidade, estatística pura e builder de `decision_event`; outras rotinas de curadoria permanecem fora desta rodada focal.

## Evidência revisada

- `stable_id`, `normalize_entity_name` e validações CPF/CNPJ em `core/identity.py` são determinísticos e não dependem de estado externo.
- `core/stats.py` mantém funções puras e tratamento explícito para entradas inválidas.
- `build_decision_event.py` e `build_process.py` fazem enriquecimento de jurisprudência sem eliminar o vínculo com `source_file` e `source_row_number`.
- A suíte adjacente passou: `tests/core/test_stats.py` e `tests/curated/test_build_decision_event.py`.

## Achados confirmados

- Nenhum achado confirmado na leitura focal deste grupo.

## Itens `INCERTO`

- A qualidade semântica das heurísticas de similaridade nominal em `core/identity.py` não foi auditada contra um corpus externo nesta rodada.
