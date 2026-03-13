# G4 — Orquestração, validação e persistência

## Objetivo

Revisar a camada que valida insumos, monta o serving DB e coordena a materialização consumida por API e interface.

## Entradas

- `src/atlas_stf/validate.py`
- `src/atlas_stf/serving/builder.py`
- `tests/test_validate.py`

## Saída esperada

Confirmar se a orquestração exige artefatos existentes, valida estrutura mínima e persiste resultados com ordem coerente.

## Restrições

- Não tratar escrita direta de arquivo como defeito sem provar risco operacional concreto.
- Não inferir incompatibilidade de schema sem evidência de ruptura.

## Critérios de validação

- O builder falha quando artefatos obrigatórios faltam.
- A validação estrutural resolve caminhos dentro do diretório esperado.
- A limpeza e reinserção do serving DB seguem ordem compatível com dependências.
- Há teste adjacente cobrindo validação estrutural.

## Riscos ou incertezas

- Não foi auditado o comportamento concorrente do builder em duas execuções simultâneas.
- A revisão não cobriu cada loader chamado pelo builder.

## Evidência revisada

- `validate.py` resolve o nome de arquivo dentro de `input_dir` antes de ler.
- `serving/builder.py` exige presença de artefatos mínimos e chama `_ensure_compatible_schema` antes de materializar.
- A suíte adjacente passou: `tests/test_validate.py`.

## Achados confirmados

- Nenhum achado confirmado na leitura focal deste grupo.

## Itens `INCERTO`

- A escrita de `output_path` em `validate.py` é direta. Não há prova nesta rodada de corrupção observável por interrupção, então o ponto permanece `INCERTO`.
