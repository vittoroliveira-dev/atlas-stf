# Definição de casos comparáveis

## Objetivo

Estabelecer critérios formais para agrupar casos de modo metodologicamente defensável.

## Princípio central

Dois casos não são comparáveis apenas porque contêm palavras parecidas ou envolvem o mesmo ministro. Comparabilidade exige proximidade processual, temática e decisória suficiente.

## Critérios mínimos do estágio atual

No código atual (`comparison-group-v1`), o grupo comparável é construído com os seguintes critérios mínimos:

- `process_class`
- `thematic_key` derivado de `subjects_normalized` com fallback para `branch_of_law`
- `decision_type`
- `is_collegiate`
- `decision_year`

## Critérios ainda não incorporados à chave atual

- texto integral da decisão
- fase processual inferida
- procedência textual detalhada
- embeddings ou similaridade semântica
- evento externo contextual

## Regras metodológicas

1. Grupos muito amplos geram falso positivo.
2. Grupos muito estreitos perdem potência analítica.
3. A definição do grupo deve ser versionada.
4. O sistema deve permitir revisão da composição do grupo.
5. Sempre que possível, devem existir amostras humanas de validação.

## Regras de validade atualmente implementadas

- grupo com menos de `5` casos: `insufficient_cases`
- grupo com mais de `5000` casos: `too_broad`
- somente grupos `valid` seguem para baseline e alertas

## Política de composição de grupo

Cada grupo materializado deve registrar:

- `comparison_group_id`
- `rule_version`
- `selection_criteria`
- `time_window`
- `case_count`
- `baseline_notes`
- `status`
- `blocked_reason`

## Exemplos de grupo comparável adequado

`mesma classe processual + mesmo tema normalizado + mesmo tipo de decisão + mesma natureza colegiada/monocrática + mesmo ano`

## Exemplos de grupo comparável inadequado

- todas as decisões do ministro
- todos os casos do tribunal
- todos os processos com o mesmo advogado
- todos os casos que contêm a mesma palavra

## Validação

Todo grupo comparável usado em alerta deve:

- poder ser explicado em linguagem simples;
- apontar sua `rule_version`;
- registrar `case_count`, `status` e `blocked_reason`, quando houver;
- manter coerência com os limites `MIN_CASE_COUNT=5` e `MAX_CASE_COUNT=5000` enquanto a regra vigente for `comparison-group-v1`.
