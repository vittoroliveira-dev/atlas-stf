# Especificação de filtros

## Objetivo

Padronizar filtros da interface analítica.

## Filtros globais

- período inicial
- período final
- ministro
- classe processual
- assunto
- ramo do direito
- órgão julgador
- tipo de decisão
- andamento da decisão
- colegialidade

## Filtros de atores

- parte
- advogado
- polo ativo
- polo passivo

## Filtros de alertas

- tipo de alerta
- score mínimo
- nota de incerteza
- grupo comparável
- janela temporal

## Regras

1. Todo filtro deve operar sobre valor bruto ou valor normalizado identificado.
2. Filtros derivados devem deixar claro qual regra usaram.
3. Quando um filtro usar campo normalizado, o valor bruto deve continuar acessível.
