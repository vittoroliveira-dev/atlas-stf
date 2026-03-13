# Baselines

## Objetivo

Documentar os baselines usados pelo projeto, incluindo definição, racional, composição e limitações.

## Função

Um baseline representa o padrão esperado dentro de um grupo comparável. Ele é a referência contra a qual a atipicidade é medida.

## Regras

1. Todo baseline deve ter identificador.
2. Todo baseline deve apontar seu grupo comparável.
3. Todo baseline deve ter janela temporal definida.
4. Toda alteração relevante deve gerar nova versão.
5. Todo baseline deve registrar limites e risco de falso positivo.

## Registro mínimo

- baseline_id
- versão
- grupo comparável
- período
- variáveis usadas
- descrição do padrão esperado
- limitações
- observações
