# Prompt — normalization — TEMPLATE

## Como usar este modelo

Copie este arquivo para um prompt de normalização específico. O texto final deve refletir apenas a regra real em discussão.

## Nome do prompt

Defina um nome curto e descritivo.

## Objetivo

Descreva qual regra, política ou proposta de normalização será analisada.

## Quando usar

Usar quando a tarefa envolver padronização, nomenclatura, transformação controlada ou definição de regra de normalização.

## Entradas disponíveis

- dicionário de dados
- camada raw
- camada staging
- decisões metodológicas já registradas

## Saída esperada

Registre documento final, formato e regra ou política esperada.

## Restrições obrigatórias

- preservar valor bruto
- documentar regra de transformação
- marcar ambiguidade
- não consolidar de forma irreversível

## Critérios de validação

- valor bruto preservado
- regra explicitada
- impacto da transformação explicado
- riscos registrados

## Prompt

Objetivo:
Descreva a transformação ou regra sob análise.

Entradas:
Liste os dados e documentos autorizados.

Saída esperada:
Descreva o artefato final e sua finalidade operacional.

Restrições:
1. Preserve sempre o valor bruto.
2. Toda regra deve ser documentada.
3. Não trate como certo o que for apenas provável.
4. Não apague variantes úteis para auditoria.
5. Se houver ambiguidade, registre-a explicitamente.

Validação:
Explique como verificar se a regra continua reversível e auditável.
