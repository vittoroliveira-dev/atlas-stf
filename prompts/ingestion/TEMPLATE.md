# Prompt — ingestion — TEMPLATE

## Como usar este modelo

Copie este arquivo para um prompt de ingestão específico. Substitua cada orientação por conteúdo aderente à fonte ou ao inventário analisado.

## Nome do prompt

Defina um nome curto e funcional.

## Objetivo

Descreva qual inventário, registro de origem ou validação de rastreabilidade será produzido.

## Quando usar

Usar quando a tarefa envolver inventário, origem, cobertura, registro ou rastreabilidade de dados.

## Entradas disponíveis

- arquivos brutos
- links oficiais já validados
- registro de fontes
- dicionário de dados preliminar

## Saída esperada

Registre documento final, formato e escopo.

## Restrições obrigatórias

- não fazer parsing jurídico
- não inferir semântica sem base
- não supor completude
- preservar separação entre origem e transformação

## Critérios de validação

- origem registrada
- cobertura tratada com prudência
- limitações explícitas
- artefato pronto para auditoria

## Prompt

Objetivo:
Descreva a tarefa de ingestão ou inventário a ser executada.

Entradas:
Liste apenas os insumos autorizados.

Saída esperada:
Descreva o artefato final e o grau de detalhe exigido.

Restrições:
1. Não invente cobertura.
2. Não invente significado de colunas.
3. Não escreva código.
4. Registre dúvidas e lacunas como `INCERTO`.
5. Preserve foco em origem, integridade e rastreabilidade.

Validação:
Explique como verificar se a saída ficou auditável e completa para o escopo dado.
