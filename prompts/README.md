# Prompts do projeto

## Objetivo

Organizar os prompts curtos e monotarefa usados pelos agentes de IA do projeto.

## Princípios

1. Um prompt por tarefa.
2. Escopo pequeno e explícito.
3. Saída esperada clara.
4. Restrições explícitas.
5. Validação incluída sempre que possível.

## Estrutura

```text
prompts/
├── system/
├── ingestion/
├── normalization/
├── analytics/
├── review/
└── reporting/
```

## Regra central

Prompts não devem pedir "o projeto inteiro". Devem pedir um artefato pequeno, auditável e reversível.
