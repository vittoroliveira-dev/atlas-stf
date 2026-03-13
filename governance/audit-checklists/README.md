# Checklists de auditoria

## Objetivo

Esta pasta reúne checklists operacionais usados para validar dados, metodologia, alertas, relatórios e revisões humanas no projeto.

O objetivo é reduzir decisões implícitas, aumentar reprodutibilidade e garantir que toda saída relevante passe por critérios mínimos de consistência.

## Princípios

1. Todo checklist deve ser curto, operacional e verificável.
2. Todo checklist deve ser aplicado antes de aceitar uma entrega como válida.
3. O checklist não substitui julgamento técnico, mas reduz falhas básicas.
4. Nenhum alerta sensível deve circular sem auditoria mínima.
5. Sempre que possível, a auditoria deve registrar data, responsável e resultado.

## Estrutura da pasta

```text
audit-checklists/
├── README.md
├── dados-ingestao.md
├── normalizacao.md
├── grupos-comparaveis.md
├── alertas.md
├── revisao-humana.md
└── relatorios.md
```

## Convenção de uso

Cada checklist deve conter:

- objetivo;
- momento de aplicação;
- itens obrigatórios;
- critério de bloqueio;
- registro final.

## Resultado esperado

Ao final do uso de um checklist, deve ser possível classificar a etapa como:

- aprovada
- aprovada com ressalvas
- reprovada
- inconclusiva

## Regra de bloqueio

Se qualquer item crítico falhar, a etapa não deve ser tratada como concluída.

## Observação

Os arquivos desta pasta são modelos operacionais do projeto e podem ser refinados conforme a maturidade da governança, desde que mantenham auditabilidade e critério verificável.
