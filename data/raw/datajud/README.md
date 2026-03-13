# Dados brutos — CNJ DataJud

## Objetivo

Preservar as agregacoes brutas obtidas da API publica do CNJ DataJud.

## Estrutura

Um arquivo JSON por indice consultado (ex: `api_publica_tjsp.json`).

Cada arquivo contem:
- `index`: nome do indice DataJud
- `tribunal_label`: label legivel do tribunal
- `total_processes`: total de processos no indice
- `top_assuntos`: principais assuntos
- `top_orgaos_julgadores`: principais orgaos julgadores
- `class_distribution`: distribuicao por classe processual

## Regras

1. Nao alterar conteudo bruto.
2. Registrar data da coleta via `_checkpoint.json`.
3. Novas coletas podem sobrescrever arquivos existentes.
