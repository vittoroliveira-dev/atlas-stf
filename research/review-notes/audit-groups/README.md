# Auditoria estrutural por grupos auditáveis

## Objetivo

Separar o código do repositório em grupos revisáveis, com fronteiras pequenas o suficiente para análise coesa e grandes o suficiente para preservar contexto arquitetural.

## Entradas

- Código em `src/atlas_stf/`, `web/src/` e `tests/`
- Contratos e princípios em `README.md`, `docs/08-governanca-e-auditoria.md` e `pyproject.toml`
- Checklist operacional em `governance/audit-checklists/codigo-grupos-auditaveis.md`

## Saída esperada

Um dossiê por grupo com:

- fronteira do grupo
- evidência revisada
- achados confirmados
- itens `INCERTO`
- critérios de validação aplicados

## Restrições

- Nenhum achado é registrado sem leitura contextual e contraprova.
- Nenhum grupo mistura pipeline interno com superfície pública.
- Nenhum ponto não comprovado é tratado como defeito.

## Critérios de validação

- Cada arquivo auditável pertence a um grupo primário.
- Cada grupo informa claramente o que foi revisado.
- Cada achado tem trilha de evidência verificável.

## Riscos ou incertezas

- O repositório é maior que a leitura integral de um único turno. Por isso, os dossiês indicam com precisão os arquivos efetivamente revisados e mantêm `INCERTO` onde a confirmação integral ainda dependeria de rodada adicional.

## Grupos

- `G1-aquisicao-e-entrada-externa.md`: aquisição externa, streaming, ZIPs e scraper
- `G2-nucleo-canonico-de-dominio.md`: identidade, regras e curadoria canônica
- `G3-motor-analitico-e-evidencia.md`: analytics e bundles de evidência
- `G4-orquestracao-validacao-e-persistencia.md`: serving, validação e orquestração
- `G5-superficie-publica-http.md`: FastAPI e contratos HTTP
- `G6-superficie-publica-web.md`: Next.js, consumo de API e links externos

## Estado atual da revisão

- G1: 1 achado confirmado
- G2: nenhum achado confirmado na leitura focal
- G3: nenhum achado confirmado na leitura focal
- G4: nenhum achado confirmado na leitura focal
- G5: 1 achado confirmado
- G6: nenhum achado confirmado na leitura focal
