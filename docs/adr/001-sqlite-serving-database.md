# SQLite como banco de serving materializado

- **Status:** accepted
- **Data:** 2024-06-01

## Contexto

A API precisa de um banco para 73 endpoints com filtering, pagination e joins.
Os dados são reconstruídos do zero pelo pipeline (batch) — não há input de usuário
que modifique dados. A infraestrutura deve ser mínima (single-user, sem DBA).

## Decisão

Usar SQLite materializado a partir de artefatos JSONL. O serving builder lê
`data/curated/` e `data/analytics/`, popula 41 tabelas SQLAlchemy e grava em
`data/serving/atlas_stf.db`.

- **Sem migrations:** schema incompatível → `drop_all()` + `create_all()` automático.
- **Schema versionado:** `SERVING_SCHEMA_VERSION` (v16) + fingerprint SHA-256 das
  tabelas/colunas/índices. Build falha cedo se o schema mudou sem bump de versão.
- **Carregamento opcional:** cada artefato analytics é carregado com `if path.exists()`,
  permitindo builds parciais sem erro.

## Consequências

### Positivas

- Zero infraestrutura — arquivo único, backup trivial (`cp`)
- Builds determinísticos — mesmo input → mesmo DB
- Deploy atômico — substituir o arquivo é um deploy
- Testável com SQLite in-memory (`tests/api/conftest.py`)

### Negativas

- Full rebuild em schema change (~2min para ~41 tabelas)
- Sem escrita concorrente (irrelevante: API é read-only)
- Dados ficam stale até re-executar pipeline

## Evidência no código

- `serving/builder.py` — orquestrador `build_serving_database()`
- `serving/_builder_schema.py` — `SERVING_SCHEMA_VERSION`, fingerprint, `_ensure_compatible_schema()`
- `serving/_builder_loaders.py` — loaders com carregamento opcional
