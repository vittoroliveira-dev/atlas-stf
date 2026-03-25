# Arquitetura — Atlas STF

Análise empírica de padrões decisórios do Supremo Tribunal Federal.
Pipeline batch: raw → staging → curated → analytics → evidence → serving → API → web.

## Fluxo de dados

```
data/raw/transparencia/       10 CSVs brutos (~580MB)
data/raw/{cgu,tse,cvm,rfb}/   fontes externas (opcionais)
data/raw/{stf_portal,agenda,deoab,oab_sp,datajud}/  fontes complementares (opcionais)
        ↓  staging
data/staging/transparencia/    CSVs limpos + _audit.jsonl
        ↓  curated builders
data/curated/                  18 entidades: process, decision_event, subject, party,
                               counsel, links, minister_bio, movement, session_event,
                               lawyer_entity, law_firm_entity, representation_edge,
                               representation_event, source_evidence, entity_identifier,
                               entity_identifier_reconciliation, agenda_event, agenda_coverage
        ↓  analytics builders (35, independentes, paralelizáveis -j6)
data/analytics/                35 artefatos JSONL + summaries JSON
        ↓  evidence builder
data/evidence/                 JSON bundles por alerta
        ↓  serving builder
data/serving/atlas_stf.db      SQLite (48 tabelas, schema v19)
        ↓  FastAPI (uvicorn)
API                            86 GET + 1 POST endpoints
        ↓  Next.js SSR
web/                           Dashboard (26 Server Components)
```

## Módulos do backend (`src/atlas_stf/`)

### Core (zero I/O, stdlib only)

| Módulo | Responsabilidade |
|--------|-----------------|
| `core/` | Lógica de domínio pura: identity, parsers, rules, stats, origin_mapping, tpu, constants, http_stream_safety, zip_safety, fetch_lock, fetch_result, progress |

### Ingestão

| Módulo | Responsabilidade | Depende de |
|--------|-----------------|------------|
| `staging/` | Limpeza CSV com `FileConfig` declarativo por arquivo | core |
| `scraper/` | Jurisprudência STF (Playwright + API) | — |
| `transparencia/` | Portal de transparência STF (Playwright) | — |

### Fontes externas

| Módulo | Responsabilidade | Padrão |
|--------|-----------------|--------|
| `cgu/` | CEIS/CNEP/Leniência (httpx) | config + parser + runner |
| `tse/` | Doações + despesas + órgãos partidários (CSV) | config + parser + runner |
| `cvm/` | Processos sancionadores CVM (CSV) | config + parser + runner |
| `rfb/` | Quadro societário CNPJ (CSV) | config + parser + runner |
| `datajud/` | API DataJud (httpx) | config + client + runner |
| `stf_portal/` | Timeline STF (httpx) | config + parser + runner |
| `agenda/` | Agenda ministerial (GraphQL) | config + client + runner |
| `oab/` | Validação CNA/CNSA | config + providers + runner |
| `oab_sp/` | Sociedades OAB/SP (httpx) | config + parser + runner |
| `deoab/` | Gazette DEOAB (pdftotext) | config + parser + runner |
| `doc_extractor/` | PDFs de representação | config + extractor + runner |

### Processamento

| Módulo | Responsabilidade | Depende de |
|--------|-----------------|------------|
| `curated/` | Entity builders (18 entidades) | core (via common.py) |
| `analytics/` | 35 builders estatísticos independentes | core, curated (identity + I/O helpers) |
| `evidence/` | Bundles de evidência por alerta | curated, analytics |

### Serving e API

| Módulo | Responsabilidade | Depende de |
|--------|-----------------|------------|
| `serving/` | SQLAlchemy models (48 tabelas) + builder JSONL→SQLite | core, curated, analytics |
| `api/` | FastAPI 86 GET + 1 POST, 9 route registrars | serving (queries ORM) |
| `cli/` | Orquestrador de comandos (lazy imports) | todos |
| `fetch/` | Manifesto unificado de downloads (discovery, plan, execute, migrate) | core |
| `contracts/` | Schema inspection e drift analysis por fonte | core |

## Módulos do frontend (`web/src/`)

| Diretório | Responsabilidade |
|-----------|-----------------|
| `app/` | 26 páginas (async Server Components) + 3 subpáginas dinâmicas |
| `lib/` | 14 data files (*-data.ts), api-client, types, mappers, filter-context, ui-copy |
| `components/dashboard/` | Componentes reutilizáveis (AppShell, charts, tables, cards, filters) |

## Fluxos representativos frontend → backend

### 1. Doações eleitorais (`/doacoes`)

```
app/doacoes/page.tsx
  → lib/donations-data.ts: fetchApiJson<PaginatedDonationsResponse>("/donations")
    → api/_routes_risk.py: @app.get("/donations")
      → api/_donations.py: get_donations(session, ...)
        → ORM: ServingDonationMatch
```

### 2. Alertas (`/alertas`)

```
app/alertas/page.tsx
  → lib/dashboard-data.ts: fetchApiJson<AlertListResponse>("/alerts")
    → api/_routes_alerts_cases.py: @app.get("/alerts")
      → api/_service_alerts_cases.py: get_alerts(session, ...)
        → ORM: ServingAlert + ServingCase
```

### 3. Representação processual (`/representacao`)

```
app/representacao/page.tsx
  → lib/representation-data.ts: fetchApiJson<RepresentationSummary>("/representation/summary")
    → api/_routes_representation.py: @app.get("/representation/summary")
      → api/_service_representation.py: get_representation_summary(session, ...)
        → ORM: ServingLawyerEntity + ServingLawFirmEntity
```

## Fontes de dados

| Fonte | Tipo | Obrigatória |
|-------|------|-------------|
| STF Transparência | 10 CSVs (~580MB) | Sim |
| STF Jurisprudência | Scraper Playwright | Sim |
| STF Portal | Timeline httpx | Não |
| STF Agenda | GraphQL API | Não |
| CGU (CEIS/CNEP/Leniência) | CSV download | Não |
| TSE (doações/despesas) | CSV download | Não |
| CVM (sanções) | CSV/ZIP download | Não |
| RFB (CNPJ/sócios) | CSV download | Não |
| DataJud | API Elasticsearch | Não |
| OAB/OAB-SP | HTTP/scraping | Não |
| DEOAB | PDF extraction | Não |

## Dependências entre módulos

```
                    ┌─────────────────────────────┐
                    │           cli/               │
                    │  (orquestrador, lazy imports) │
                    └─────┬───┬───┬───┬───┬───┬───┘
                          │   │   │   │   │   │
    ┌─────────────────────┘   │   │   │   │   └──────────────────┐
    ▼                         ▼   │   ▼   ▼                      ▼
  external/              curated/ │ analytics/              evidence/
  (cgu,tse,cvm,          │       │ │                        │
   rfb,datajud,...)       │       │ │                        │
    │                     │       │ │                        │
    ▼                     ▼       ▼ ▼                        ▼
  fetch/ ──► core/ ◄─────┘       serving/ ◄─────────────────┘
  (stdlib only)                   │
                                  ▼
                                api/
                                  │
                                  ▼
                                web/
```

## Decisões arquiteturais

Documentadas em `docs/adr/`:
- [ADR-001](adr/001-sqlite-serving-database.md) — SQLite materializado como serving DB
- [ADR-002](adr/002-independent-builders-jsonl.md) — Builders independentes com JSONL
- [ADR-003](adr/003-ssr-only-frontend.md) — Frontend SSR-only com Server Components
- [ADR-004](adr/004-get-only-read-only-api.md) — API GET-only read-only (1 exceção POST)
- [ADR-005](adr/005-unified-fetch-manifest.md) — Manifesto unificado de fetch
- [ADR-006](adr/006-graph-review-post-endpoint.md) — Endpoint POST para graph review
