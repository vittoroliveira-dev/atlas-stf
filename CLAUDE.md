# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Atlas STF: análise empírica de padrões decisórios do Supremo Tribunal Federal.
Data pipeline: **raw → staging → curated → analytics → evidence → serving → API → web**.

## Commands — Backend (Python)

```bash
uv sync                              # install/sync dependencies
uv run pytest                        # all tests (~1707)
uv run pytest tests/staging/ -v      # single test dir
uv run pytest tests/core/test_stats.py::test_chi_square  # single test

uv run ruff check src/ tests/        # lint (line-length=120, rules E/F/I/W)
uv run ruff format src/ tests/       # format
uv run pyright src/                   # type check (strict)
uv run vulture src/ --min-confidence 80  # dead code

make scrape                                        # download decisões + acórdãos do STF (TLS auto)
uv run python -m atlas_stf.staging              # staging pipeline
uv run python -m atlas_stf.staging --file acervo.csv  # single file
uv run python -m atlas_stf.staging --dry-run     # dry run

uv run python -m atlas_stf cgu fetch               # download CEIS/CNEP/Leniência CSVs
uv run python -m atlas_stf cgu build-matches       # build sanction match analytics
uv run python -m atlas_stf cgu build-corporate-links # build sanction → corporate → STF links via RFB bridge
uv run python -m atlas_stf tse fetch               # download TSE donation CSVs
uv run python -m atlas_stf tse fetch-expenses      # download TSE campaign expense CSVs (2002-2024)
uv run python -m atlas_stf tse fetch-party-org     # download TSE party organ finance CSVs (2018+)
uv run python -m atlas_stf tse build-matches       # build donation match analytics
uv run python -m atlas_stf tse build-counterparties # build payment counterparty rollup analytics
uv run python -m atlas_stf tse build-donor-links    # build donor → corporate links (join TSE→RFB)
uv run python -m atlas_stf tse empirical-report    # build donation empirical metrics report
uv run python -m atlas_stf cvm fetch               # download CVM sanction data
uv run python -m atlas_stf cvm build-matches       # build CVM sanction matches
uv run python -m atlas_stf rfb fetch               # download RFB CNPJ partner data (Socios+Empresas+Estabelecimentos+reference tables)
uv run python -m atlas_stf rfb build-groups        # build economic group analytics (Union-Find)
uv run python -m atlas_stf rfb build-network       # build corporate network analytics (enriched with labels+establishments+groups)
uv run python -m atlas_stf analytics counsel-affinity  # build counsel affinity analytics
uv run python -m atlas_stf analytics compound-risk     # build compound risk index
uv run python -m atlas_stf analytics minister-flow     # build minister flow analytics
uv run python -m atlas_stf analytics decision-velocity # build decision velocity analytics
uv run python -m atlas_stf analytics rapporteur-change # build rapporteur change analytics
uv run python -m atlas_stf analytics counsel-network   # build counsel network analytics
uv run python -m atlas_stf analytics procedural-timeline # build procedural timeline analytics
uv run python -m atlas_stf analytics pauta-anomaly       # build pauta anomaly analytics
uv run python -m atlas_stf analytics representation-graph     # build representation graph analytics
uv run python -m atlas_stf analytics representation-recurrence # build recurrence analytics
uv run python -m atlas_stf analytics representation-windows    # build temporal windows analytics
uv run python -m atlas_stf analytics amicus-network            # build amicus network analytics
uv run python -m atlas_stf analytics firm-cluster              # build firm cluster analytics
uv run python -m atlas_stf curate representation               # build lawyer entities, firms, edges, events
uv run python -m atlas_stf oab validate --provider null        # validate OAB numbers
uv run python -m atlas_stf doc-extract run                     # extract representation from PDFs
uv run python -m atlas_stf transparencia fetch            # download CSVs do painel de transparência STF
uv run python -m atlas_stf stf-portal fetch              # fetch timeline from STF portal
uv run python -m atlas_stf agenda fetch                  # fetch ministerial agenda from STF GraphQL API
uv run python -m atlas_stf agenda build-events           # build agenda events with process cross-reference
uv run python -m atlas_stf analytics agenda-exposure     # build agenda exposure scoring
uv run python -m atlas_stf analytics calibrate-match     # run fuzzy matching calibration harness
```

## Commands — Frontend (`web/`)

```bash
cd web && npm ci                      # install deps (deterministic)
npm run dev                           # dev server
npm run build                         # production build
npm run lint                          # ESLint
npm run typecheck                     # next typegen && tsc --noEmit
```

## Commands — Makefile (root)

```bash
# Setup e limpeza
make install   # uv sync (default goal)
make setup     # install + npm ci + playwright install (onboarding completo)
make clean     # remove artefatos de build e cache
make clean-all # clean + dados gerados (preserva data/raw)
make help      # lista todos os targets disponíveis

# Qualidade
make check    # ruff + pyright + vulture (all linters)
make test     # pytest
make ci       # simula CI local completo (check + test + web-ci)
make format   # ruff format
make format-check  # ruff format --check (sem alterar)
make lint-fix # ruff check --fix (auto-fix)

# Pipeline
make pipeline  # scrape → staging → curate → analytics → external → evidence → serving (tudo)
make reproduce # reproduz pipeline a partir de data/raw (sequencial, sem re-download)
make fetch-all # baixa todas as fontes de uma vez (STF + externas + agenda, exceto DataJud)
make scrape    # download decisões + acórdãos do STF (com TLS bypass)
make staging   # staging pipeline
make curate    # curated builders (process, party, counsel, etc.)
make analytics # all analytics builders (-j6 recomendado)
make evidence  # evidence bundles

# Fontes externas
make cgu       # fetch CEIS/CNEP/Leniência + sanction matches + corporate links
make cgu-corporate-links  # sanction → corporate → STF links via RFB bridge
make tse       # fetch TSE donation CSVs + donation matches
make tse-expenses      # fetch TSE campaign expense CSVs (2002-2024)
make tse-counterparties # build payment counterparty rollup analytics
make tse-donor-links   # build donor → corporate links (join TSE→RFB)
make tse-empirical-report  # build donation empirical metrics report
make cvm       # fetch CVM sanction data + build matches
make rfb       # fetch RFB CNPJ data + economic groups + corporate network
make stf-portal    # fetch timeline from STF portal
make agenda        # fetch agenda + build events + agenda exposure
make agenda-fetch  # fetch ministerial agenda from STF GraphQL
make agenda-build  # build agenda events with process cross-reference
make transparencia-fetch  # baixa CSVs do painel STF

# Serving e servidores
make serving-build # materializa SQLite para API
make serve-api     # sobe API (FastAPI + Uvicorn)
make web-dev       # dev server frontend
make web-build     # production build do frontend
make web-typecheck # typecheck do frontend
make web-ci        # CI frontend (npm ci + lint + typecheck + build)
make docker-build  # docker compose build
make docker-up     # docker compose up --build
```

## Architecture

### Data Flow

```
data/raw/transparencia/     10 CSVs brutos (~580MB)
        ↓  staging
data/staging/transparencia/  CSVs limpos + _audit.jsonl
        ↓  curated builders
data/curated/                process, decision_event, subject, party, counsel, links, minister_bio,
                             movement, session_event, lawyer_entity, law_firm_entity,
                             representation_edge, representation_event, source_evidence,
                             agenda_event, agenda_coverage
        ↓  analytics builders
data/analytics/              comparison_group, baseline, outlier_alert,
                             rapporteur_profile, assignment_audit, sequential_analysis,
                             temporal_analysis, ml_outlier_score, minister_flow,
                             sanction_match, sanction_corporate_link, donation_match,
                             corporate_network, counsel_affinity, compound_risk,
                             decision_velocity, rapporteur_change, counsel_network,
                             procedural_timeline, pauta_anomaly, economic_group,
                             representation_graph, representation_recurrence,
                             representation_windows, amicus_network, firm_cluster,
                             agenda_exposure, donation_event, payment_counterparty,
                             campaign_expenses_raw, donor_corporate_link,
                             donation_match_ambiguous, donation_empirical_metrics,
                             match_calibration_summary, match_calibration_review
data/reference/              TPU tables (847 classes, 957 movements, 5598 subjects)
data/raw/stf_portal/         timeline data from STF portal (optional)
data/raw/datajud/            agregacoes da API DataJud (opcional)
data/raw/cgu/                CEIS/CNEP/Leniência CSVs (opcional)
data/raw/tse/                doações eleitorais + despesas de campanha + finanças órgãos partidários TSE (opcional)
data/raw/cvm/                processos sancionadores CVM (opcional)
data/raw/rfb/                quadro societário RFB (opcional)
data/raw/agenda/             agenda ministerial STF GraphQL (opcional)
        ↓  evidence builder
data/evidence/               JSON bundles
        ↓  serving builder
data/serving/atlas_stf.db   SQLite (41 tables)
        ↓  FastAPI
API (uvicorn)               73 endpoints
        ↓  Next.js SSR
web/                         Dashboard (React 19 + Recharts)
```

### Source Layout (`src/atlas_stf/`)

- **`core/`** — Pure domain logic, zero I/O: `identity.py`, `parsers.py`, `rules.py`, `stats.py`, `origin_mapping.py`, `tpu.py`
- **`staging/`** — CSV cleaning pipeline with declarative `FileConfig` per file
- **`scraper/`** — Jurisprudência scraper (Playwright + API STF)
- **`stf_portal/`** — STF portal timeline extractor (httpx): `_config.py`, `_checkpoint.py`, `_parser.py`, `_extractor.py`, `_runner.py`
- **`curated/`** — Entity builders (process, decision_event, subject, party, counsel, links, minister_bio, movement, session_event, build_representation + _lawyers/_firms/_edges, build_agenda)
- **`analytics/`** — Statistical builders (groups, baseline, alerts, rapporteur_profile, assignment_audit, sequential, temporal_analysis, ml_outlier, score, minister_flow, origin_context, sanction_match, sanction_corporate_link, donation_match, donation_empirical, donor_corporate_link, match_calibration, corporate_network, counsel_affinity, compound_risk, decision_velocity, rapporteur_change, counsel_network, procedural_timeline, pauta_anomaly, economic_group, representation_graph, representation_recurrence, representation_windows, amicus_network, firm_cluster, agenda_exposure, `_corporate_enrichment.py`, `_donor_identity.py`, `_match_helpers.py`, `_temporal_*.py`)
- **`evidence/`** — Evidence bundle builder for alerts
- **`cgu/`** — CGU CEIS/CNEP/Leniência client (httpx): `_client.py`, `_config.py`, `_runner.py`
- **`tse/`** — TSE donation + expense + party org + donor links (CSV): `_config.py`, `_parser.py`, `_runner.py`, `_parser_expenses.py`, `_runner_expenses.py`, `_parser_party_org.py`, `_runner_party_org.py`, `_resource_classifier.py`
- **`cvm/`** — CVM sanction data (CSV): `_config.py`, `_parser.py`, `_runner.py`
- **`rfb/`** — RFB CNPJ partner data (CSV): `_config.py`, `_parser.py`, `_parser_estabelecimentos.py`, `_reference.py`, `_enrichment.py`, `_runner.py`, `_runner_fetch.py`
- **`datajud/`** — DataJud API client (httpx): `_client.py`, `_config.py`, `_queries.py`, `_runner.py`
- **`oab/`** — OAB CNA/CNSA validation with provider pattern: `_config.py`, `_providers.py`, `_runner.py`
- **`doc_extractor/`** — Selective PDF extraction for representation enrichment: `_config.py`, `_extractor.py`, `_parser.py`, `_runner.py`
- **`agenda/`** — Ministerial agenda fetcher from STF GraphQL API: `_config.py`, `_client.py`, `_parser.py`, `_runner.py`
- **`transparencia/`** — STF transparency portal CSV scraper (Playwright): `_config.py`, `_runner.py`
- **`serving/`** — SQLAlchemy models (41 tables) + SQLite builder (`builder.py`, `_builder_flow.py`, `_builder_loaders.py`, `_builder_loaders_analytics.py`, `_builder_loaders_corporate.py`, `_builder_loaders_representation.py`, `_builder_loaders_timeline.py`, `_builder_schema.py`, `_builder_utils.py`, `models.py`, `_models_analytics.py`, `_models_representation.py`, `_models_timeline.py`)
- **`api/`** — FastAPI (73 endpoints, schema v16): `app.py` (router), `schemas.py`/`_schemas_*.py` (Pydantic), `service.py`/`_service_*.py` (queries), `_routes_*.py` (endpoints), `_routes_representation.py`, `_schemas_representation.py`, `_service_representation.py`, `_routes_timeline.py`, `_schemas_timeline.py`, `_service_timeline.py`, `_filters.py`, `_formatters.py`, `_aggregation.py`, `_sanctions.py`, `_sanction_corporate_links.py`, `_donations.py`, `_payment_counterparties.py`, `_corporate_network.py`, `_economic_groups.py`, `_counsel_affinity.py`, `_compound_risk.py`, `_temporal_analysis.py`, `temporal_schemas.py`, `_decision_velocity.py`, `_rapporteur_change.py`, `_counsel_network.py`, `_schemas_velocity.py`
- **`cli/`** — Package: `__init__.py`, `_parsers.py`/`_parsers_*.py`, `_handlers.py`/`_handlers_*.py`

### Frontend (`web/src/`)

- Next.js 16 App Router, React 19, TypeScript, Tailwind 4, Recharts
- `lib/api-client.ts` — SSR fetch wrapper (`ATLAS_STF_API_BASE_URL` env var, default `http://127.0.0.1:8000`)
- `lib/dashboard-data.ts` — API types and data fetching functions
- `lib/filter-context.ts` — URL search param helpers for filters (minister, period, collegiate)
- `lib/ui-copy.ts` — Human-readable labels in Portuguese
- `components/dashboard/` — Reusable components (AppShell, charts, tables, filters)
- `lib/dashboard-types.ts` — Shared TypeScript types for dashboard
- `lib/dashboard-mappers.ts` — Data transformation helpers
- `lib/decision-velocity-data.ts` — Decision velocity API types and fetching
- `lib/rapporteur-change-data.ts` — Rapporteur change API types and fetching
- `lib/counsel-network-data.ts` — Counsel network API types and fetching
- `lib/representation-data.ts` — Representation network API types and fetching
- Pages: `/` (dashboard), `/alertas`, `/caso`, `/caso/[id]`, `/ministros`, `/ministros/[minister]`, `/advogados`, `/advogados/[id]`, `/partes`, `/partes/[id]`, `/auditoria`, `/sancoes`, `/doacoes`, `/vinculos`, `/afinidade`, `/origem`, `/temporal`, `/convergencia`, `/velocidade`, `/redistribuicao`, `/rede-advogados`, `/representacao`, `/representacao/advogados/[id]`, `/representacao/escritorios/[id]`, `/agenda`, `/agenda/ministro/[slug]`
- All pages are async Server Components (SSR, no client state)

### Key Design Patterns

- **Builders are independent** — each reads from `data/curated/`, writes to `data/analytics/`, no inter-dependencies
- **Serving builder loads optionally** — checks `if path.exists()` before loading each artifact type
- **JSON in SQLAlchemy** — dicts/lists stored as `*_json` Text columns, deserialized with `json.loads()` in service layer
- **Core re-exports** — `curated/common.py` and `analytics/group_rules.py` re-export from `core/` for backward compat
- **CLI lazy imports** — `__init__.py` imports `_parsers` and `_handlers` inside `main()` to avoid circular deps

## Conventions

- Communication in Portuguese; code identifiers in English
- Python 3.14, managed with `uv`, build system: hatchling
- Source layout: `src/atlas_stf/` (hatch wheel)
- Tests mirror source: `tests/{core,staging,scraper,curated,analytics,cli,api,cgu,tse,cvm,rfb,datajud,stf_portal,oab,doc_extractor,agenda,smoke}/`
- Coverage threshold: 83% (`pyproject.toml`)
- Files should stay under 500 lines

## Gotchas

### Pandas 3.x

- `pd.Timestamp.strptime()` NOT implemented — use `datetime.strptime()`
- `select_dtypes(include="object")` deprecated — use `include=["object", "str"]`
- `low_memory` not supported with `engine="python"` — omit it
- numpy `int64` not JSON-serializable — use custom default handler

### Pyright + Pandas

- `df[col]` returns `Series | DataFrame` — use `df.loc[:, col]` for `Series`
- `.sum()`, `.max()` return ambiguous type — use `.sum().item()` for scalar
- `numbers.Number` doesn't satisfy `SupportsInt` — use `hasattr(obj, "__int__")`

### Pyright + SQLAlchemy

- `session.scalars(stmt).all()` returns `Sequence[Unknown]` — cast to `list[Model]`
- Heterogeneous dicts (`set | int`) cause issues — separate by type

### Frontend

- `eslint-plugin-react` foi removido — lint usa `@next/eslint-plugin-next` + `typescript-eslint` direto (ESLint 10 OK)
- `npm run typecheck` requires `next typegen` first (generates route types)

## Auto Memory

Persistent memory at `.claude/memory/` with detailed notes on staging, architecture, analytics, and scraper internals.
