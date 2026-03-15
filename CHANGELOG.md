# Changelog

Todas as alterações relevantes do projeto são documentadas neste arquivo.

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).

## [Unreleased]

## [1.0.4] - 2026-03-15

### Added

- **agenda/**: módulo de agenda ministerial — fetcher da API GraphQL do STF (`noticias.stf.jus.br/graphql`) com ingestão de audiências, sessões e compromissos oficiais; GET/POST fallback, retry, rate limiting, validação de contrato
- **curated/build_agenda.py**: builder de eventos de agenda com cruzamento referencial contra dados processuais (processo, partes, advogados); calcula cobertura por ministro/mês com business_days, recesso, vacation/leave heuristic, publication_gap
- **analytics/agenda_exposure.py**: scoring de proximidade temporal entre eventos de agenda e decisões em janelas 7d/14d/30d/60d, com baseline intra-ministro condicionado por classe+tipo decisório; cap em 0.29 para observações insuficientes (n<5)
- **Taxonomia de agenda**: 4 categorias — `institutional_core`, `institutional_external_actor`, `private_advocacy`, `unclear`; 9 meeting_nature types; regra de precedência para eventos mistos (public+private → unclear, conf max 0.4)
- **Rede de Representação Processual**: subsistema completo de representação com identidade profissional OAB
- **core/identity.py**: `normalize_oab_number`, `is_valid_oab_format`, `normalize_cnsa_number`, `build_lawyer_identity_key` (prioridade OAB > CPF > nome), `build_firm_identity_key` (CNPJ > CNSA > nome), `VALID_UF_CODES` (27 UFs)
- **5 JSON Schemas**: `lawyer_entity`, `law_firm_entity`, `representation_edge`, `representation_event`, `source_evidence` — com enums de `representative_kind` (lawyer|law_firm), `role_type` (7 papéis) e `event_type` (8 tipos)
- **docs/representation_network_contract.md**: contrato semântico com glossário, matriz de confiança, regras de identidade, exclusão ConfirmADV
- **stf_portal/_parser.py**: 3 novos parsers DOM — `parse_partes_representantes_html`, `parse_peticoes_detailed_html`, `parse_oral_argument_html`; `build_process_document` estendido com `representantes`, `peticoes_detailed`, `oral_arguments`
- **curated/build_representation.py**: orquestrador que produz 5 artefatos JSONL (lawyer_entity, law_firm_entity, representation_edge, representation_event, source_evidence)
- **curated/_build_representation_lawyers.py**: builder de advogados com dedup por identity_key (OAB > tax > name), absorve lógica do build_counsel
- **curated/_build_representation_firms.py**: builder de escritórios com confidence "low" para nomes extraídos do portal
- **curated/_build_representation_edges.py**: builder de arestas (representative_entity_id obrigatório), eventos processuais e evidências de proveniência
- **oab/**: módulo de validação OAB com provider pattern resiliente — NullOabProvider, FormatOnlyProvider, CnaProvider (fallback), CnsaProvider (unavailable); `run_oab_validation` lê/atualiza lawyer_entity.jsonl
- **doc_extractor/**: extração seletiva de PDFs com pdfplumber opcional — parsers de procuração, petição, OAB/CNPJ inline; `run_doc_extraction` enriquece arestas de baixa confiança
- **analytics/representation_graph.py**: grafo de representação com agregação de eventos por aresta, span temporal, co-advogados
- **analytics/representation_recurrence.py**: recorrência advogado<->parte com contagem por processo, classes e role_types
- **analytics/representation_windows.py**: presença em janelas processuais (distribuição, pauta, vista, julgamento, publicação)
- **analytics/amicus_network.py**: rede de amicus curiae por classe, tema, ministro
- **analytics/firm_cluster.py**: clusters de escritórios via Union-Find sobre partes compartilhadas
- **serving/_models_representation.py**: 5 novas tabelas SQLAlchemy — ServingLawyerEntity, ServingLawFirmEntity, ServingProcessLawyer, ServingRepresentationEdge, ServingRepresentationEvent
- **serving/_models_agenda.py**: 3 novas tabelas SQLAlchemy — ServingAgendaEvent, ServingAgendaCoverage, ServingAgendaExposure (total: 38 tabelas)
- **serving/_builder_loaders_representation.py**: 5 loaders com fallback process_counsel_link → process_lawyer_link
- **serving/_builder_loaders_agenda.py**: 3 loaders para agenda (events, coverage, exposures)
- **api/_routes_representation.py**: 7 novos endpoints — GET /representation/lawyers, /lawyers/{id}, /firms, /firms/{id}, /process/{id}, /events, /summary
- **api/_routes_agenda.py**: 6 novos endpoints — GET /agenda/events, /events/{id}, /ministers, /ministers/{slug}, /exposures, /summary (total: 68 endpoints)
- **api/_schemas_representation.py**: Pydantic schemas (LawyerEntityItem, LawFirmEntityItem, RepresentationEdgeItem, RepresentationEventItem, PaginatedLawyersResponse, PaginatedFirmsResponse, LawyerDetailResponse, FirmDetailResponse, ProcessRepresentationResponse, RepresentationNetworkSummary)
- **api/_service_representation.py**: query functions (get_lawyers, get_lawyer_detail, get_firms, get_firm_detail, get_process_representation, get_representation_events, get_representation_summary)
- **Frontend**: página /representacao com dual-tab advogados/escritórios, badge de confiança OAB (confirmado/provável/não confirmado), detalhes /representacao/advogados/[id] e /representacao/escritorios/[id], nav item no AppShell
- **web/src/lib/representation-data.ts**: tipos TypeScript e funções de fetch para representação
- **audit_gates.py**: `audit_representation()` com validação de schema, integridade referencial (orphan edges), métricas de cobertura OAB e thresholds
- **CLI**: subcomandos `curate representation`, `oab validate`, `doc-extract run`, `analytics representation-graph`, `analytics representation-recurrence`, `analytics representation-windows`, `analytics amicus-network`, `analytics firm-cluster`, `audit representation`, `agenda fetch`, `agenda build-events`, `analytics agenda-exposure`
- **Makefile**: alvos `curate-representation`, `oab-validate`, `_ag-representation-graph`, `_ag-representation-recurrence`, `_ag-representation-windows`, `_ag-amicus-network`, `_ag-firm-cluster`, `agenda`, `agenda-fetch`, `agenda-build`, `_ag-agenda-exposure`
- **Frontend**: páginas `/agenda` (visão geral) e `/agenda/ministro/[slug]` (detalhe ministerial) com banner de cobertura, stat cards, tabela de ministros, painel de exposures
- **GitHub Packages**: distribuição como pacote Python via GitHub Release assets
- **Workflow de publicação**: `.github/workflows/publish.yml` — build e upload automático em tags `v*`
- **py.typed**: marker PEP 561 para verificação de tipos
- **scripts/probe_stf_portal.py**: script one-shot de probing do portal STF (não entra no pipeline)
- ~224 novos testes (total: ~1260) cobrindo identity OAB, providers, curated builders, parser portal, analytics graph, doc_extractor, agenda client/parser/exposure, serving loaders, API routes, audit gates, smoke tests

### Fixed

- **serving/_builder_loaders_representation.py**: corrigido fallback `event_count` que tratava `0` como falsy — `event_count=0` agora preservado corretamente em vez de cair para `evidence_count`
- **api/_schemas_agenda.py + _service_agenda.py**: campos `contains_public_actor`, `contains_private_actor` e `actor_count` adicionados ao schema Pydantic `AgendaEventItem` e mapeador `_ev_item()` — dados já existiam no modelo serving mas não eram expostos pela API

### Changed

- **serving/_builder_schema.py**: SERVING_SCHEMA_VERSION 6 → 7
- **serving/builder.py**: carrega 8 novas entidades (5 representação + 3 agenda), _total = 37
- **serving/models.py**: re-exporta `_models_representation` e `_models_agenda`
- **pyproject.toml**: metadados completos — authors, license, keywords, classifiers, urls; versão 1.0.4
- **Backend deps**: numpy 2.4.2 → 2.4.3, ruff 0.15.5 → 0.15.6
- **Frontend deps**: ESLint 9 → 10, TypeScript 5.8 → 5.9, Tailwind 4.1 → 4.2, @types/react 19.1 → 19.2, @types/react-dom 19.1 → 19.2, @types/node 25.3 → 25.5
- **Frontend lint**: substituído `eslint-config-next` por `@next/eslint-plugin-next` + `typescript-eslint` — desbloqueia ESLint 10
- **Node.js**: pinado v24.14.0 LTS via `.nvmrc`
- **stf_portal/_extractor.py**: chama os 3 novos parsers e passa resultados ao build_process_document
- **cli/_handlers_data.py**: `curate all` agora inclui build_representation_jsonl (total = 9 passos)
- **cli/_parsers_analytics.py**: 6 novos subparsers de analytics (5 representação + agenda-exposure)
- **cli/_handlers_analytics.py**: 6 novos dispatchers
- **cli/_parsers_external.py**: subparsers oab validate, doc-extract run, agenda fetch, agenda build-events
- **cli/_handlers_external.py**: dispatchers correspondentes
- **web/src/components/dashboard/app-shell.tsx**: +nav items "Representação processual" e "Agenda ministerial"

## [1.0.3] - 2026-03-15

### Added

- **rfb/_reference.py**: Fetch e parse de 5 tabelas de domínio RFB (Qualificações, Naturezas, CNAEs, Municípios, Motivos) — lookup O(1) por código → descrição legível
- **rfb/_parser_estabelecimentos.py**: Parser CSV de Estabelecimentos RFB (delimitador `;`) com filtro por CNPJ, montagem de `cnpj_full` (14 dígitos) e split de CNAEs secundários
- **rfb/_enrichment.py**: Funções puras de enriquecimento — decodifica qualification_code, natureza_jurídica, cnae_fiscal, município e motivo de situação cadastral
- **rfb/_runner_fetch.py**: Lógica de 4 passes extraída do runner (Sócios x2, Empresas, Estabelecimentos) com enriquecimento integrado via tabelas de referência
- **analytics/economic_group.py**: Detecção de grupos econômicos via Union-Find sobre cadeia societária PJ — output `economic_group.jsonl` com member_count, capital social, CNAEs, UFs, flags de ministro/parte/advogado, sem cap artificial
- **serving/_builder_loaders_corporate.py**: Loaders corporativos extraídos de `_builder_loaders_analytics.py` — `load_corporate_conflicts()` (atualizado com 25+ campos novos) e `load_economic_groups()`
- **ServingEconomicGroup**: Nova tabela no serving (total: 30 tabelas) com 14 colunas — grupo econômico materializado para consulta API
- **api/_economic_groups.py**: `GET /economic-groups` (lista paginada com filtros booleanos) e `GET /economic-groups/{group_id}` (detalhe) — total: 55 endpoints
- **Enriquecimento do corporate_network.jsonl**: 25+ campos novos por conflito — labels decodificadas (qualificação, natureza jurídica, CNAE), dados multi-estabelecimento (sede, UFs, CNAEs, até 3 key_establishments), grupo econômico (id, member_count, razões sociais) e proveniência (evidence_type, source_dataset, evidence_strength)
- **Campos opcionais em temporal_analysis**: enrichment de establishment_count, active_establishment_count, headquarters_uf, headquarters_cnae_label, economic_group_id, economic_group_member_count nos registros `corporate_link_timeline`
- **CLI**: subcomando `rfb build-groups` com `--rfb-dir` e `--output-dir`
- **Makefile**: alvo `rfb-groups` com dependência correta (rfb-fetch → rfb-groups → rfb-network)
- **Frontend /vinculos enriquecido**: badges de proveniência (evidence_type + evidence_strength), qualificação decodificada, CNAE da sede, natureza jurídica, status cadastral com motivo, contagem de estabelecimentos, grupo econômico com razões sociais
- 38 novos testes (total: 1036): test_reference (6), test_enrichment (6), test_parser_estabelecimentos (8), test_economic_group (13), test_economic_groups_api (5)

### Changed

- **rfb/_runner.py**: Refatorado — passes 1-4 extraídos para `_runner_fetch.py`, checkpoint atualizado com `completed_estabelecimentos` e `completed_reference`, total_steps=41, cache check inclui `establishments_raw.jsonl`
- **rfb/_config.py**: +2 constantes (`RFB_ESTABELECIMENTOS_FILE_COUNT=10`, `RFB_REFERENCE_TABLES`)
- **serving/_builder_schema.py**: SERVING_SCHEMA_VERSION 5 → 6
- **ServingCorporateConflict**: +25 colunas (labels decodificadas, multi-estabelecimento, grupo econômico, proveniência, taxa substantiva)
- **api/_schemas_risk.py**: `EstablishmentSummary` (9 campos), novos campos em `CorporateConflictItem`, `EconomicGroupItem`, `PaginatedEconomicGroupResponse`
- **pyproject.toml**: filterwarnings corrigido para suprimir DeprecationWarning do fork() no Python 3.14 (pytest-xdist)

## [1.0.2] - 2026-03-15

### Added

- **core/tpu.py**: Camada de normalização TPU (Tabelas Processuais Unificadas) — funções puras para categorização semântica de movimentos (`categorize_movement_text`, `is_redistribution`, `is_pedido_de_vista`, `is_pauta_inclusion` etc.), resolução sigla→código TPU (`normalize_class_sigla_to_tpu`), consulta por código (`tpu_class_name`, `tpu_movement_name`)
- **data/reference/**: Artefatos estáticos TPU (847 classes, 957 movimentos, 5598 assuntos) baixados da API PDPJ/CNJ (`gateway.cloud.pje.jus.br/tpu`) via `scripts/build_tpu_tables.py`
- **stf_portal/**: Módulo extrator de linha do tempo processual do portal do STF — configuração, checkpoint, parser HTML, extrator httpx, orquestrador com priorização e rate limiting (`src/atlas_stf/stf_portal/`)
- **build_movement.py**: Builder curated para andamentos e deslocamentos do portal STF → `movement.jsonl` com categorização TPU fuzzy e auditabilidade (`tpu_match_confidence`, `normalization_method`)
- **build_session_event.py**: Builder curated para eventos de sessão (pauta, vista, julgamento, sessão virtual) → `session_event.jsonl` com `vista_duration_days` calculado e `session_type` detectado
- **Enriquecimento do process.jsonl com o portal STF**: 4 novos campos — `stf_portal_movement_count`, `stf_portal_last_updated`, `prevencao_process_number`, `first_distribution_date`
- **Propagação de campos da jurisprudência**: 4 campos já capturados pelo scraper mas descartados, agora propagados no curated — `juris_publicacao_data`, `juris_acompanhamento_url`, `juris_tese_texto`, `juris_acordao_ata`; `juris_publicacao_data` também propagado por decisão no `decision_event.jsonl`
- **ServingMovement** e **ServingSessionEvent**: 2 novas tabelas no serving (total: 29 tabelas) com campos semânticos indexados (`movement_category`, `session_type`, `event_type`, `movement_date`)
- **`acompanhamento_url`** e **`first_distribution_date`** no ServingCase
- **API de linha do tempo**: `GET /caso/{process_id}/timeline` (movimentos cronológicos) e `GET /caso/{process_id}/sessions` (eventos de sessão) — total: 53 endpoints
- **procedural_timeline.py**: Analytics de janelas temporais precisas — `days_distribution_to_first_decision`, `days_in_vista_total`, `pauta_cycle_count`, `redistribution_count` com comparação entre pares `(process_class, decision_year)` e sinalizadores de risco (vista > P95, ciclo de pauta > P95)
- **pauta_anomaly.py**: Analytics de anomalia de sessão por ministro — frequência de vista (z-score), duração de vista vs. linha de base, retirada de pauta sem re-agendamento em 90 dias
- **CLI**: subcomandos `stf-portal fetch`, `analytics procedural-timeline`, `analytics pauta-anomaly`
- **Makefile**: alvos `stf-portal`, `_ag-procedural-timeline`, `_ag-pauta-anomaly`
- 6 novos esquemas JSON (movement, session_event, procedural_timeline + resumo, pauta_anomaly + resumo)
- 107 novos testes (total: 998)

### Changed

- **Divisão do models.py**: `serving/models.py` (545→140 linhas) dividido em `_models_analytics.py` (419 linhas) e `_models_timeline.py` (37 linhas) — re-exportações mantêm compatibilidade retroativa
- **decision_velocity.py refinado**: usa `first_distribution_date` (portal) em vez de `filing_date` quando disponível; desconta `days_in_vista` para comparação justa; novo campo opcional `days_in_vista_deducted`
- **SERVING_SCHEMA_VERSION**: 3 → 5

## [1.0.1] - 2026-03-14

### Added

- **Signal details no compound_risk**: campo `signal_details` no output de `compound_risk.jsonl` — dict esparso com metadados rastreáveis por tipo de sinal (count, sources, total_brl, affinity_ids, max_score, flagged). Keys == sinais efetivamente presentes no par
- **Dimensão temporal no compound_risk**: campos `earliest_year` e `latest_year` no output de `compound_risk.jsonl` — span temporal dos processos compartilhados por par, calculados a partir de `decision_date`
- **Propagação serving/API**: campos `signal_details_json`, `earliest_year`, `latest_year` em `ServingCompoundRisk`, `CompoundRiskItem` (Pydantic) e tipo TypeScript `CompoundRiskItem`
- **Classificação de materialidade**: nova função `classify_outcome_materiality()` em `core/rules.py` — classifica decisões em `substantive`, `procedural`, `provisional` ou `unknown` com ordem de precedência determinística (provisional > procedural > substantive > unknown)
- **Taxa de êxito substantiva**: nova função `compute_favorable_rate_substantive()` em `_match_helpers.py` — calcula taxa de êxito filtrando apenas decisões de mérito (exclui liminares, desistências, inadmissibilidade e embargos de declaração)
- **Campos substantivos nos analytics**: `favorable_rate_substantive`, `substantive_decision_count` e `red_flag_substantive` adicionados aos outputs de `sanction_match`, `donation_match`, `corporate_network` e `counsel_affinity`
- **Auditabilidade do matching**: campos `match_strategy`, `match_score` e `match_confidence` materializados no serving (`ServingSanctionMatch`, `ServingDonationMatch`), expostos na API e exibidos no frontend como badge de confiança nos cards de sanções e doações
- **Badge de confiança no frontend**: indicador visual nos cards de sanções e doações — "CPF/CNPJ exato", "Nome exato", "Match fuzzy (score)", "Revisão manual necessária" ou "Confiança não determinada"
- **Smoke tests de validação real** (`tests/test_smoke_validation.py`): 39 testes em 3 fases — (A) materialidade + taxa substantiva + builder output, (B) match_strategy/confidence no serving + DB schema + API endpoints, (C) normalização CGU CPF/CNPJ + tipo PF/PJ + datas — sem mocks, fixtures mínimas em tmp_path
- **CGU checkpoint**: download incremental via HEAD request — compara Content-Length e data do servidor antes de baixar. Se o ZIP não mudou, reutiliza o CSV existente em disco. (`src/atlas_stf/cgu/_runner.py`, `src/atlas_stf/cgu/_checkpoint.py`)
- **CVM checkpoint**: download incremental via HEAD request — compara ETag/Content-Length do servidor. Se o ZIP não mudou, pula download e reutiliza `sanctions_raw.jsonl` em cache. (`src/atlas_stf/cvm/_runner.py`)
- **RFB auto-discovery**: se o token NextCloud da RFB expirar (401/403), o sistema tenta descobrir automaticamente o novo token via scraping da página principal. Loga o token mascarado para o usuário persistir na env var. (`src/atlas_stf/rfb/_runner.py`)

### Changed

- **Compound risk substantivo**: `compound_risk.py` agora usa `red_flag_substantive` como autoridade exclusiva quando presente no registro; campo legado `red_flag` só governa para dados antigos ou fontes sem métrica substantiva (ex: `rapporteur_change`). Elimina falsos positivos inflados por liminares
- **CGU normalização de CPF/CNPJ**: `entity_cnpj_cpf` agora contém apenas dígitos (normalizado via `normalize_tax_id()`); valor original preservado em `entity_cnpj_cpf_raw`
- **CGU extração tipo PF/PJ**: novo campo `entity_type_pf_pj` (canonicalizado para "PF"/"PJ") e `entity_type_pf_pj_raw` (valor bruto do CSV) na ingestão CEIS/CNEP/Leniência
- **CGU normalização de datas**: `sanction_start_date` e `sanction_end_date` agora em formato YYYY-MM-DD (parseia DD/MM/YYYY, YYYY-MM-DD, YYYYMMDD); valores originais preservados em `*_raw`; datas inválidas gravadas como `null`

### Fixed

- **CGU encoding**: CSVs do Portal da Transparência (CEIS/CNEP/Leniência) agora são transcodificados de Latin-1 para UTF-8 na extração, eliminando caracteres `�` nos arquivos em disco. Leitura posterior usa fallback (UTF-8 → Latin-1) para compatibilidade com arquivos existentes. (`src/atlas_stf/cgu/_runner.py`)
- **Pipeline**: `make pipeline` agora inclui `scrape` como primeiro passo da cadeia (scrape → staging → curate → analytics → ...), garantindo que os dados do STF sejam baixados antes do processamento. (`Makefile`)
- **Scraper**: mês corrente não é mais marcado como "complete" no checkpoint — ao re-executar, o scraper consulta a API para verificar se há dados novos em vez de pular. (`src/atlas_stf/scraper/_runner.py`)
- **Scraper TLS**: `make scrape` agora configura `ATLAS_STF_SCRAPER_IGNORE_HTTPS_ERRORS=true` para resolver falha de certificado TLS do STF (cadeia ICP-Brasil incompleta no Playwright). Também inclui acórdãos além de decisões. (`Makefile`)

### Security

- **ZIP path traversal**: verificação de membros ZIP agora usa `Path.resolve()` + `is_relative_to()` em vez de checagem inline fraca (`".." not in` / `startswith("/")`). Aplica-se a CGU, CVM e RFB. (`core/zip_safety.py`)
- **RFB token mascarado**: token NextCloud não é mais logado em plaintext — apenas primeiros/últimos 4 caracteres. (`rfb/_runner.py`)
- **Compound risk `_coerce_float`**: rejeita `inf`/`nan`/`-inf` via `math.isfinite()`. (`compound_risk.py`)
- **Compound risk year bounds**: anos fora de `1900-2100` são ignorados no span temporal. (`compound_risk.py`)

## [1.0.0] - 2026-03-12

### Added

- Pipeline completo de dados: raw → staging → curated → analytics → evidence → serving → API → web
- **Staging**: limpeza e normalização de 10 CSVs do Portal do STF (~580 MB)
- **Scraper**: download de jurisprudência via API do STF (Playwright + httpx)
- **Curated**: builders de entidades (process, decision_event, subject, party, counsel, links, minister_bio)
- **Analytics**: 18 módulos estatísticos (comparison groups, baseline, outlier alerts, rapporteur profile, assignment audit, sequential analysis, temporal analysis, ML outlier score, minister flow, origin context, sanction match, donation match, corporate network, counsel affinity, compound risk, decision velocity, rapporteur change, counsel network)
- **Evidence**: bundles JSON de evidência para alertas
- **Serving**: banco SQLite com 27 tabelas materializado via builder
- **API**: FastAPI com 51+ endpoints (dashboard, alertas, casos, ministros, advogados, partes, sanções, doações, vínculos, afinidade, origem, temporal, convergência, velocidade, redistribuição, rede de advogados)
- **Frontend**: Next.js 16 + React 19 + TypeScript + Tailwind 4 + Recharts com 21 páginas SSR
- **CGU**: cliente Portal da Transparência (CEIS/CNEP/Leniência) com download bulk CSV + fallback API REST
- **TSE**: download de doações eleitorais (CSV público CDN)
- **CVM**: download de processos sancionadores (ZIP público CVM)
- **RFB**: download de dados abertos CNPJ — sócios e empresas (ZIPs RFB)
- **DataJud**: cliente API CNJ DataJud para contexto de origem
- **Core**: domínio puro (identity, parsers, rules, stats, origin_mapping) — sem I/O, 100% testado
- **CI/CD**: GitHub Actions (uv sync → ruff → pyright → pytest) + Docker multi-stage
- **Design System**: paleta Brasil (verde/azul/ouro/branco) com terminologia simplificada em português
- Sistema de alertas com supressão processual, sinais de risco e estratificação por órgão julgador (GroupKey v3)
- 775+ testes com 83% de cobertura
- Licença PolyForm Noncommercial 1.0.0
