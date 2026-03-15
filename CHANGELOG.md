# Changelog

Todas as alterações relevantes do projeto são documentadas neste arquivo.

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).

## [Unreleased]

## [1.0.3] - 2026-03-15

### Added

- **rfb/_reference.py**: Fetch e parse de 5 tabelas de dominio RFB (Qualificacoes, Naturezas, Cnaes, Municipios, Motivos) — lookup O(1) por codigo → descricao legivel
- **rfb/_parser_estabelecimentos.py**: Parser CSV de Estabelecimentos RFB (delimitador `;`) com filtro por CNPJ, montagem de `cnpj_full` (14 digitos) e split de CNAEs secundarios
- **rfb/_enrichment.py**: Funcoes puras de enriquecimento — decodifica qualification_code, natureza_juridica, cnae_fiscal, municipio e motivo de situacao cadastral
- **rfb/_runner_fetch.py**: Logica de 4 passes extraida do runner (Socios x2, Empresas, Estabelecimentos) com enriquecimento integrado via tabelas de referencia
- **analytics/economic_group.py**: Deteccao de grupos economicos via Union-Find sobre cadeia societaria PJ — output `economic_group.jsonl` com member_count, capital social, CNAEs, UFs, flags de ministro/parte/advogado, sem cap artificial
- **serving/_builder_loaders_corporate.py**: Loaders corporativos extraidos de `_builder_loaders_analytics.py` — `load_corporate_conflicts()` (atualizado com 25+ campos novos) e `load_economic_groups()`
- **ServingEconomicGroup**: Nova tabela no serving (total: 30 tabelas) com 14 colunas — grupo economico materializado para consulta API
- **api/_economic_groups.py**: `GET /economic-groups` (lista paginada com filtros booleanos) e `GET /economic-groups/{group_id}` (detalhe) — total: 55 endpoints
- **Enriquecimento do corporate_network.jsonl**: 25+ campos novos por conflito — labels decodificadas (qualificacao, natureza juridica, CNAE), dados multi-estabelecimento (sede, UFs, CNAEs, ate 3 key_establishments), grupo economico (id, member_count, razoes sociais) e proveniencia (evidence_type, source_dataset, evidence_strength)
- **Campos opcionais em temporal_analysis**: enrichment de establishment_count, active_establishment_count, headquarters_uf, headquarters_cnae_label, economic_group_id, economic_group_member_count nos registros `corporate_link_timeline`
- **CLI**: subcomando `rfb build-groups` com `--rfb-dir` e `--output-dir`
- **Makefile**: alvo `rfb-groups` com dependencia correta (rfb-fetch → rfb-groups → rfb-network)
- **Frontend /vinculos enriquecido**: badges de proveniencia (evidence_type + evidence_strength), qualificacao decodificada, CNAE da sede, natureza juridica, status cadastral com motivo, contagem de estabelecimentos, grupo economico com razoes sociais
- 38 novos testes (total: 1036): test_reference (6), test_enrichment (6), test_parser_estabelecimentos (8), test_economic_group (13), test_economic_groups_api (5)

### Changed

- **rfb/_runner.py**: Refatorado — passes 1-4 extraidos para `_runner_fetch.py`, checkpoint atualizado com `completed_estabelecimentos` e `completed_reference`, total_steps=41, cache check inclui `establishments_raw.jsonl`
- **rfb/_config.py**: +2 constantes (`RFB_ESTABELECIMENTOS_FILE_COUNT=10`, `RFB_REFERENCE_TABLES`)
- **serving/_builder_schema.py**: SERVING_SCHEMA_VERSION 5 → 6
- **ServingCorporateConflict**: +25 colunas (labels decodificadas, multi-estabelecimento, grupo economico, proveniencia, taxa substantiva)
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
- **Classificacao de materialidade**: nova funcao `classify_outcome_materiality()` em `core/rules.py` — classifica decisoes em `substantive`, `procedural`, `provisional` ou `unknown` com ordem de precedencia deterministica (provisional > procedural > substantive > unknown)
- **Taxa de exito substantiva**: nova funcao `compute_favorable_rate_substantive()` em `_match_helpers.py` — calcula taxa de exito filtrando apenas decisoes de merito (exclui liminares, desistencias, inadmissibilidade e embargos de declaracao)
- **Campos substantivos nos analytics**: `favorable_rate_substantive`, `substantive_decision_count` e `red_flag_substantive` adicionados aos outputs de `sanction_match`, `donation_match`, `corporate_network` e `counsel_affinity`
- **Auditabilidade do matching**: campos `match_strategy`, `match_score` e `match_confidence` materializados no serving (`ServingSanctionMatch`, `ServingDonationMatch`), expostos na API e exibidos no frontend como badge de confianca nos cards de sancoes e doacoes
- **Badge de confianca no frontend**: indicador visual nos cards de sancoes e doacoes — "CPF/CNPJ exato", "Nome exato", "Match fuzzy (score)", "Revisao manual necessaria" ou "Confianca nao determinada"
- **Smoke tests de validacao real** (`tests/test_smoke_validation.py`): 39 testes em 3 fases — (A) materialidade + taxa substantiva + builder output, (B) match_strategy/confidence no serving + DB schema + API endpoints, (C) normalizacao CGU CPF/CNPJ + tipo PF/PJ + datas — sem mocks, fixtures minimas em tmp_path
- **CGU checkpoint**: download incremental via HEAD request — compara Content-Length e data do servidor antes de baixar. Se o ZIP não mudou, reutiliza o CSV existente em disco. (`src/atlas_stf/cgu/_runner.py`, `src/atlas_stf/cgu/_checkpoint.py`)
- **CVM checkpoint**: download incremental via HEAD request — compara ETag/Content-Length do servidor. Se o ZIP não mudou, pula download e reutiliza `sanctions_raw.jsonl` em cache. (`src/atlas_stf/cvm/_runner.py`)
- **RFB auto-discovery**: se o token NextCloud da RFB expirar (401/403), o sistema tenta descobrir automaticamente o novo token via scraping da página principal. Loga o token mascarado para o usuário persistir na env var. (`src/atlas_stf/rfb/_runner.py`)

### Changed

- **Compound risk substantivo**: `compound_risk.py` agora usa `red_flag_substantive` como autoridade exclusiva quando presente no registro; campo legado `red_flag` só governa para dados antigos ou fontes sem métrica substantiva (ex: `rapporteur_change`). Elimina falsos positivos inflados por liminares
- **CGU normalizacao de CPF/CNPJ**: `entity_cnpj_cpf` agora contem apenas digitos (normalizado via `normalize_tax_id()`); valor original preservado em `entity_cnpj_cpf_raw`
- **CGU extracao tipo PF/PJ**: novo campo `entity_type_pf_pj` (canonicalizado para "PF"/"PJ") e `entity_type_pf_pj_raw` (valor bruto do CSV) na ingestao CEIS/CNEP/Leniencia
- **CGU normalizacao de datas**: `sanction_start_date` e `sanction_end_date` agora em formato YYYY-MM-DD (parseia DD/MM/YYYY, YYYY-MM-DD, YYYYMMDD); valores originais preservados em `*_raw`; datas invalidas gravadas como `null`

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
