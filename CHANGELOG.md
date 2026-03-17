# Changelog

Todas as alterações relevantes do projeto são documentadas neste arquivo.

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).

## [Unreleased]

## [1.0.7] - 2026-03-17

### Changed

- **agenda/_client.py**: client reescrito de httpx para Playwright — resolve AWS WAF challenge via navegador headless antes de executar queries GraphQL. `page.evaluate(fetch(...))` herda cookies/tokens do WAF automaticamente. Query GraphQL atualizada para schema atual do STF: `ano`/`mes` → `where:{dateQuery:{year,month}}`, `evento` → `eventos`, `horaInicio` → `hora`, campo `descricao` removido (não existe mais na API)
- **agenda/_runner.py**: barra de progresso Rich (`cli_progress`) integrada ao fetch de agenda, mesmo padrão dos demais comandos (CGU, TSE, CVM, RFB). Validação de cache endurecida: arquivos JSONL vazios e raw com erros GraphQL são automaticamente descartados e re-baixados em vez de bloquear re-fetch
- **agenda/_parser.py**: campos atualizados para schema GraphQL atual (`eventos` em vez de `evento`, `hora` em vez de `horaInício`)
- **Decomposição modular — analytics**: builders grandes decompostos em módulos focados — `compound_risk` (→ `_compound_risk_evidence` + `_compound_risk_loaders`), `corporate_network` (→ `_corporate_network_context`), `donation_match` (→ `_donation_aggregator` + `_donation_match_counsel`), `_match_helpers` (→ `_match_io` + `_outcome_helpers`), `sanction_corporate_link` (→ `_scl_bridge`). Backward compat preservado via re-imports
- **Decomposição modular — serving**: `_builder_loaders_analytics` dividido em `_common` (match confidence map), `_risk` (compound risk, velocity, rapporteur change, corporate conflict) e `_sanctions` (sanction matches, SCL, counsel profiles). Tabelas risk-based movidas de `models.py`/`_models_analytics.py` para `_models_analytics_risk.py`
- **Decomposição modular — fontes externas**: CGU `_runner.py` → `_runner_csv.py` (schema maps, normalização, download incremental); RFB `_runner.py` → `_runner_http.py` (WebDAV PROPFIND, download streaming, extração ZIP). Helpers de auditoria extraídos de `audit_gates.py` para `_audit_helpers.py`
- **Testes**: 10 arquivos monolíticos decompostos em 31 módulos focados + 6 helpers compartilhados — compound_risk (2), donation_match (4), donor_corporate_link (2), sanction_corporate_link (3), sanction_match (2), CGU runner (2), TSE expenses (2), curated representation (2), API service (2), smoke (2)
- **Makefile**: `scrape` agora inclui `transparencia-fetch` antes de jurisprudência; target `scrape-unsafe` removido (scrape já usa `--ignore-tls`). `stf-portal-fetch` passa `--ignore-tls` por padrão. Pipeline com dependências explícitas (não order-only). `serving-build` usa `$(CLI)` em vez de `uv run atlas-stf`
- **README.md**: seção Quick Start com `make setup` como atalho, seção Qualidade e CI reescrita com `make ci`, seção de limpeza com `make clean`/`clean-all`, seção Contribuindo simplificada

### Added

- **Makefile**: target `fetch-all` — baixa todas as fontes de uma vez (STF transparência + jurisprudência, CGU, TSE doações + despesas + órgãos partidários, CVM, RFB, portal STF, agenda GraphQL). DataJud excluído por exigir `DATAJUD_API_KEY`
- **agenda/_client.py**: exceção `AgendaWafChallengeError` — erro semântico específico para bloqueio WAF (HTTP 202 + `x-amzn-waf-action: challenge`). Fail fast sem retry (retry piora o score do bot)
- **stf_portal/_config.py**: campo `ignore_tls: bool` em `StfPortalConfig` — bypass de verificação TLS para portal STF (cadeia ICP-Brasil incompleta)
- **stf_portal/_extractor.py**: parâmetro `ignore_tls` no `PortalExtractor` — propaga `verify=not ignore_tls` ao httpx.Client
- **cli/_parsers_external.py**: flag `--ignore-tls` no subcomando `stf-portal fetch`
- **stf_portal/_runner.py**: barra de progresso Rich (`cli_progress`) integrada ao fetch do portal STF — mesmo padrão dos demais comandos (CGU, TSE, CVM, RFB, Agenda)
- **transparencia/**: módulo de download de CSVs dos painéis de transparência do STF (Qlik Sense) via Playwright headless — 11 painéis disponíveis (acervo, decisões, distribuídos, recebidos/baixados, repercussão geral, controle concentrado, plenário virtual, decisões COVID, reclamações, taxa de provimento, omissão inconstitucional). Suporte a `--dry-run`, `--ignore-tls`, `--paineis` para seleção customizada
- **cli/_parsers_external.py**: subcomando `transparencia fetch` com flags `--output-dir`, `--paineis`, `--headless`, `--ignore-tls`, `--dry-run`
- **stf_portal/_runner.py**: execução concorrente com flag `--workers` — `ThreadPoolExecutor` com um `PortalExtractor` independente por thread. Worker único usa caminho sequencial (sem overhead de pool)
- **stf_portal/_checkpoint.py**: dataclass `PortalCheckpoint` com métodos `is_completed()`, `mark_completed()`, `mark_failed()`, `is_stale()` — substitui manipulação manual de dicionário
- **cli/_parsers_external.py**: flag `--workers` no subcomando `stf-portal fetch` (default: 1)
- **Makefile**: targets `setup` (install + npm ci + playwright), `clean`/`clean-all`, `help` (grep de comentários `##`), `ci` (check + test + web-ci), `web-ci` (npm ci + lint + typecheck + build), `reproduce` (pipeline sequencial a partir de data/raw), `format-check`, `lint-fix`, `transparencia-fetch`
- **Makefile**: todos os targets documentados com comentários `## descrição` — `make help` lista targets disponíveis com descrição

### Fixed

- **rfb/_runner.py**: filtro de membro ZIP ignorava arquivos `ESTABELE` (mainframe-style) — predicado inline substituído por `_is_rfb_data_member()` que aceita `.csv`, `csv` no nome (`SOCIOCSV`, `EMPRECSV`) e `estabele` no nome (`ESTABELE`). Corrige 10× "No CSV found in ZIP" no pass 4
- **rfb/_runner_fetch.py**: `enrich_and_write_results()` sobrescrevia JSONL com conteúdo vazio quando passes anteriores eram pulados por checkpoint (listas em memória vazias). Nova `_safe_write_jsonl()` preserva arquivos existentes com conteúdo quando não há dados novos
- **stf_portal/_runner.py**: `except json.JSONDecodeError, ValueError, KeyError:` → `except (json.JSONDecodeError, ValueError, KeyError):` — sintaxe Python 2 que levantaria `TypeError` no Python 3 ao re-fetch de arquivo corrompido

## [1.0.6] - 2026-03-16

### Added

- **core/identity.py**: `strip_accents()` — remove diacríticos preservando caracteres base via `unicodedata.normalize("NFKD")`. Aplicada em `_tokenize_for_similarity()` e `levenshtein_distance()` — Jaccard e Levenshtein passam a ser accent-insensitive. `canonicalize_entity_name()` e lookup `by_canonical_name` permanecem accent-sensitive por design (identity keys estáveis)
- **core/stats.py**: `red_flag_power(n, p0, delta, alpha)` — poder estatístico de teste de proporção unilateral (z-test) para medir se a amostra é suficiente para detectar desvio `Δ=0.15` na taxa favorável. Fórmula fechada com `math.erfc` (stdlib, sem dependências externas). Bordas: `n<=0`, `p0<=0`, `p0>=1` → `0.0`. Retorna `float` bruto clamped a `[0, 1]`. `red_flag_confidence_label(power)` — classifica power em `"high"` (≥0.80), `"moderate"` (≥0.50), `"low"` (<0.50), `None` se `power is None`

- **tse/_config.py**: `TsePartyOrgFetchConfig` (frozen dataclass) com constante `TSE_PARTY_ORG_YEARS = (2018, 2020, 2022, 2024)` — anos pré-2018 fora do escopo. `TseExpenseFetchConfig` (frozen dataclass) com `years: tuple[int, ...] | None` — `None` usa `_SUPPORTED_EXPENSE_YEARS` (2002, 2004, 2006, 2008, 2010, 2022, 2024)
- **tse/_parser_party_org.py**: novo parser para CSVs de órgãos partidários TSE — receitas (48 colunas) e despesas contratadas (46 colunas). Suporta anos 2018-2024 com schema estável. Registros sem contraparte preservados; descartados apenas quando estruturalmente inválidos (sem valor, sem contraparte e sem descrição). Contagem de registros com `counterparty_name` vazio logada por arquivo
- **tse/_runner_party_org.py**: runner para download e ingestão de finanças de órgãos partidários (`prestacao_de_contas_eleitorais_orgaos_partidarios_{year}.zip`). Artefato unificado `party_org_finance_raw.jsonl` com `record_kind: "revenue"|"expense"` e `actor_kind: "party_org"`. Checkpoint isolado `_checkpoint_party_org.json` (sem interferir no pipeline de candidatos). Usa apenas `_BRASIL.csv` (agregado nacional) para evitar contagem dupla. Proveniência completa: `record_hash`, `source_file`, `source_url`, `collected_at`, `ingest_run_id`
- **tse/_parser_expenses.py**: parser de despesas de campanha de candidatos com 18 campos mapeados sobre 6 gerações de schema TSE (Gen1: 2002, Gen2: 2004 com artefatos SQL, Gen3: 2006, Gen4: 2008, Gen5: 2010 per-UF, Gen6: 2022-2024 `despesas_contratadas`). Aliases derivados exclusivamente de evidência empírica (headers reais). Campos ausentes em gerações antigas preservados como `None` (não string vazia) — `candidate_cpf` é `None` em Gen1-Gen4, `party_name` é `None` em Gen1-Gen5, `supplier_name_rfb`/`supplier_cnae_*` são `None` em Gen1-Gen5. Registros com `supplier_name` vazio preservados (nullable), não descartados
- **tse/_runner_expenses.py**: runner de despesas com checkpoint isolado `_checkpoint_expenses.json`, artefato `campaign_expenses_raw.jsonl`, download paralelo (ThreadPoolExecutor), escrita atômica (tmp + rename). Validação explícita de anos: 2018 rejeitado com mensagem sobre `SQ_PRESTADOR_CONTAS` (sem identificação de candidato), 2012/2014/2016/2020 rejeitados como "não implementados nesta versão". Importa helpers de `_runner.py` (débito técnico consciente). Proveniência completa: `record_hash`, `source_file`, `source_url`, `collected_at`, `ingest_run_id`
- **tse/_runner.py**: proveniência por registro no `donations_raw.jsonl` — 5 campos novos: `record_hash` (SHA-256 dos 21 campos normalizados), `source_file` (caminho relativo dentro do ZIP), `source_url` (URL do CDN), `collected_at` (timestamp ISO do ingest run), `ingest_run_id` (UUID do run). Helper `_record_content_hash()` — hash determinístico sobre conteúdo normalizado, excluindo metadados de proveniência
- **tse/_resource_classifier.py**: classificador determinístico de tipos de recurso TSE a partir de `donation_description` — 5 categorias (`empty`, `payment_method`, `source_type`, `in_kind`, `unknown`) com subtipagem detalhada (10 subtipos source_type, 4 payment_method, 10 in_kind). Normalização textual reproduzível (strip acentos via NFKD, collapse whitespace, strip pontuação de borda). Regras em ordem de prioridade: null markers → códigos numéricos (somente 0/1/2) → exact match → keyword → fallback. Cobertura global estimada: 98,23% (corpus real 20.8M registros). Módulo puro sem I/O

- **analytics/donation_empirical.py**: builder de relatório empírico sobre o corpus de doações TSE — lê artefatos existentes (`donations_raw.jsonl`, `donation_match.jsonl`, `donation_match_ambiguous.jsonl`) sem re-executar matching. Relatório `donation_empirical_metrics.json` com 3 seções: `raw_data_quality` (taxa de CPF vazio/mascarado, distribuição por ano/UF, percentis de valor via reservoir sampling 10K, proxy de homonímia via `donor_identity_key()` compartilhado), `match_quality` (distribuição por estratégia e entity_type, histograma Jaccard em buckets semiabertos, histograma Levenshtein, red flags por estratégia, enriquecimento corporativo), `ambiguous_analysis` (contagem por entity_type e uncertainty_note, distribuição de candidate_count, valor total ambíguo, ambiguous_rate com guard de divisão por zero). Notas metodológicas explícitas no output. Tolerância a artefatos ausentes (zeros, sem crash)
- **analytics/_donor_identity.py**: helper compartilhado `donor_identity_key()` extraído de `donation_match.py` — elimina risco de drift entre módulos que geram chaves de identidade de doador
- **analytics/donor_corporate_link.py**: novo builder determinístico de vínculos doador → empresa via CPF/CNPJ join com dados RFB. Três caminhos de resolução: PJ empresa própria (`exact_cnpj_basico`), PJ sócia (`exact_partner_cnpj`), PF sócio (`exact_partner_cpf`). Invariante: todo `donor_identity_key` gera ao menos 1 registro no output (doadores sem resolução emitem `masked_cpf`, `missing_document`, `invalid_document` ou `not_in_rfb_corpus`). Artefatos: `donor_corporate_link.jsonl` + `donor_corporate_link_summary.json`
- **analytics/_corporate_enrichment.py**: módulo de enriquecimento corporativo para donation matches — carrega `donor_corporate_link.jsonl`, `economic_group.jsonl` e `corporate_network.jsonl` (todos opcionais) e anota cada match in-place com 12 campos: identidade corporativa (`donor_document_type`, `donor_tax_id_normalized`, `donor_cnpj_basico`, `donor_company_name`), grupo econômico (`economic_group_id`, `economic_group_member_count`, `is_law_firm_group`, `donor_group_has_minister_partner`, `donor_group_has_party_partner`, `donor_group_has_counsel_partner`), rede societária (`min_link_degree_to_minister`, `corporate_link_red_flag`). Somente links `confidence == "deterministic"` são usados; ordenação por prioridade de `link_basis` (exact_cnpj_basico > exact_partner_cnpj > exact_partner_cpf); desempate de grupo econômico por `member_count` DESC, `group_id` ASC; flags booleanas agregadas via OR lógico
- **analytics/donation_match.py**: trilha revisável de matches ambíguos — artefato `donation_match_ambiguous.jsonl` com campos `donor_identity_key`, `entity_type`, `match_strategy`, `match_score`, `uncertainty_note`, `candidate_count`, `sample_candidate_name`, `total_donated_brl`, `donation_count`, `election_years`. Contadores explícitos no summary: `party_ambiguous_candidate_count`, `counsel_ambiguous_candidate_count`, `total_ambiguous_candidate_count`, `ambiguous_records_written`. Propagação dos 5 campos de proveniência do raw para `donation_event.jsonl` com fallback `.get()` para registros antigos. Etapa de enriquecimento corporativo pós-match — chama `build_corporate_enrichment_index()` + `enrich_match_corporate()` antes de escrever o JSONL; 4 campos de metadado no summary (`corporate_links_present`, `economic_groups_present`, `corporate_network_present`, `corporate_enriched_count`). Campo `resource_types_observed` agregado por match — conjunto de categorias de recurso observadas nos registros raw do doador; métricas de cobertura no summary (`resource_category_counts`, `resource_subtype_counts`, `resource_classification_coverage_rate`, `resource_classification_nonempty_coverage_rate`, `resource_classification_unknown_count`, `resource_classification_empty_count`). Métricas temporais e de concentração por doador — `first_donation_date`, `last_donation_date`, `active_election_year_count`, `max_single_donation_brl`, `avg_donation_brl`, `top_candidate_share`, `top_party_share`, `top_state_share`, `donation_year_span`, `recent_donation_flag` (presença nos últimos 2 ciclos eleitorais do corpus); acumulados single-pass durante `_stream_aggregate_donations()` sem leitura adicional. Consumidor migrado para baseline estratificado — substitui `build_baseline_rates` por `build_baseline_rates_stratified` + `build_process_jb_category_map` + `lookup_baseline_rate` com par modal `(class, jb_category)` nos branches party e counsel. Campos `red_flag_power` e `red_flag_confidence` nos registros de match — calculados com `n=len(seen_pids)` e `p0=baseline_favorable_rate`; `None` quando baseline indisponível

- **analytics/_match_helpers.py**: campo `candidate_count: int | None` no dataclass `EntityMatchResult` — populado nos retornos ambíguos (Jaccard e Levenshtein) com `len(best_records)`. Dataclass `MatchThresholds` (frozen, 4 campos: `jaccard_min=0.8`, `levenshtein_max=2`, `length_prefilter_max=2`, `max_fuzzy_candidates=10_000`) e constante `DEFAULT_MATCH_THRESHOLDS`; parâmetro `thresholds: MatchThresholds | None = None` em `match_entity_record()` — call sites existentes inalterados (default implícito); constante `_MAX_FUZZY_CANDIDATES` removida (absorvida pelo dataclass). `_collect_fuzzy_candidates()` extraída do bloco inline de pré-filtro de candidatos — reutilizada pelo harness de calibração sem duplicação de lógica. Baseline estratificado por `(process_class, judging_body_category)` — 3 novas funções: `build_baseline_rates_stratified()` retorna taxas estratificadas (apenas células com `>= MIN_RELIABLE_SIZE` eventos) + fallback por `process_class` (idêntico ao legado); `build_process_jb_category_map()` mapeia cada processo à categoria predominante de órgão julgador via `Counter.most_common(1)`; `lookup_baseline_rate()` faz lookup em 2 camadas (célula estratificada → fallback class → `None`). `MIN_RELIABLE_SIZE` importado de `baseline.py` (sem ciclo). Função legada `build_baseline_rates()` preservada para backward compat
- **analytics/_parallel.py**: propagação de `candidate_count` na serialização fork (`_match_one` e `_result_from_dict`). `match_entities_parallel()` aceita `thresholds: MatchThresholds | None = None` e propaga via module-level state (`_worker_thresholds`) para workers forked
- **analytics/_atomic_io.py**: `AtomicJsonlWriter` — context manager para escrita JSONL crash-safe (tmp + rename atômico, criação automática de diretórios pais, preserva arquivo original em caso de erro). Adotado em 15 builders de analytics existentes (assignment_audit, build_alerts, build_groups, counsel_affinity, counsel_network, decision_velocity, economic_group, ml_outlier, origin_context, pauta_anomaly, procedural_timeline, rapporteur_change, rapporteur_profile, sequential, temporal_analysis)
- **analytics/match_calibration.py**: harness de calibração reprodutível para fuzzy matching — `MatchDiagnostic` (16 campos, sem short-circuit), `match_entity_record_diagnostic()` computa todos os estágios (tax_id, alias, exact, canonical, jaccard, levenshtein) independentemente de hits determinísticos. 6 configurações de threshold avaliadas post-hoc (`default`, `jaccard_0.75/0.85/0.90`, `levenshtein_1/3`). Análise contrafactual de impacto de acentos (`accent_affected_count`, `accent_only_match_gain_count`, `accent_only_ambiguous_gain_count`, `accent_strategy_shift_count`). Review JSONL com cap de 500 registros por `review_reason` por `entity_type`. Artefatos: `match_calibration_summary.json` (histogramas de scores, configs comparados, accent impact por entity type) e `match_calibration_review.jsonl` (casos borderline, ambíguos e accent-affected)

- **analytics/sanction_match.py**: consumidor migrado para baseline estratificado — substitui `build_baseline_rates` por `build_baseline_rates_stratified` + `build_process_jb_category_map` + `lookup_baseline_rate` com pares `(class, jb_category)` e par modal via `Counter.most_common(1)`. Campos `red_flag_power` e `red_flag_confidence` nos registros de match — calculados com `n=len(seen_pids)` e `p0=baseline_favorable_rate`; `None` quando baseline indisponível
- **analytics/corporate_network.py**: consumidor migrado para baseline estratificado — closure `_compute_conflict()` captura `stratified_rates`, `fallback_rates` e `process_jb_map` do escopo externo; lógica de `_degree_decay` inalterada. Campos `red_flag_power` e `red_flag_confidence` nos registros de conflito — `n=len(seen_pids)` (processos compartilhados perante o ministro), `p0=baseline_favorable_rate`
- **analytics/sanction_corporate_link.py**: builder de vínculos corporativos indiretos de sancionados (CEIS/CVM → RFB → STF) — 3 caminhos determinísticos: CNPJ direto da sanção (`exact_cnpj_basico`), CNPJ sócio PJ (`exact_partner_cnpj`), CPF sócio PF (`exact_partner_cpf`). Expansão por grupo econômico (grau 3). Risk score com decaimento exponencial por grau de separação (`0.5^(degree-2)`). Red flag quando `risk_score > 0.15 AND process_count >= 3`. Trilha de evidência completa (`evidence_chain`), campos de auditabilidade (`matched_alias`, `matched_tax_id`, `uncertainty_note`), power analysis (`red_flag_power`, `red_flag_confidence`). Deduplicação preserva rotas distintas (mesmo alvo, `bridge_link_basis` diferente). Artefatos: `sanction_corporate_link.jsonl` + `sanction_corporate_link_summary.json`
- **analytics/compound_risk.py**: 3 campos de metadados de vínculos corporativos de sancionados no compound risk index. Risco composto enriquecido — `adjusted_rate_delta` como campo de ranking com multiplicadores corporativos explícitos (`is_law_firm_group ×1.5`, `donor_group_has_minister_partner ×2.0`, atenuação `0.5^(degree−2)` para `min_link_degree_to_minister > 2`). Promoção de SCL para família "sanction" quando não há sanção direta (pós-passe limpo, uma vez por par). Acumulação de enrichment corporativo de doações via `accumulate_donation_enrichment()` em 3 locais de ingestão (counsel direto, party direto, cross-entity). Metadados corporativos em `signal_details["donation"]` (15 campos) e `signal_details["sanction"]` (scl_count, scl_min_degree). Novos campos no output: `adjusted_rate_delta`, `has_law_firm_group`, `donor_group_has_minister_partner`, `donor_group_has_party_partner`, `donor_group_has_counsel_partner`, `min_link_degree_to_minister`. Ordenação atualizada: `signal_count DESC → adjusted_rate_delta DESC → max_alert_score DESC → shared_process_count DESC`. Anti-double-counting preservado integralmente
- **analytics/payment_counterparty.py**: builder de contrapartes de pagamento de órgãos partidários — rollup analítico single-pass sobre `party_org_finance_raw.jsonl` (apenas `record_kind == "expense"`). Consolida recebimentos por contraparte com identidade estável (`build_identity_key`), campos `identity_basis` ("tax_id" ou "name_fallback"), melhor nome (rfb > raw > normalized), date range, proveniência resumida (contadores, não listas), artefatos `payment_counterparty.jsonl` + `payment_counterparty_summary.json`

- **rfb/_parser.py**: parâmetros `target_cpfs` e `target_partner_cnpjs` em `parse_socios_csv_filtered()` / `_parse_socios_reader()` — comparação via `normalize_tax_id()` no `partner_cpf_cnpj` raw. CPF/CNPJ matches adicionam `cnpj_basico` ao `matched_cnpjs` (mesmo comportamento que name match)
- **rfb/_runner.py**: `_extract_tse_donor_targets()` lê `donations_raw.jsonl` e classifica doadores em PJ cnpj_basico / PF CPFs / PJ CNPJs completos. `_compute_tse_targets_hash()` para invalidação de checkpoint. `fetch_rfb_data()` auto-detecta `donations_raw.jsonl` e injeta alvos TSE no Pass 1 + cnpj_basico direto no `matched_cnpjs`
- **rfb/_config.py**: campo `donations_path` no `RfbFetchConfig` (default: `data/raw/tse/donations_raw.jsonl`)
- **rfb/_runner_fetch.py**: `run_pass1_socios()` aceita `target_cpfs` e `target_partner_cnpjs`

- **serving/_models_analytics.py**: nova tabela `ServingPaymentCounterparty` (18 colunas) e nova tabela `ServingSanctionCorporateLink` (~30 colunas). Em `ServingDonationMatch`: `donor_identity_key` (String(512), indexada), 12 colunas de enriquecimento corporativo, `resource_types_observed_json`, 10 métricas temporais/concentração, `red_flag_power` (Float) e `red_flag_confidence` (String(16)). Em `ServingDonationEvent`: `donor_identity_key`, 5 campos de proveniência (`source_file`, `collected_at`, `source_url`, `ingest_run_id`, `record_hash`) e 4 campos de classificação de recurso (`resource_type_category`, `resource_type_subtype`, `resource_classification_confidence`, `resource_classification_rule`). Em `ServingSanctionMatch` e `ServingCorporateConflict`: `red_flag_power` (Float) e `red_flag_confidence` (String(16)). Em `ServingCompoundRisk`: 3 campos SCL e 6 campos de enriquecimento (`adjusted_rate_delta` Float indexado, `has_law_firm_group`, `donor_group_has_minister_partner`, `donor_group_has_party_partner`, `donor_group_has_counsel_partner` Boolean, `min_link_degree_to_minister` Integer nullable)
- **serving/_builder_loaders_analytics.py**: `load_donation_matches()` mapeia `donor_identity_key`, 12 campos corporativos, `resource_types_observed_json`, 10 temporais e power analysis. `load_donation_events()` mapeia 6 proveniência e 4 classificação de recurso. `load_sanction_matches()` mapeia power analysis. `load_sanction_corporate_links()` com dedup por `link_id`. `load_payment_counterparties()` com dedup por `counterparty_id`. `load_compound_risks()` mapeia 6 campos de enriquecimento
- **serving/_builder_loaders_corporate.py**: `load_corporate_conflicts()` mapeia `red_flag_power` e `red_flag_confidence`

- **api/_schemas_risk.py**: schemas `PaymentCounterpartyItem` (18 campos), `PaginatedPaymentCounterpartiesResponse`, `SanctionCorporateLinkItem`, `PaginatedSanctionCorporateLinksResponse`, `SanctionCorporateLinkRedFlagsResponse`. Em `DonationMatchItem`: `donor_identity_key`, 12 campos corporativos, `resource_types_observed`, 10 temporais, `red_flag_power`, `red_flag_confidence`. Em `DonationEventItem`: 6 proveniência e 4 classificação de recurso. Em `SanctionMatchItem` e `CorporateConflictItem`: `red_flag_power` e `red_flag_confidence`. Em `CompoundRiskItem`: 6 campos de enriquecimento + `adjusted_rate_delta` em `CompoundRiskHeatmapCell`. Campos SCL em `CompoundRiskItem`
- **api/_donations.py**: mapeamento de `donor_identity_key`, 12 campos corporativos, `resource_types_observed`, 10 temporais e campos de proveniência/classificação de eventos
- **api/_sanctions.py**, **api/_corporate_network.py**: mapeamento de `red_flag_power` e `red_flag_confidence` nos service mappers (com `cast` para `Literal` type em pyright strict)
- **api/_payment_counterparties.py**: `GET /payment-counterparties` (lista paginada, ordenação por `total_received_brl DESC`) — endpoint 70
- **api/_sanction_corporate_links.py**: service layer para consultas de vínculos corporativos de sancionados
- **api/_routes_risk.py**: 3 endpoints — `GET /sanction-corporate-links` (lista paginada), `GET /sanction-corporate-links/red-flags`, `GET /parties/{party_id}/sanction-corporate-links`
- **api/_compound_risk.py**: 6 campos de enriquecimento em `_row_to_item()`. `order_by` alinhado com builder em 3 locais (list, red-flags, heatmap). Heatmap cell inclui `adjusted_rate_delta`

- **web/src/lib/sanctions-data.ts**: types e fetchers para vínculos corporativos de sancionados
- **web/src/app/sancoes/page.tsx**: seção de vínculos corporativos na página de sanções
- **web/src/lib/compound-risk-data.ts**: 10 novos campos TypeScript em `CompoundRiskItem` (SCL + enriquecimento) + `adjusted_rate_delta` em `CompoundRiskHeatmapCell`
- **web/src/components/dashboard/compound-risk-ranking.tsx**: label "Maior delta" → "Delta ajustado", valor `max_rate_delta` → `adjusted_rate_delta`
- **web/src/components/dashboard/compound-risk-panels.tsx**: heatmap cell exibe `adjusted_rate_delta` em vez de `max_rate_delta`

- **docs/10-glossario.md**: nova seção "Termos técnicos (campos e identificadores)" com 16 subseções e ~145 entradas — cada termo explica o contexto real no Atlas STF (identidade, estatística, matching, red flags, doações TSE, rede corporativa, sanções, contrapartes, velocidade decisória, relator/fluxo, afinidade, representação, proveniência, classificação de resultado, recurso TSE, métricas empíricas)
- **README.md**: endpoint `GET /donations/{match_id}/events` e 7 endpoints de `/representation/*` adicionados à tabela de endpoints (faltavam desde v1.0.4/v1.0.5)

- **cli**: subcomandos `tse empirical-report`, `tse fetch-party-org`, `tse fetch-expenses`, `tse build-counterparties`, `tse build-donor-links`, `analytics calibrate-match`, `cgu build-corporate-links` — todos com flags `--output-dir` e opções específicas por comando (`--years`, `--dry-run`, `--force-refresh`, `--tse-dir`, `--rfb-dir`, `--analytics-dir`, `--curated-dir`, `--cgu-dir`, `--cvm-dir`, `--party-path`, `--counsel-path`, `--alias-path`)
- **Makefile**: targets `tse-empirical-report`, `tse-party-org-fetch`, `tse-party-org`, `tse-fetch-expenses`, `tse-expenses`, `tse-counterparties`, `tse-donor-links`, `cgu-corporate-links` (targets agregados `external-fetch`, `external-data`, `pipeline` e `tse` inalterados)
- ~383 novos testes cobrindo: relatório empírico (28), trilha de ambíguos e proveniência (9), órgãos partidários TSE (25), despesas de campanha (52), contrapartes de pagamento (22), vínculos doador→empresa (41), enriquecimento corporativo (10), classificador de recurso (100), métricas temporais (11), calibração de matching (25+7+2), baseline estratificado (11), power analysis (17), vínculos corporativos de sancionados (16), risco composto enriquecido (17)

### Changed

- **analytics/donation_match.py**: campo `ambiguous_candidate_count` substituído por `party_ambiguous_candidate_count` + `counsel_ambiguous_candidate_count` + `total_ambiguous_candidate_count` — semântica explícita, sem margem para interpretação errada. `_donor_identity_key()` agora delega para `_donor_identity.donor_identity_key()` (backward-compatible wrapper, mesma lógica)
- **rfb/_runner.py**: checkpoint agora inclui `tse_targets_hash` — se o hash muda, passes 1-4 são invalidados para rescan. Checkpoints antigos sem o campo carregam normalmente (backward-compatible)
- **serving/_builder_schema.py**: SERVING_SCHEMA_VERSION 10 → 16. **Upgrade destrutivo**: `_ensure_compatible_schema` detecta incompatibilidade, DROP + CREATE ALL. Executar `make serving-build` reconstrói toda a base
- **serving/_builder_loaders_analytics.py**: `load_donation_matches()` mapeia `donor_identity_key`, 12 campos corporativos, `resource_types_observed_json` e 10 campos temporais/concentração; `load_donation_events()` mapeia 6 campos de proveniência e 4 campos de classificação de recurso
- **Trilha de auditoria TSE**: cadeia completa **match → events (via `match_id`) → proveniência per-registro**. `donation_match.jsonl` não recebe proveniência detalhada — o match agrega registros de múltiplos arquivos/runs; proveniência vive nos events
- **README.md**: contagem de páginas web corrigida (21 → 26), contagem de componentes corrigida (35+ → 19), endpoints de agenda corrigidos (`/agenda/coverage` → `/agenda/ministers`, `/agenda/exposure` → `/agenda/exposures`), endpoint `GET /cases/{decision_event_id}/ml-outlier` adicionado à tabela
- **scraper/_session.py**: bypass TLS via parâmetro explícito `ignore_tls` — variável de ambiente `ATLAS_STF_IGNORE_HTTPS_ERRORS` marcada como deprecated com warning; preferir flag `--ignore-tls`
- **docs/representation_network_contract.md**: removido — conteúdo integrado ao glossário e à documentação inline dos módulos

### Fixed

- **serving/_builder_loaders.py**: `build_source_audits()` sanitiza paths de artefatos para relativos (`label/filename`) — eliminada exposição de caminhos absolutos do filesystem do servidor
- **api/_compound_risk.py**: campos SCL (`scl_count`, `scl_min_degree`, `scl_max_risk_score`) existiam no schema Pydantic mas não eram mapeados em `_row_to_item()` — gap silencioso corrigido

## [1.0.5] - 2026-03-15

### Added

- **tse/_parser.py**: campo `donor_name_originator` separado de `donor_name` — `NM_DOADOR_ORIGINARIO` deixa de ser alias de `donor_name` e passa a campo autônomo com normalização independente (P1)
- **tse/_parser.py**: campo `donation_date` com alias `DT_RECEITA`/`DATA_RECEITA` e parser `_parse_donation_date` para formatos `dd/MM/yyyy` e ISO (P3)
- **analytics/donation_match.py**: chave de agregação estável por `donor_cpf_cnpj` quando disponível, fallback para `donor_name_normalized` — previne fusão de homônimos (P2)
- **analytics/donation_match.py**: escrita de `donation_event.jsonl` com doações individuais dos doadores matched, preservando data, candidato, partido, cargo e descrição (P3)
- **analytics/donation_match.py**: campos `donor_name_normalized`, `donor_name_originator` e `donor_identity_key` nos registros de match (P1)
- **serving/_models_analytics.py**: colunas `entity_id`, `donor_name_normalized`, `donor_name_originator`, `favorable_rate_substantive`, `substantive_decision_count`, `red_flag_substantive`, `matched_alias`, `matched_tax_id`, `uncertainty_note` em `ServingDonationMatch` (P4, P5)
- **serving/_models_analytics.py**: nova tabela `ServingDonationEvent` com 13 campos para doações individuais (P3)
- **api/_donations.py**: endpoint `GET /donations/{match_id}/events` com paginação para consultar doações individuais de um match (P3)
- **api/_schemas_risk.py**: schemas `DonationEventItem` e `PaginatedDonationEventsResponse` (P3)
- **api/_schemas_risk.py**: campos de auditabilidade (`favorable_rate_substantive`, `red_flag_substantive`, `matched_alias`, `matched_tax_id`, `uncertainty_note`, `entity_id`, `donor_name_normalized`, `donor_name_originator`) em `DonationMatchItem` (P4, P5)
- **tse/_config.py**: opção `force_refresh` em `TseFetchConfig` (P6)
- **tse/_runner.py**: suporte a `force_refresh` — limpa checkpoint dos anos solicitados antes de re-baixar (P6)
- **cli/_parsers_external.py**: flag `--force-refresh` no subcomando `tse fetch` (P6)

### Changed

- **serving/_builder_schema.py**: schema version 7 → 8 (nova tabela + novos campos). **Upgrade destrutivo**: o serving builder detecta incompatibilidade de schema e faz DROP + CREATE ALL de todas as tabelas gerenciadas antes de reconstruir. O serving é uma visão materializada — a fonte de verdade são os artefatos JSONL. Executar `make serving-build` após o upgrade reconstrói toda a base
- **tse/_runner.py**: checkpoint agora é salvo **após** o rename atômico do JSONL — previne inconsistência checkpoint-vs-dados se o processo cair no meio da escrita
- **serving/_builder_loaders_analytics.py**: `load_donation_matches` carrega todos os campos de auditabilidade; nova função `load_donation_events` (P3, P4)
- **api/_donations.py**: `_row_to_match_item` usa `entity_id` nativo em vez de reusar `party_id` para counsel (P5)
- **serving/builder.py**: carrega e persiste `donation_events` durante o build do serving
- **tse/_runner.py**: copy loop do JSONL agora exclui registros de anos sendo re-baixados — previne duplicação em force-refresh e cenários de remote-change (P6)
- **analytics/donation_match.py**: `_donor_identity_key` usa `normalize_tax_id()` para sanitizar CPF/CNPJ antes da chave — mascarados (`***-**`) e vazios caem no fallback por nome (P2)
- **tse/_parser.py**: fallback para `donor_name_originator` quando `donor_name` está vazio em variantes antigas de CSV (P1)
- **api/_donations.py**: corrigido `except ValueError, TypeError` → `except (ValueError, TypeError)` (sintaxe Python 3)
- **README.md**: seção de instalação atualizada — Docker via ghcr.io e wheel via release asset substituem referência ao registro PyPI do GitHub Packages (descontinuado)
- **CHANGELOG.md**: entradas v1.0.4, v1.0.3 e v1.0.1 revisadas com acentuação gráfica correta em português brasileiro
- **CI unificado**: workflow `publish.yml` removido; job `publish` integrado ao `ci.yml` — um único workflow por release, com upload de assets e push Docker condicionais em tags `v*`
- **Dockerfile**: adicionado `README.md` ao `COPY` do builder (exigido pelo hatchling para build do wheel)

## [1.0.4] - 2026-03-15

### Added

- **agenda/**: módulo de agenda ministerial — fetcher da API GraphQL do STF (`noticias.stf.jus.br/graphql`) com ingestão de audiências, sessões e compromissos oficiais; GET/POST fallback, retry, rate limiting, validação de contrato
- **curated/build_agenda.py**: builder de eventos de agenda com cruzamento referencial contra dados processuais (processo, partes, advogados); calcula cobertura por ministro/mês com business_days, recesso, vacation/leave heuristic, publication_gap
- **analytics/agenda_exposure.py**: scoring de proximidade temporal entre eventos de agenda e decisões em janelas 7d/14d/30d/60d, com baseline intra-ministro condicionado por classe+tipo decisório; cap em 0.29 para observações insuficientes (n<5)
- **Taxonomia de agenda**: 4 categorias — `institutional_core`, `institutional_external_actor`, `private_advocacy`, `unclear`; 9 meeting_nature types; regra de precedência para eventos mistos (public+private → unclear, conf max 0.4)
- **Rede de Representação Processual**: subsistema completo de representação com identidade profissional OAB
- **core/identity.py**: `normalize_oab_number`, `is_valid_oab_format`, `normalize_cnsa_number`, `build_lawyer_identity_key` (prioridade OAB > CPF > nome), `build_firm_identity_key` (CNPJ > CNSA > nome), `VALID_UF_CODES` (27 UFs)
- **5 JSON Schemas**: `lawyer_entity`, `law_firm_entity`, `representation_edge`, `representation_event`, `source_evidence` — com enums de `representative_kind` (lawyer|law_firm), `role_type` (7 papéis) e `event_type` (8 tipos)
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
