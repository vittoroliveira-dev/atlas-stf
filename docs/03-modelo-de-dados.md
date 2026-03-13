# Modelo de dados

## Objetivo

Definir o modelo conceitual do projeto, separando claramente entidades centrais, relações e camadas analíticas.

## Princípios do modelo

1. Separar processo de evento decisório.
2. Separar entidade jurídica de ator humano.
3. Preservar origem e proveniência.
4. Permitir evolução sem quebrar compatibilidade.
5. Manter compatibilidade entre camada estruturada e futura camada textual.

---

## Entidades centrais

### 1. `process`

Representa o processo como unidade jurídica principal.

#### Campos conceituais
- `process_id`
- `process_number`
- `process_class`
- `filing_date`
- `origin_description`
- `origin_court_or_body`
- `branch_of_law`
- `subjects_raw`
- `subjects_normalized`
- `procedural_status`
- `case_environment`
- `source_id`
- `source_record_hash`

---

### 2. `decision_event`

Representa o fato decisório vinculado a um processo.

#### Campos conceituais
- `decision_event_id`
- `process_id`
- `decision_date`
- `decision_year`
- `current_rapporteur`
- `decision_origin`
- `decision_type`
- `decision_progress`
- `decision_note`
- `panel_indicator`
- `judging_body`
- `is_collegiate`
- `source_id`
- `source_row_id`

---

### 3. `party`

Representa parte processual.

#### Campos conceituais
- `party_id`
- `party_name_raw`
- `party_name_normalized`
- `party_type`
- `confidence_level`

---

### 4. `counsel`

Representa advogado ou representante.

#### Campos conceituais
- `counsel_id`
- `counsel_name_raw`
- `counsel_name_normalized`
- `confidence_level`

---

### 5. `process_party_link`

Relaciona processo e parte.

#### Campos conceituais
- `link_id`
- `process_id`
- `party_id`
- `role_in_case`
- `source_id`

---

### 6. `process_counsel_link`

Relaciona processo e advogado.

#### Campos conceituais
- `link_id`
- `process_id`
- `counsel_id`
- `side_in_case`
- `source_id`

---

### 7. `subject`

Representa assunto ou tema.

#### Campos conceituais
- `subject_id`
- `subject_raw`
- `subject_normalized`
- `subject_group`
- `branch_of_law`

---

### 8. `comparison_group`

Representa um conjunto de casos comparáveis.

#### Campos conceituais
- `comparison_group_id`
- `definition_version`
- `selection_criteria`
- `time_window`
- `case_count`
- `baseline_notes`

---

### 9. `outlier_alert`

Representa um alerta analítico produzido pelo sistema.

#### Campos conceituais
- `alert_id`
- `process_id`
- `decision_event_id`
- `comparison_group_id`
- `alert_type`
- `alert_score`
- `expected_pattern`
- `observed_pattern`
- `evidence_summary`
- `status`
- `uncertainty_note`

---

### 10. `external_event`

Representa fato externo documentado, quando utilizado.

#### Campos conceituais
- `external_event_id`
- `event_date`
- `event_title`
- `event_description`
- `event_source`
- `confidence_level`
- `notes`

---

### 11. `alert_analysis`

Representa uma camada derivada, opcional e não determinística de síntese ou refino assistido por IA sobre um `outlier_alert`.

#### Campos conceituais
- `analysis_id`
- `alert_id`
- `analysis_model`
- `analysis_version`
- `prompt_version`
- `source_bundle_version`
- `source_artifact_hash`
- `summary_descriptive`
- `summary_comparative`
- `uncertainty_note`
- `recommended_next_step`
- `created_at`

#### Regras conceituais
- `alert_analysis` nunca substitui `outlier_alert`, `baseline` ou `comparison_group`.
- `alert_analysis` deve ser sempre derivado de artefatos já materializados.
- `alert_analysis` não pode introduzir rótulos proibidos nem conclusões acusatórias.
- `alert_analysis` deve preservar referência explícita à versão do bundle ou artefato de origem.

#### Estado do artefato
- O schema já existe em `schemas/alert_analysis.schema.json`.
- A entidade ainda não entra no pipeline atual como saída obrigatória.
- Não há builder, CLI ou rotina de geração material para essa camada no escopo atual.
- A especificação mínima do builder está descrita em `docs/04-metodologia-analitica.md`.

---

### 12. `origin_context`

Representa o contexto agregado de um tribunal de origem, derivado da API CNJ DataJud.

#### Campos conceituais
- `origin_index`
- `tribunal_label`
- `state`
- `datajud_total_processes`
- `stf_process_count`
- `stf_share_pct`
- `top_assuntos`
- `top_orgaos_julgadores`
- `class_distribution`
- `generated_at`

#### Estado do artefato
- Schema: `schemas/origin_context.schema.json`
- Builder: `analytics/origin_context.py`
- Serving: `serving_origin_context` (tabela SQLite)
- API: `GET /origin-context`, `GET /origin-context/{state}`

---

### 13. `sanction_match`
Representa o cruzamento entre uma parte processual do STF e um registro de sanção CEIS/CNEP/Leniência/CVM. Integra registros de CEIS, CNEP e Acordos de Leniência (CGU) e Processos Sancionadores (CVM).

#### Campos conceituais
- `match_id`
- `party_id`
- `party_name_normalized`
- `sanction_source` (ceis | cnep | cvm | leniencia)
- `sanction_id`
- `sanctioning_body`
- `sanction_type`
- `sanction_start_date`
- `sanction_end_date`
- `sanction_description`
- `stf_case_count`
- `favorable_rate`
- `baseline_favorable_rate`
- `favorable_rate_delta`
- `red_flag`
- `matched_at`

#### Estado do artefato
- Builder: `analytics/sanction_match.py`
- Serving: `serving_sanction_match` (tabela SQLite)
- API: `GET /sanctions`, `GET /parties/{party_id}/sanctions`

### 14. `counsel_sanction_profile`
Perfil agregado de advogado em relação a clientes sancionados.

#### Campos conceituais
- `counsel_id`
- `counsel_name_normalized`
- `sanctioned_client_count`
- `total_client_count`
- `sanctioned_client_rate`
- `sanctioned_favorable_rate`
- `overall_favorable_rate`
- `red_flag`

#### Estado do artefato
- Builder: `analytics/sanction_match.py`
- Serving: `serving_counsel_sanction_profile` (tabela SQLite)
- API: `GET /counsels/{counsel_id}/sanction-profile`

### 15. `donation_match`
Representa o cruzamento entre uma parte processual do STF e um doador de campanha eleitoral registrado no TSE.

#### Campos conceituais
- `match_id`
- `party_id`
- `party_name_normalized`
- `donor_cpf_cnpj`
- `total_donated_brl`
- `donation_count`
- `election_years`
- `parties_donated_to`
- `candidates_donated_to`
- `positions_donated_to`
- `stf_case_count`
- `favorable_rate`
- `baseline_favorable_rate`
- `favorable_rate_delta`
- `red_flag`
- `matched_at`

#### Estado do artefato
- Builder: `analytics/donation_match.py`
- Serving: `serving_donation_match` (tabela SQLite)
- API: `GET /donations`, `GET /parties/{party_id}/donations`

### 16. `counsel_donation_profile`
Perfil agregado de advogado em relação a clientes doadores de campanha.

#### Campos conceituais
- `counsel_id`
- `counsel_name_normalized`
- `donor_client_count`
- `total_client_count`
- `donor_client_rate`
- `donor_client_favorable_rate`
- `overall_favorable_rate`
- `red_flag`

#### Estado do artefato
- Builder: `analytics/donation_match.py`
- Serving: `serving_counsel_donation_profile` (tabela SQLite)
- API: `GET /counsels/{counsel_id}/donation-profile`

### 17. `corporate_conflict`
Representa um vínculo societário detectado entre um ministro do STF e uma parte ou advogado, via co-participação em empresa (dados RFB).

#### Campos conceituais
- `conflict_id`
- `minister_name`
- `company_cnpj_basico`
- `company_name`
- `minister_qualification`
- `linked_entity_type` (party | counsel)
- `linked_entity_id`
- `linked_entity_name`
- `entity_qualification`
- `shared_process_ids`
- `shared_process_count`
- `favorable_rate`
- `baseline_favorable_rate`
- `favorable_rate_delta`
- `red_flag`
- `link_chain`
- `generated_at`

#### Estado do artefato
- Builder: `analytics/corporate_network.py`
- Serving: `serving_corporate_conflict` (tabela SQLite)
- API: `GET /corporate-network`, `GET /corporate-network/red-flags`, `GET /ministers/{minister}/corporate-conflicts`

---

### 18. `counsel_affinity`
Representa um par (ministro, advogado) com taxa de vitória anômala, derivado apenas de dados curated internos (sem dependência externa).

#### Campos conceituais
- `affinity_id`
- `rapporteur`
- `counsel_id`
- `counsel_name_normalized`
- `shared_case_count`
- `favorable_count`
- `unfavorable_count`
- `pair_favorable_rate`
- `minister_baseline_favorable_rate`
- `counsel_baseline_favorable_rate`
- `pair_delta_vs_minister`
- `pair_delta_vs_counsel`
- `red_flag`
- `top_process_classes`
- `generated_at`

#### Estado do artefato
- Builder: `analytics/counsel_affinity.py`
- Serving: `serving_counsel_affinity` (tabela SQLite)
- API: `GET /counsel-affinity`, `GET /counsel-affinity/red-flags`, `GET /ministers/{minister}/counsel-affinity`, `GET /counsels/{counsel_id}/minister-affinity`

---

### 19. `compound_risk`
Representa o índice consolidado de risco de uma entidade (parte ou advogado) que cruza sanções, doações, vínculos corporativos e afinidade num ranking unificado.

#### Campos conceituais
- `entity_id`
- `entity_name`
- `entity_type` (party | counsel)
- `risk_sources`
- `process_ids`
- `process_count`
- `alert_ids`
- `alert_count`
- `max_alert_score`
- `max_rate_delta`
- `red_flag`
- `generated_at`

#### Estado do artefato
- Builder: `analytics/compound_risk.py`
- Serving: `serving_compound_risk` (tabela SQLite)
- API: `GET /compound-risk`, `GET /compound-risk/red-flags`, `GET /compound-risk/heatmap`

---

### 20. `temporal_analysis`
Representa a análise temporal de padrões decisórios ministeriais, incluindo tendências mensais, eventos significativos e cruzamento com rede corporativa.

#### Campos conceituais
- `minister_name`
- `monthly_stats`
- `significant_events`
- `corporate_conflicts_timeline`
- `trend_summary`
- `generated_at`

#### Estado do artefato
- Builder: `analytics/temporal_analysis.py` (com submódulos `_temporal_corporate.py`, `_temporal_events.py`, `_temporal_monthly.py`, `_temporal_utils.py`)
- Serving: `serving_temporal_analysis` (tabela SQLite)
- API: `GET /temporal-analysis`, `GET /temporal-analysis/{minister}`

---

### 21. `minister_flow`
Representa o fluxo decisório de um ministro por recorte filtrado.

#### Campos conceituais
- `minister_name`
- `flow_data`
- `generated_at`

#### Estado do artefato
- Builder: `analytics/minister_flow.py`
- Serving: `serving_minister_flow` (tabela SQLite)
- API: `GET /ministers/{minister}/flow`

---

## Relações principais

- Um `process` pode ter muitos `decision_event`.
- Um `process` pode ter muitas `party`.
- Um `process` pode ter muitos `counsel`.
- Um `decision_event` pode originar zero ou muitos `outlier_alert`.
- Um `outlier_alert` pertence a um `comparison_group`.
- Um `comparison_group` reúne muitos `decision_event`.
- Um `outlier_alert` pode originar zero ou muitos `alert_analysis`.
- Um `origin_context` agrega estatísticas de muitos `process` com mesma origem (via `origin_description`/`origin_court_or_body`).
- Um `party` pode ter zero ou muitos `sanction_match`.
- Um `counsel` pode ter zero ou um `counsel_sanction_profile`.
- Um `party` pode ter zero ou muitos `donation_match`.
- Um `counsel` pode ter zero ou um `counsel_donation_profile`.
- Um ministro pode ter zero ou muitos `corporate_conflict` (via co-participação societária RFB).
- Um `party` ou `counsel` pode ter zero ou muitos `corporate_conflict`.
- Um par (ministro, `counsel`) pode ter zero ou um `counsel_affinity`.
- Uma entidade (party ou counsel) pode ter zero ou um `compound_risk`.
- Um ministro pode ter zero ou um `temporal_analysis`.
- Um ministro pode ter zero ou um `minister_flow`.

---

## Camadas do modelo

### Camada raw
Mantém dados exatamente como recebidos.

### Camada staging
Padroniza nomes de colunas, tipos e formatos.

### Camada curated
Transforma registros em entidades canônicas.

### Camada analytics
Gera features, scores e agregações.

### Camada evidence
Anexa justificativas, comparações e evidências.

### Camada analysis
Produz sínteses e refinamentos opcionais por IA a partir de artefatos já explicados e versionados.

---

## Chaves e identificação

O projeto deve priorizar:

- identificadores estáveis da origem;
- chaves lógicas por processo;
- versionamento de regras de normalização;
- preservação do valor bruto original.

---

## Regras de modelagem

1. Nunca sobrescrever o valor bruto original.
2. Toda normalização deve gerar um campo derivado.
3. Toda inferência deve registrar sua regra ou versão.
4. Toda entidade deve preservar origem e data de validação.
5. Entidades incertas devem ser marcadas explicitamente.

---

## Extensões futuras

A camada textual poderá introduzir novas entidades:

- `document`
- `citation`
- `legal_basis`
- `precedent_reference`
- `argument_span`
- `textual_comparison`

Essas entidades não entram no estado atual como obrigatorias.

`alert_analysis` não integra o pipeline atual. Seu schema já está materializado, mas não há geração, validação de lote ou artefato operacional dessa camada no escopo atual.
