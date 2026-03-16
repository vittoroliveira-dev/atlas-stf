# Glossário

## Alerta
Registro analítico que indica um caso ou subconjunto que pode merecer verificação externa ou leitura documental adicional.

## Atipicidade
Desvio observável em relação a um baseline definido.

## Baseline
Padrão esperado em um grupo comparável.

## Camada raw
Nível em que os dados são preservados exatamente como recebidos.

## Camada staging
Nível em que os dados passam por limpeza e padronização mínima.

## Camada curated
Nível em que entidades canônicas são produzidas.

## Casos comparáveis
Conjunto de casos agrupados segundo critérios explícitos de proximidade processual e decisória.

## Corpus inicial
Conjunto de arquivos estruturados recebidos do portal de transparência.

## Decisão-evento
Registro de um fato decisório vinculado a um processo.

## Descritivo
Tipo de análise focada em distribuição, contagem e perfil.

## Divergência aparente
Diferença observável que ainda não foi validada por camada textual complementar ou verificação externa.

## Evidência
Conjunto de informações que sustenta uma análise ou alerta.

## Grupo comparável
Subconjunto formalizado de casos usados para comparação.

## Inconclusivo
Rótulo usado quando os dados não sustentam conclusão útil.

## INCERTO
Marcador obrigatório para afirmações ou campos não comprovados.

## Outlier
Observação que se afasta de forma relevante do padrão esperado.

## Parte
Pessoa física, jurídica ou ente processual associado ao processo.

## Priorização
Ordenação de casos com base em relevância para aprofundamento documental ou inspeção externa.

## Processo
Unidade jurídica principal do corpus.

## Verificação externa
Leitura adicional, opcional e fora do fluxo do sistema, feita por quem desejar inspecionar um alerta com mais profundidade.

## Score de atipicidade
Medida sintética de desvio em relação ao baseline.

## Trilha de auditoria
Conjunto de registros que permite reconstruir origem, regra e racional de uma saída.

## Sanção (sanction_match)
Registro de cruzamento entre uma parte processual do STF e uma sanção pública (CEIS, CNEP, Leniência da CGU ou processo sancionador da CVM).

## Doação (donation_match)
Registro de cruzamento entre uma parte processual do STF e um doador de campanha eleitoral registrado no TSE.

## Vínculo corporativo (corporate_conflict)
Registro de co-participação societária entre um ministro do STF e uma parte ou advogado, detectado via dados abertos de CNPJ da Receita Federal.

## Afinidade ministro-advogado (counsel_affinity)
Par (ministro, advogado) cuja taxa de vitória observada se afasta significativamente do baseline individual de cada um, derivado apenas de dados curated internos.

## Risco composto (compound_risk)
Índice consolidado que agrega evidências de sanções, doações, vínculos corporativos e afinidade num ranking unificado por entidade.

## Análise temporal (temporal_analysis)
Análise de padrões decisórios ministeriais ao longo do tempo, incluindo tendências mensais, eventos significativos e cruzamento com rede corporativa.

## Red flag
Indicador binário de que uma entidade ou relação apresenta combinação de sinais que merece atenção prioritária. Não equivale a irregularidade comprovada.

## Serving database
Banco SQLite derivado (24 tabelas) que materializa artefatos curated e analytics para consumo pela API e pelo dashboard.

## Contexto de origem (origin_context)
Agregação estatística de tribunais de origem derivada da API CNJ DataJud, usada para contextualizar a procedência dos processos que chegam ao STF.

---

## Termos técnicos (campos e identificadores)

Cada entrada descreve o que o termo significa **dentro do Atlas STF** — não é tradução literal, mas explicação contextual do papel que o campo ou conceito exerce no pipeline.

### Identidade e normalização

- **entity_type** — Papel da entidade no processo do STF: `party` (parte processual) ou `counsel` (advogado/representante legal). Determina em qual tabela curated a entidade é buscada para cruzamento.
- **identity_key** — Chave composta que identifica univocamente uma entidade. Formato `tax:DÍGITOS` quando CPF/CNPJ está disponível, ou `name:NOME_CANÔNICO` como fallback. Usada para deduplicação entre fontes distintas (TSE, CGU, CVM, RFB).
- **identity_strategy** — Método pelo qual a identity_key foi resolvida: `tax_id` (via CPF/CNPJ), `name` (via nome canônico), `oab` (via número OAB para advogados). Registrada para auditabilidade da vinculação.
- **donor_identity_key** — Chave de identidade específica para doadores TSE. Formato `cpf:DÍGITOS` quando CPF/CNPJ válido está presente, ou `name:NOME` como fallback. Permite agregar doações da mesma pessoa mesmo quando registradas com variações de nome.
- **stable_id** — Hash SHA-256 determinístico com prefixo (ex: `counsel_`, `party_`). Dado o mesmo input, sempre gera o mesmo ID — garante que re-execuções do pipeline não criam entidades duplicadas.
- **normalize_entity_name** — Converte nomes para maiúsculas e colapsa espaços múltiplos. Ex: `"  João da   Silva "` → `"JOÃO DA SILVA"`.
- **normalize_tax_id** — Extrai apenas dígitos de CPF/CNPJ. Ex: `"123.456.789-00"` → `"12345678900"`.
- **canonicalize_entity_name** — Vai além de normalize: remove sufixos corporativos (SA, LTDA, ME, etc.), pontuação e acentos. Usado para matching fuzzy onde `"CONSTRUTORA ABC S.A."` precisa casar com `"CONSTRUTORA ABC"`.
- **strip_accents** — Remove diacríticos preservando caracteres base. Ex: `"João"` → `"Joao"`. Usado internamente pela cascata de matching.

### Estatística

- **chi2_statistic** — Estatística qui-quadrado de Pearson. Mede quanto a distribuição observada de decisões de um relator diverge do esperado pelo grupo comparável. Valores altos indicam desvio significativo.
- **p_value_approx** — P-valor aproximado via tabelas de lookup (retorna 0,001, 0,01, 0,05 ou 1,0). Indica a probabilidade de observar o desvio por acaso. Usado para filtrar alertas com significância estatística.
- **z_score** — Número de desvios-padrão em relação à média do grupo. Usado na velocidade decisória: um `velocity_z_score` de −2,5 indica decisão 2,5 desvios mais rápida que o normal.
- **autocorrelation_lag1** — Autocorrelação de lag-1 da série binária de decisões (favorável=1, desfavorável=0). Valores altos sugerem que o resultado de uma decisão influencia a próxima — possível viés sequencial.
- **beta_binomial_posterior_mean** — Média posterior bayesiana com suavização de Laplace. Evita que relator com 1 decisão em 1 caso apareça com 100% de taxa favorável — puxa em direção a 50% quando a amostra é pequena.
- **odds_ratio** — Razão de chances entre dois grupos numa tabela 2×2. Usado para comparar taxa de resultado favorável entre entidades sancionadas vs. não-sancionadas.
- **red_flag_power** — Poder estatístico (probabilidade de detectar um desvio real de 15 p.p. na taxa favorável). Calculado via teste z unilateral. Valores ≥ 0,80 = confiança alta; ≥ 0,50 = moderada; < 0,50 = baixa. Não bloqueia o red flag — apenas qualifica a solidez da evidência.
- **red_flag_confidence** — Rótulo derivado do power: `high` (≥ 0,80), `moderate` (≥ 0,50), `low` (< 0,50). Aparece no serving e na API para que o consumidor saiba quão confiável é o red flag.
- **favorable_rate** — Proporção de decisões com resultado favorável à parte ou entidade analisada. É a métrica central de comparação do projeto.
- **baseline_favorable_rate** — Taxa favorável esperada para o grupo comparável (mesma classe processual, tema, tipo de decisão, órgão julgador, ano). Serve como referência contra a qual a taxa observada é comparada.
- **favorable_rate_delta** — Diferença entre a taxa observada e o baseline (`favorable_rate − baseline_favorable_rate`). Delta positivo indica taxa acima do esperado; negativo, abaixo.
- **favorable_rate_substantive** — Taxa favorável calculada apenas sobre decisões de mérito (substantive), excluindo decisões processuais e liminares. Mais conservadora e resistente a ruído processual.
- **substantive_decision_count** — Quantidade de decisões classificadas como "substantive" (de mérito). Quando zero, a favorable_rate_substantive não pode ser calculada.
- **breakpoint_score** / **breakpoint_flag** — Detecção de mudança estrutural na série temporal de decisões. Score alto + flag = ruptura estatisticamente significativa no padrão decisório de um ministro.
- **deviation_flag** / **deviation_direction** — Indica se o perfil de um relator desvia do grupo comparável (`deviation_flag=True`) e em qual direção (`over` = acima do esperado, `under` = abaixo).
- **rolling_favorable_rate_6m** — Média móvel de 6 meses da taxa favorável. Suaviza variações mensais para revelar tendências.
- **delta_vs_prior_year** / **delta_vs_overall** — Variação da taxa favorável em relação ao ano anterior e em relação ao acumulado geral do relator, respectivamente.

### Correspondência e vinculação (matching)

- **match_id** — Identificador determinístico de um cruzamento entre entidade STF e fonte externa (TSE, CGU, CVM). Gerado por hash do par (entidade, registro externo).
- **match_strategy** — Algoritmo que produziu o cruzamento. Cascata de 6 passos: `tax_id` (CPF/CNPJ exato) → `alias` (nome alternativo) → `exact` (nome normalizado idêntico) → `canonical_name` (nome canônico sem sufixos) → `jaccard` (similaridade de tokens ≥ 0,8) → `levenshtein` (distância de edição ≤ 2). Registrada para auditabilidade.
- **match_score** — Score numérico de similaridade. 1,0 para estratégias determinísticas; variável (0,0–1,0) para fuzzy. Quanto maior, mais confiável o cruzamento.
- **match_confidence** — Rótulo categórico derivado da estratégia: `deterministic` (tax_id), `exact_name` (alias/exact/canonical), `fuzzy` (jaccard/levenshtein), `nominal_review_needed` (ambíguo).
- **matched_alias** — Nome alternativo que resolveu o cruzamento quando a estratégia foi `alias`. Permite rastrear qual variação do nome foi decisiva.
- **matched_tax_id** — CPF/CNPJ que resolveu o cruzamento quando a estratégia foi `tax_id`. Prova documental inequívoca.
- **jaccard_similarity** / **jaccard_min** — Similaridade de Jaccard entre conjuntos de tokens dos nomes. Threshold mínimo: 0,8 (80% de tokens em comum). Ex: `{"CONSTRUTORA", "ABC"}` vs. `{"CONSTRUTORA", "ABC", "SA"}` = 0,67 (reprovado).
- **levenshtein_distance** / **levenshtein_max** — Distância de edição entre nomes canônicos (inserções, deleções, substituições). Threshold máximo: 2 caracteres. Usado como fallback quando Jaccard falha.
- **is_ambiguous** — Indica que o cruzamento resolveu para múltiplos candidatos com score idêntico. O match é registrado, mas com `match_confidence = nominal_review_needed`.
- **uncertainty_note** — Texto explicativo do motivo da ambiguidade. Ex: `"multiple_candidates_same_jaccard_score"`, `"name_only_match_no_tax_id"`.

### Red flags e risco

- **red_flag** — Booleano indicando que a combinação de sinais observados merece atenção prioritária. Não equivale a irregularidade comprovada — é um filtro de priorização para verificação humana.
- **red_flag_substantive** — Red flag calculado apenas com decisões de mérito (substantive). Mais conservador: exige que o desvio persista mesmo excluindo decisões processuais e liminares.
- **signal_count** — Quantidade de tipos distintos de sinal de risco ativos para uma entidade no compound_risk: alert, sanction_match, donation_match, corporate_conflict, affinity. Mínimo de 2 para emissão de red flag composto.
- **signals** — Lista dos tipos de sinal ativos. Ex: `["sanction_match", "corporate_conflict"]`.
- **signal_details** — JSON com metadados de cada sinal ativo (fonte, score, data, processo).
- **risk_score** — Score numérico de risco no conflito corporativo. Combina grau de proximidade societária (`link_degree`) com decaimento temporal (`decay_factor`).
- **decay_factor** — Fator de decaimento temporal aplicado ao risk_score de vínculos corporativos. Vínculos mais antigos têm peso menor, refletindo que uma participação societária encerrada há anos é menos relevante.
- **corporate_link_red_flag** — Red flag específico para quando um doador TSE tem proximidade corporativa (via quadro societário RFB) com um ministro do STF.
- **alert_type** / **alert_score** — Tipo de alerta estatístico (ex: desvio de distribuição, outlier de taxa) e sua severidade numérica.

### Doações eleitorais — TSE

- **donor_cpf_cnpj** — CPF ou CNPJ do doador conforme registrado no TSE. Pode conter formatação, estar mascarado (`***123456**`) ou vazio nos dados mais antigos.
- **donor_name_normalized** — Nome do doador após normalização (maiúsculas + colapso de espaços). Usado para matching quando CPF/CNPJ não está disponível.
- **donor_name_originator** — Nome do doador original quando a doação é intermediada (ex: comitê financeiro repassa recurso de um doador). Permite rastrear a origem real do recurso.
- **total_donated_brl** — Valor total doado em reais (BRL) por um doador, acumulado em todos os ciclos eleitorais.
- **donation_count** — Número total de registros de doação individuais de um doador no corpus TSE.
- **election_years** — Conjunto de anos eleitorais em que o doador fez contribuições (ex: `[2010, 2014, 2018]`).
- **parties_donated_to** / **candidates_donated_to** / **positions_donated_to** — Conjuntos de partidos, candidatos e cargos para os quais o doador contribuiu, respectivamente.
- **first_donation_date** / **last_donation_date** — Datas extremas das doações. Permitem avaliar se o doador era ativo antes ou depois do processo no STF.
- **active_election_year_count** — Quantidade de ciclos eleitorais distintos com atividade. Doador presente em 5+ ciclos indica engajamento político consistente.
- **max_single_donation_brl** / **avg_donation_brl** — Maior doação individual e média por doação. Ajudam a distinguir grandes doadores de pequenos contribuintes.
- **top_candidate_share** / **top_party_share** / **top_state_share** — Proporção do total doado concentrada no candidato/partido/estado principal (0–1). Valores altos indicam concentração; baixos, dispersão.
- **donation_year_span** — Intervalo em anos entre o primeiro e último ciclo eleitoral com doação. Mede longevidade do engajamento.
- **recent_donation_flag** — Booleano indicando se o doador contribuiu nos dois ciclos eleitorais mais recentes do corpus.
- **resource_type_category** — Classificação do tipo de recurso da doação: `payment_method` (meio de pagamento), `source_type` (origem do recurso), `in_kind` (bens/serviços), `empty` (campo vazio), `unknown` (não classificável).
- **resource_type_subtype** — Subtipo específico dentro da categoria. Ex: `cash`, `party_fund`, `campaign_material`, `professional_service`.
- **donor_document_type** — Tipo de documento do doador após validação: `cpf` (pessoa física), `cnpj` (pessoa jurídica), `unknown` (não determinável).
- **donor_cnpj_basico** — Primeiros 8 dígitos do CNPJ do doador (raiz da empresa). Usado para vincular o doador ao quadro societário da RFB e ao grupo econômico.
- **donor_company_name** — Razão social do doador obtida via cruzamento com a base RFB, quando o doador é pessoa jurídica.

### Identidade corporativa e rede

- **company_cnpj_basico** — Raiz de 8 dígitos do CNPJ que identifica a empresa (sem filial/dígito verificador). Base para agrupamento de estabelecimentos e detecção de grupo econômico.
- **economic_group_id** — Identificador do grupo econômico detectado pelo algoritmo Union-Find. Empresas que compartilham sócios PJ são agrupadas transitivamente — se A e B compartilham sócio, e B e C também, então {A, B, C} formam um grupo.
- **economic_group_member_count** — Quantidade de CNPJs (raiz) no grupo econômico.
- **is_law_firm_group** — Booleano indicando que o grupo econômico contém pelo menos uma empresa com natureza jurídica de sociedade de advogados. Usado como multiplicador (×1,5) no compound_risk.
- **has_minister_partner** / **has_party_partner** / **has_counsel_partner** — Booleanos indicando que alguma empresa do grupo econômico tem como sócio, respectivamente: um ministro do STF, uma parte processual, ou um advogado. `has_minister_partner` é multiplicador ×2,0 no compound_risk.
- **min_link_degree_to_minister** — Menor número de saltos (hops) no grafo societário entre o doador/entidade e um ministro do STF. 1 = sócio direto; 2 = compartilham um sócio intermediário.
- **link_degree** — Número de saltos no caminho societário entre um ministro e uma parte/advogado no conflito corporativo.
- **link_chain** — JSON descrevendo a sequência de entidades no caminho societário. Ex: `["ministro → empresa A → sócio PJ → empresa B → parte"]`.
- **establishment_count** / **active_establishment_count** — Quantidade total e ativa de estabelecimentos (filiais) vinculados ao CNPJ raiz.
- **headquarters_uf** / **headquarters_cnae_fiscal** / **headquarters_cnae_label** — UF, código e descrição da atividade econômica principal (CNAE) da sede da empresa.
- **evidence_type** / **evidence_strength** — Tipo de evidência que sustenta o conflito corporativo (`direct_partnership`, `shared_company`) e sua força (`strong`, `moderate`, `weak`).

### Sanções

- **sanction_source** — Base de dados de origem da sanção: `cgu` (CEIS/CNEP/Leniência da CGU) ou `cvm` (processos sancionadores da CVM).
- **sanctioning_body** — Órgão que aplicou a sanção (ex: `CGU`, `IBAMA`, `TCU`, `CVM`).
- **sanction_type** — Categoria da sanção (ex: `Impedimento`, `Suspensão`, `Inidoneidade`, `Multa`).
- **sanction_start_date** / **sanction_end_date** — Período de vigência da sanção. Sanção ativa durante o processo STF tem mais peso.
- **sanction_description** — Descrição textual da sanção conforme registrada pela CGU/CVM.

### Contrapartes de pagamento

- **counterparty_id** — Identificador determinístico da contraparte (fornecedor/prestador que recebeu pagamento de órgão partidário).
- **counterparty_identity_key** — Chave de identidade da contraparte, análoga à `donor_identity_key`. Formato `tax:DÍGITOS` ou `name:NOME`.
- **identity_basis** — Como a chave foi resolvida: `tax_id` (CNPJ/CPF presente) ou `name` (apenas nome disponível).
- **total_received_brl** / **payment_count** — Valor total recebido em BRL e quantidade de pagamentos distintos.
- **payer_parties** — Conjunto de partidos políticos que efetuaram pagamentos à contraparte.
- **payer_actor_type** — Tipo do pagador. Atualmente sempre `party_org` (órgão partidário — diretório estadual/municipal/nacional).

### Velocidade decisória

- **days_to_decision** — Dias corridos entre a data de autuação (`filing_date`) e a data da decisão (`decision_date`). Métrica central de celeridade/morosidade.
- **filing_date** — Data em que o processo foi protocolado/autuado no STF.
- **group_size** — Quantidade de decisões no grupo comparável (mesma classe + tema + ano). Amostras pequenas reduzem a confiabilidade dos percentis.
- **p5_days** / **p10_days** / **median_days** / **p90_days** / **p95_days** — Distribuição de percentis do tempo de tramitação no grupo comparável. Decisão abaixo de p5 é excepcionalmente rápida; acima de p95, excepcionalmente lenta.
- **velocity_flag** — Classificação da velocidade: `fast` (abaixo de p10), `slow` (acima de p90), ou nulo (dentro da faixa normal).
- **velocity_z_score** — Z-score do `days_to_decision` em relação ao grupo. Valores negativos indicam decisão mais rápida que o normal; positivos, mais lenta.

### Relator e fluxo

- **current_rapporteur** — Ministro relator atual do processo no momento da decisão.
- **previous_rapporteur** / **new_rapporteur** — Relator antes e depois de uma redistribuição.
- **change_date** — Data em que ocorreu a redistribuição de relatoria.
- **post_change_decision_count** / **post_change_favorable_rate** — Quantidade de decisões e taxa favorável após a redistribuição. Permite avaliar se a mudança de relator alterou o padrão de resultado.
- **new_rapporteur_baseline_rate** / **delta_vs_baseline** — Taxa favorável esperada do novo relator e diferença entre observado e esperado.
- **monocratic_event_count** / **monocratic_favorable_rate** — Decisões e taxa favorável em decisões monocráticas (relator decide sozinho).
- **collegiate_event_count** / **collegiate_favorable_rate** — Decisões e taxa favorável em decisões colegiadas (turma ou plenário).
- **monocratic_blocking_flag** — Indica que o padrão monocrático de um relator (alta taxa de indeferimento) suprime sistematicamente a chegada de casos ao colegiado.
- **sequential_bias_flag** — Indica autocorrelação significativa na série de decisões: o resultado de uma decisão pode estar influenciando a próxima (efeito de streak).

### Afinidade e rede de advogados

- **affinity_id** — Identificador do par (ministro, advogado) na análise de afinidade.
- **shared_case_count** — Número de processos em que o ministro e o advogado co-ocorrem (ministro como relator, advogado como representante de parte).
- **pair_favorable_rate** — Taxa favorável observada especificamente quando esse par atua junto.
- **minister_baseline_favorable_rate** / **counsel_baseline_favorable_rate** — Taxas favoráveis individuais de cada um (ministro com todos os advogados; advogado com todos os ministros).
- **pair_delta_vs_minister** / **pair_delta_vs_counsel** — Quanto a taxa do par excede (ou fica abaixo) do baseline individual de cada um. Se ambos deltas são positivos e significativos, sugere afinidade.
- **cluster_id** / **cluster_size** — Identificador e tamanho de um cluster na rede de advogados. Advogados que compartilham muitos clientes em comum são agrupados.
- **shared_client_count** — Número de clientes (partes processuais) compartilhados entre advogados do cluster.
- **cluster_favorable_rate** / **cluster_case_count** — Taxa favorável e número de processos do cluster como um todo.

### Rede de representação

- **lawyer_id** — Identificador determinístico de um advogado (hash estável do nome + OAB).
- **lawyer_name_raw** / **lawyer_name_normalized** / **canonical_name_normalized** — Nome conforme registrado, após normalização básica, e na forma canônica (sem acentos, sem sufixos), respectivamente.
- **oab_number** / **oab_state** — Número de inscrição na OAB e seccional (ex: `123456/SP`).
- **oab_status** — Status da inscrição: `active`, `suspended`, `cancelled`, `not_found`, `unknown`.
- **oab_validation_method** — Como o status foi verificado: `cna` (Cadastro Nacional de Advogados), `cnsa` (Cadastro Nacional de Sociedades de Advogados), `null` (não validado).
- **firm_id** — Identificador determinístico de um escritório de advocacia.
- **cnpj** / **cnpj_valid** — CNPJ do escritório e booleano indicando se passa na validação de dígitos verificadores.
- **cnsa_number** — Número no Cadastro Nacional de Sociedades de Advogados da OAB.
- **edge_id** — Identificador de uma aresta na rede de representação (advogado/escritório → processo/parte).
- **representative_kind** — Tipo de representante: `lawyer` ou `firm`.
- **role_type** — Papel do representante no processo (ex: `advogado`, `amicus_representative`, `curador`).
- **confidence** — Grau de confiança na aresta de representação (0–1). Arestas derivadas de parsing de nome têm confiança menor que arestas confirmadas por OAB.

### Proveniência e auditoria

- **record_hash** — Hash SHA-256 do registro-fonte original (antes de qualquer transformação). Usado para deduplicação: se o mesmo registro aparece em dois arquivos TSE, o hash garante que não será contado duas vezes.
- **source_file** — Nome do arquivo dentro do ZIP/CSV de origem (ex: `consulta_cand_2018_SP.csv`). Rastreia exatamente de onde veio cada registro.
- **source_url** — URL de download do arquivo-fonte (ex: URL do repositório TSE). Permite reproduzir a coleta.
- **collected_at** — Timestamp ISO de quando o dado foi coletado/baixado pelo pipeline.
- **ingest_run_id** — UUID que identifica uma execução específica do pipeline de ingestão. Permite saber quais registros entraram juntos.
- **schema_version** — Versão inteira do schema do banco serving (atualmente 14). Incrementada quando a estrutura de tabelas muda.
- **schema_fingerprint** — Hash SHA-256 da estrutura completa dos modelos SQLAlchemy. Detecta mudanças não-versionadas no schema.
- **generated_at** / **matched_at** / **built_at** — Timestamps de quando o artefato analytics/match/banco foi produzido.

### Classificação de resultado

- **favorable** / **unfavorable** / **neutral** — Categorias de resultado decisório. `favorable` = provido, deferido, procedente, concedido. `unfavorable` = desprovido, indeferido, improcedente, denegado. `neutral` = prejudicado, extinto, baixa sem resolução. Classificação invertida para partes passivas (respondente).
- **substantive** / **procedural** / **provisional** / **unknown** — Subcategorias de materialidade. `substantive` = decisão de mérito efetivo (provido/desprovido em recurso, procedente/improcedente em ação). `procedural` = admissibilidade ou extinção sem mérito (não conhecido, desistência). `provisional` = tutela de urgência (liminar, ad referendum). `unknown` = não classificável.
- **is_collegiate** — Booleano indicando se a decisão foi tomada por órgão colegiado (turma ou plenário) ou monocrática (relator sozinho).
- **judging_body_category** — Classificação granular do órgão julgador: `plenario_virtual`, `plenario` (presencial), `turma`, `monocratico`, `colegiado_outro`, `incerto`.

### Classificação de recurso TSE

- **payment_method** — Meio de pagamento da doação: `cash` (espécie), `check` (cheque), `estimated` (estimado), `not_informed` (não informado).
- **source_type** — Origem do recurso: `individual` (pessoa física), `corporate` (pessoa jurídica, pré-2015), `own_resources` (recursos próprios do candidato), `party_transfer`/`committee_transfer` (repasse partidário/de comitê), `party_fund`/`campaign_fund` (fundos públicos), `internet` (financiamento coletivo online).
- **in_kind** — Doações em bens/serviços: `campaign_material`, `professional_service`, `transport_fuel`, `media_communication`, `food_beverage`, etc.

### Métricas empíricas

- **homonymy_proxy_count** / **homonymy_proxy_rate** — Quantidade e proporção de identity_keys resolvidas apenas por nome que possuem 2+ CPF/CNPJs distintos no corpus TSE. Proxy de homonímia: indica possíveis falsos positivos no matching por nome.
- **cpf_cnpj_masked_count** / **cpf_cnpj_masked_rate** — Registros TSE com CPF/CNPJ mascarado (ex: `***123456**`). Não podem ser usados para matching determinístico.
- **identity_key_cpf_rate** — Proporção de identity_keys resolvidas via CPF/CNPJ (vs. fallback por nome). Quanto maior, mais confiável o corpus de matches.
- **unique_identity_keys_count** — Total de chaves de identidade distintas no corpus. Proxy do número real de doadores únicos.
