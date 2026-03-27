# Metodologia analítica

## Objetivo

Descrever a lógica analítica do projeto conforme ela está realmente materializada no código atual.

## Princípio central

O sistema não identifica favorecimento, corrupção ou intenção. O sistema identifica padrões, desvios e outliers em relação a grupos comparáveis explicitamente definidos.

## Fluxo metodológico atual

### Etapa 1 — inventário

Catalogar e validar as fontes estruturadas disponíveis.

### Etapa 2 — normalização

Padronizar datas, nomes, papéis, assuntos e chaves.

### Etapa 3 — modelagem de entidades

Separar processo, decisão, parte, advogado e assunto.

### Etapa 4 — análise descritiva

Mapear distribuição por ministro, classe, assunto, período e tipo de decisão.

### Etapa 5 — definição de casos comparáveis

Criar grupos comparáveis com critérios explícitos e versionados.

### Etapa 6 — construção de baselines

Calcular o comportamento esperado dentro de cada grupo válido.

### Etapa 7 — detecção de outliers

Identificar observações que se afastam do baseline por score reprodutível.

### Etapa 8 — explicação

Registrar por que o caso foi marcado e quais limites metodológicos permanecem.

### Etapa 9 — análise derivada opcional

Gerar sínteses assistidas por IA a partir de bundles já materializados, sem alterar o resultado determinístico anterior.

## Regras efetivamente materializadas no código

### Comparabilidade atualmente implementada

O builder de grupos (`src/atlas_stf/analytics/build_groups.py`) usa a regra `comparison-group-v3`, definida sobre a chave:

- `process_class`
- `thematic_key`
- `decision_type`
- `is_collegiate`
- `decision_year`

O `thematic_key` é derivado por:

1. primeiro item não vazio de `subjects_normalized`;
2. fallback para `branch_of_law`;
3. fallback final para `INCERTO`.

Um grupo só é considerado válido quando:

- `case_count >= 5`
- `case_count <= 5000`

Grupos fora desses limites permanecem materializados, mas com `status` bloqueado.

### Baseline atualmente implementado

Para cada grupo válido, o baseline materializa:

- distribuição esperada de `decision_progress`;
- distribuição esperada de `current_rapporteur`;
- distribuição esperada de `judging_body`;
- `favorable_rate` com prior Beta-Binomial (`alpha=1`, `beta=1`), equivalente a Laplace smoothing;
- flag `low_confidence` quando `process_count < 10`;
- distribuição de `decision_progress` estratificada por `process_class`;
- período observado mínimo e máximo do grupo.

O baseline atual usa smoothing apenas para `favorable_rate`. As distribuições categóricas do baseline continuam baseadas nas frequências observadas, sem pesos externos.

### Score atualmente implementado

O score do alerta é a média simples da raridade observada nas dimensões disponíveis:

- `decision_progress`
- `current_rapporteur`
- `judging_body`
- `process_class_outcome`

Parâmetros atualmente codificados:

- limiar de alerta: `0.75`
- baseline considerado pequeno para leitura conclusiva: menos de `10` eventos
- distribuição estratificada por classe só substitui a global se houver pelo menos `5` eventos da classe

### Tipos e status de alerta atualmente implementados

- `alert_type = "atipicidade"` quando não há ressalva estrutural de incerteza
- `alert_type = "inconclusivo"` quando o baseline é pequeno ou o score depende de dimensão insuficiente
- `status = "novo"` para alertas regulares acima do limiar
- `status = "inconclusivo"` para alertas cujo próprio tipo já exige cautela reforçada

### Bundle de evidência atualmente implementado

Cada bundle materializado por `evidence build|build-all` contém:

- `alert`
- `decision_event`
- `process`
- `baseline`
- `comparison_group`
- `score_details`
- `gate_status`
- `analysis_context`
- `analysis_prompts`

Quando existirem artefatos avançados opcionais, o bundle também agrega:

- `rapporteur_profile`
- `sequential_analysis`
- `assignment_audit`

### Risco composto atualmente implementado

O builder de risco composto (`src/atlas_stf/analytics/compound_risk.py`) consolida evidências de múltiplas fontes (sanções, doações, vínculos corporativos e afinidade) num índice unificado por entidade. Para cada entidade:

- agrega todos os processos e alertas vinculados às fontes de risco;
- calcula `max_rate_delta` e `max_alert_score` como indicadores de pico;
- atribui `red_flag` quando qualquer fonte individual já possui red flag;
- permite ranking cruzado e heatmap por ministro.

O compound risk não introduz nova inferência causal — é uma consolidação de indicadores já existentes.

### Análise temporal atualmente implementada

O builder de análise temporal (`src/atlas_stf/analytics/temporal_analysis.py`) produz:

- estatísticas mensais por ministro (volume, taxa de favorabilidade, distribuição de classes);
- detecção de eventos significativos (mudanças abruptas de padrão);
- cruzamento temporal com conflitos corporativos (quando materializados);
- resumo de tendência por ministro.

O módulo é dividido em submódulos (`_temporal_monthly.py`, `_temporal_events.py`, `_temporal_corporate.py`, `_temporal_utils.py`) para manter cada arquivo sob 500 linhas.

### Velocidade decisória atualmente implementada

O builder de velocidade decisória (`src/atlas_stf/analytics/decision_velocity.py`) mede o tempo de tramitação de cada evento decisório em relação ao grupo comparável. Para cada evento:

- calcula `days_elapsed` entre datas relevantes do processo;
- agrupa por `(process_class, thematic_key, year)` com `MIN_GROUP_SIZE=10`;
- calcula percentis P5 e P95 do grupo para detectar anomalias;
- atribui flag `queue_jump` quando o tempo é inferior ao P5 (processo tramitou anormalmente rápido);
- atribui flag `stalled` quando o tempo é superior ao P95 (processo ficou anormalmente parado);
- calcula z-score para medir a magnitude do desvio.

O módulo não infere causa — apenas sinaliza eventos cuja velocidade se afasta significativamente da distribuição do grupo.

### Mudança de relatoria atualmente implementada

O builder de mudança de relatoria (`src/atlas_stf/analytics/rapporteur_change.py`) detecta redistribuições de processos entre ministros e avalia o resultado pós-mudança:

- compara o relator entre eventos decisórios consecutivos do mesmo processo;
- registra cada mudança com relator anterior e novo relator;
- calcula a taxa de decisão favorável pós-mudança para cada par (relator anterior → novo relator);
- compara essa taxa contra o baseline do grupo comparável;
- atribui red flag quando o delta entre taxa pós-mudança e baseline excede 15 pontos percentuais;
- exige mínimo de 2 decisões pós-mudança para considerar o resultado significativo.

### Rede de advogados atualmente implementada

O builder de rede de advogados (`src/atlas_stf/analytics/counsel_network.py`) constrói um grafo de co-clientela entre advogados e identifica clusters suspeitos:

- constrói arestas entre advogados que compartilham clientes (partes);
- filtra com `MIN_SHARED_CLIENTS=2` para reduzir ruído;
- filtra com `MAX_COUNSEL_PER_PARTY=50` para excluir partes institucionais (União, INSS, etc.);
- identifica componentes conexos via BFS (busca em largura);
- calcula `favorable_rate` do cluster e `process_count` agregado;
- atribui red flag quando `favorable_rate > 65%` com `process_count >= 5`.

O módulo usa apenas dados curated (counsel + party), sem dependência de fontes externas.

## Regras gerais

1. Toda comparação deve ter baseline explícito.
2. Todo baseline deve ter critérios de composição documentados.
3. Nenhuma métrica isolada deve produzir conclusão forte.
4. Todo alerta deve permitir inspeção externa e rastreio do racional.
5. Toda análise deve registrar hipóteses alternativas ou limites residuais quando houver fragilidade estrutural.

## Interpretação correta de alertas

Um alerta significa:

`vale verificação externa ou leitura documental adicional, se houver interesse`

Um alerta não significa:

`há irregularidade comprovada`

## Etapa textual posterior

Depois de priorizar casos, o projeto poderá incorporar documentos oficiais para responder perguntas como:

- o fundamento foi compatível com outros casos próximos?
- houve distinguishing explícito?
- houve uso assimétrico de precedente?
- o caso era realmente comparável?

Essa etapa é posterior e depende de coleta documental complementar.

## Análise derivada opcional por IA

O schema `schemas/alert_analysis.schema.json` já existe, mas:

- não há builder obrigatório no pipeline atual;
- não há rotina material obrigatória no estado presente;
- a camada continua opcional e subordinada ao `evidence_bundle`.

Se essa etapa for implementada, o artefato mínimo deve:

- referenciar `alert_id`;
- referenciar a versão do bundle de origem;
- registrar `analysis_model`, `analysis_version` e `prompt_version`;
- preservar nota de incerteza;
- permanecer restrito a síntese descritiva ou comparativa.

## Scoring de grafo de investigação

O serving materializa um grafo de investigação (Camada E) que conecta entidades via arestas tipadas e produz scores decompostos para priorização de revisão humana.

### Traversal modes

- **Strict**: apenas arestas determinísticas (`evidence_strength = "deterministic"`, `traversal_policy = "strict_allowed"`, sem truncação).
- **Broad**: inclui arestas estatísticas, fuzzy, truncadas e inferidas, com penalties proporcionais.

### Componentes do score

O score é aditivo-subtrativo: `raw = documentary + statistical + network + temporal`. Penalties (fuzzy, truncation, singleton, missing_identifier) são subtraídas. `operational_priority = calibrated × min(signal_count, 10)`.

Os pesos atuais são heurísticos (não calibrados empiricamente). O plano de calibração está documentado em `_builder_scoring.py`.

### Workflow de revisão

O endpoint `POST /review/decision` (ADR-006) permite que revisores humanos classifiquem itens da fila como `confirmed_relevant`, `false_positive`, `needs_more_data` ou `deferred`. Reviews não recalibram scores automaticamente na versão atual.

## Critérios de validação

- a documentação metodológica deve continuar coerente com `build_groups.py`, `baseline.py`, `score.py`, `build_alerts.py`, `build_bundle.py`, `compound_risk.py`, `temporal_analysis.py`, `decision_velocity.py`, `rapporteur_change.py`, `counsel_network.py`, `_builder_graph.py` e `_builder_scoring.py`;
- qualquer mudança no limiar, nas dimensões do score ou na chave do grupo exige revisão deste documento;
- nenhum texto metodológico pode afirmar causalidade, intenção ou prova.

## Riscos ou incertezas

- os grupos comparáveis continuam dependentes da qualidade da camada `curated`;
- o `favorable_rate` foi estabilizado com smoothing Beta-Binomial, mas `low_confidence` continua necessário para grupos com menos de `10` processos;
- a suavização atual não altera as distribuições categóricas usadas no score de atipicidade;
- o uso de módulos complementares não elimina a necessidade de leitura documental em casos sensíveis.
