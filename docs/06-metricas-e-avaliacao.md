# Métricas e avaliação

## Objetivo

Definir como o projeto mede integridade de dados, qualidade analítica e utilidade operacional das saídas materializadas.

## Bloco 1 — métricas de dados

### Completude

Percentual de preenchimento por campo crítico.

Campos críticos:

- processo
- ministro
- data da decisão
- tipo de decisão
- andamento
- assunto
- órgão julgador

### Duplicidade

Percentual de registros duplicados por chave lógica.

### Integridade de join

Percentual de processos do dataset principal que encontram correspondência nos artefatos derivados necessários.

### Consistência temporal

Verificação de coerência entre datas relevantes.

### Normalização de nomes

Taxa de nomes consolidados com sucesso para advogados, partes e assuntos.

## Bloco 2 — métricas analíticas

### Cobertura de grupos comparáveis

Percentual de decisões que entram em ao menos um grupo comparável válido.

### Estabilidade do baseline

Quanto os resultados mudam quando os parâmetros são ajustados.

### Taxa de alertas por grupo

Quantidade de outliers gerados por grupo comparável.

### Distribuição de score

Faixa de dispersão dos scores de atipicidade.

### Explicabilidade

Percentual de alertas com justificativa clara e reproduzível.

## Bloco 3 — métricas de utilidade operacional

### Bundle pronto para análise

Percentual de alertas cujo `gate_status.passes_for_analysis` é verdadeiro.

### Consistência entre camadas

Presença coerente de artefatos opcionais no serving, API e dashboard quando documentados como materializados.

### Reprodutibilidade

Capacidade de diferentes leitores chegarem a entendimento semelhante do racional do alerta.

## Critérios de qualidade

Um alerta de qualidade mínima deve conter:

- grupo comparável explícito
- baseline explícito
- caso sinalizado
- desvio observado
- racional de sinalização
- status analítico e notas de incerteza

## Snapshot materializado atualmente

Os resumos já presentes em `data/analytics/` permitem registrar, como fotografia operacional do workspace:

- `comparison_group_summary.json`:
  - `group_count = 5153`
  - `valid_group_count = 1463`
  - `linked_event_count = 87964`
  - gerado em `2026-03-07T02:10:47+00:00`
- `baseline_summary.json`:
  - `baseline_count = 1463`
  - `event_count_linked = 87964`
  - gerado em `2026-03-07T02:17:16+00:00`
- `outlier_alert_summary.json`:
  - `alert_count = 487`
  - `avg_score = 0.8224772833675565`
  - `threshold = 0.75`
  - `skipped_below_threshold = 87477`
  - gerado em `2026-03-07T02:39:36+00:00`
- `sequential_analysis_summary.json`:
  - `total_analyses = 182`
  - `bias_flagged_count = 151`
  - gerado em `2026-03-08T15:32:22+00:00`
- `sanction_match_summary.json`:
  - `sanction_match_count = 1854`
  - `party_red_flag_count = 105`
  - gerado em `2026-03-08T16:52:36+00:00`
- `donation_match_summary.json`:
  - `donation_match_count = 11972`
  - `party_red_flag_count = 446`
  - gerado em `2026-03-08T17:01:09+00:00`
- `counsel_affinity_summary.json`:
  - `total_pairs_analyzed = 20644`
  - `red_flag_count = 719`
  - gerado em `2026-03-08T22:16:50+00:00`
- `corporate_network_summary.json`:
  - `total_conflicts = 12`
  - `red_flag_count = 0`
  - gerado em `2026-03-09T00:37:26+00:00`

Esses números descrevem o material já derivado no repositório atual. Eles não devem ser tratados como cobertura total do universo de decisões ou entidades.

## Política de avaliação por estágio

### Estágio atual

Foco em:

- integridade de dados
- coerência dos grupos comparáveis
- clareza do score e da explicação
- presença consistente dos artefatos materializados

### Aprofundamentos posteriores

Adicionar:

- verificação externa amostral
- análise derivada opcional por IA
- comparação com camada textual
- análise de robustez do ranking

## Saída esperada

Relatórios de métricas devem ser produzidos por ciclo analítico e versionados na pasta `reports/`, quando aplicável.
