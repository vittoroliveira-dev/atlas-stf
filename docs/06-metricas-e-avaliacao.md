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

## Snapshot materializado

Os valores operacionais dos artefatos analíticos — contagens de grupos, alertas, matches, red flags e vínculos — são gerados pela execução do pipeline e registrados nos summaries individuais de cada builder (`data/analytics/*_summary.json`).

O snapshot consolidado de release é mantido em `build/release-metrics.json`, gerado automaticamente por `scripts/automation/metrics_snapshot.py`.

Para consultar os valores atuais:
- **Inventário estrutural:** `README.md` (blocos auto-gerenciados)
- **Métricas por artefato:** `data/analytics/*_summary.json`
- **Resumo consolidado:** `build/release-metrics.json`

O threshold de score mínimo para emissão de alertas é `0.75` (definido em `outlier_alert`). Valores abaixo desse limiar são descartados pelo builder.

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
