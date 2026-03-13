# Especificação do dashboard

## Objetivo

Descrever os painéis principais do produto, consolidando o que já está materializado na interface e orientando refinamentos posteriores.

## Status

Há interface operacional materializada no repositório. Este documento continua normativo, mas não deve mais ser lido como mera hipótese de produto futuro.

## Dashboard 1 — Visão geral

### Finalidade
Fornecer panorama do corpus.

### Blocos
- total de processos
- total de eventos decisórios
- distribuição por ministro
- distribuição temporal
- distribuição por tipo de decisão
- distribuição por assunto
- distribuição por órgão julgador
- trilha de origem exibida na própria interface

### Filtros
- período
- ministro
- classe
- assunto
- colegialidade

---

## Dashboard 2 — Perfil do ministro

### Finalidade
Avaliar padrão decisório do ministro em diferentes recortes.

### Blocos
- volume por período
- tipos de decisão
- distribuição por assunto
- distribuição por classe
- taxa de certos andamentos
- monocrática vs colegiada
- comparação com baseline agregado

### Saídas
- série temporal
- heatmap
- tabela de destaque

---

## Dashboard 3 — Atores

### Finalidade
Mapear presença de partes e advogados no corpus.

### Blocos
- advogados mais frequentes
- partes mais frequentes
- relações por ministro
- relações por assunto
- relações por tipo de resultado

### Observação
Os dados dessa área são sensíveis a problemas de normalização de nomes.

---

## Dashboard 4 — Alertas

### Finalidade
Exibir casos e subconjuntos atípicos.

### Blocos
- ranking de alertas
- score
- tipo de alerta
- baseline
- padrão esperado
- padrão observado
- nota de incerteza
- vínculo direto com caso e recorte ativo

### Filtros
- ministro
- período
- tipo de alerta
- classe
- assunto
- score mínimo

---

## Dashboard 5 — Caso

### Finalidade
Permitir inspeção detalhada.

### Blocos
- metadados do processo
- sequência de decisões
- partes e advogados
- contexto comparável
- razão do alerta
- links para documentos complementares
- observações analíticas derivadas, quando existirem

## Dashboard 6 — Velocidade decisória

### Finalidade
Identificar anomalias de tempo de tramitação (processos que tramitaram muito rápido ou ficaram parados).

### Blocos
- total de eventos analisados
- eventos com flag fura-fila (queue_jump)
- eventos com flag parado (stalled)
- total de anomalias
- cards por evento com percentis do grupo e z-score

### Filtros
- flag (queue_jump / stalled / todos)
- ministro
- classe processual

---

## Dashboard 7 — Redistribuição de relatoria

### Finalidade
Exibir mudanças de relatoria e avaliar se o resultado pós-redistribuição diverge do baseline.

### Blocos
- total de mudanças de relatoria
- pontos críticos (red flags com delta >15pp)
- cards por mudança com delta e barra de comparação contra baseline

### Filtros
- red flag (sim / todos)
- ministro (anterior ou novo)

---

## Dashboard 8 — Rede de advogados

### Finalidade
Exibir clusters de advogados que compartilham clientes com taxas de resultado anômalas.

### Blocos
- total de clusters identificados
- pontos críticos (red flags com favorable_rate >65%)
- cards por cluster com membros, clientes compartilhados e taxa favorável

### Filtros
- red flag (sim / todos)

---

## Observação de implementação

As páginas centrais de visão geral, alertas, caso, advogados, partes, velocidade decisória, redistribuição e rede de advogados já existem na camada `web/`. O papel deste documento é alinhar evolução funcional e critérios de revisão, não descrever um produto inexistente.

## Regras obrigatórias

1. O dashboard não pode usar linguagem acusatória.
2. Todo score deve ter explicação acessível.
3. Toda visualização deve permitir rastreamento até a origem.
