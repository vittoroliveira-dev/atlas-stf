# Roadmap

## Leitura do status

- `concluída`: capacidade já materializada no repositório.
- `em consolidação`: capacidade já existente, mas ainda sendo endurecida, alinhada ou expandida.
- `futura`: capacidade ainda não materializada no estado atual.

## Fase 0 — fundação documental

### Status

`concluída`

### Objetivo

Estabelecer linguagem, escopo, entidades e regras do projeto.

### Entregas

- README
- visão geral
- objetivos e escopo
- fontes de dados
- modelo de dados
- metodologia
- riscos
- governança
- glossário

## Fase 1 — inventário do corpus inicial

### Status

`concluída`

### Objetivo

Consolidar e documentar a base recebida.

### Entregas

- inventário de arquivos
- dicionário de dados
- registro da origem
- validação de duplicidade
- validação de join entre tabelas

## Fase 2 — modelagem canônica

### Status

`concluída`

### Objetivo

Transformar planilhas em entidades estáveis e documentadas.

### Entregas

- processo
- decisão
- parte
- advogado
- assunto
- relações

## Fase 3 — análise descritiva

### Status

`concluída`

### Objetivo

Criar visão estatística básica do corpus.

### Entregas

- perfil por ministro
- perfil por assunto
- perfil por classe
- perfil temporal
- perfil por advogado

## Fase 4 — casos comparáveis e baselines

### Status

`concluída`

### Objetivo

Definir grupos comparáveis e comportamento esperado.

### Entregas

- regra inicial de comparabilidade
- grupos comparáveis iniciais
- baseline por grupo

## Fase 5 — detecção de outliers

### Status

`concluída`

### Objetivo

Localizar sinais de atipicidade relevantes.

### Entregas

- score de atipicidade
- ranking de alertas
- lista de casos prioritários

## Fase 6 — aprofundamento documental

### Status

`em consolidação`

### Objetivo

Anexar evidência técnica e, futuramente, evidência jurídico-textual aos casos mais relevantes.

### Entregas

- bundles de evidência para alertas
- trilha documental
- comparação argumentativa de amostra
- validação de outliers estatísticos

### Observação

A camada de evidência técnica já existe. O aprofundamento jurídico-textual ainda não está completo no estado atual.

## Fase 7 — produto analítico

### Status

`em consolidação`

### Objetivo

Transformar os resultados em interface e relatórios.

### Entregas

- serving database
- API HTTP
- dashboard web
- relatórios por ministro
- relatórios de anomalia
- painel de casos prioritários
- contexto de tribunais de origem via DataJud
- cruzamento com CEIS/CNEP/Leniência da CGU
- cruzamento com doações eleitorais do TSE
- cruzamento com processos sancionadores da CVM
- cruzamento com quadro societário da RFB
- análise de afinidade ministro-advogado
- índice de risco composto (compound risk)
- análise temporal de padrões decisórios
- velocidade decisória (anomalias de tempo de tramitação)
- mudança de relatoria (redistribuição e resultado pós-mudança)
- rede de advogados (clusters com clientes compartilhados)
- linha do tempo processual (andamentos e eventos de sessão do portal STF)
- normalização TPU (Tabelas Processuais Unificadas do CNJ)
- análise de janelas temporais precisas (procedural timeline)
- detecção de anomalia de sessão por ministro (pauta anomaly)
- rede de representação processual (advogados, escritórios, arestas, eventos)
- grafo de investigação materializado (nós, arestas tipadas, paths, scoring decomposto)
- fila de revisão humana com status rastreáveis (ADR-006)
- vínculos corporativos indiretos de sancionados (CEIS/CVM→RFB→STF)
- contrapartes de pagamento de órgãos partidários TSE
- agenda ministerial (fetch GraphQL + eventos + exposição temporal)
- contratos de schema e drift analysis por fonte

### Observação

O produto analítico já possui implementação materializada. O foco atual deixou de ser criar a interface do zero e passou a ser consolidar, alinhar e expandir a superfície pública existente.

## Fase 8 — consolidação operacional

### Status

`em consolidação`

### Objetivo

Reduzir lacunas entre documentação, contratos públicos, artefatos materializados e experiência do produto.

### Entregas

- documentação pública sincronizada com API e dashboard
- ativos visuais estáveis do produto para uso institucional
- endurecimento de fluxos de execução local
- revisão contínua de checklists de interface e governança

### Observação

O alinhamento documental atual já entra nesta fase, mas ela ainda não pode ser tratada como concluída porque a sincronização precisa acompanhar cada expansão dos módulos opcionais.

## Fase 9 — análise derivada opcional

### Status

`futura`

### Objetivo

Adicionar camadas derivadas opcionais, sem substituir o pipeline determinístico.

### Entregas

- análise derivada por IA sobre bundles válidos
- sínteses comparativas derivadas
- artefatos `alert_analysis`
- controles adicionais de versionamento de prompt e modelo

## Snapshot operacional atual

- pipeline-base determinístico: `concluída`
- serving database: `concluída`
- API e dashboard auditáveis: `em consolidação`
- módulos opcionais CGU, TSE, CVM, DataJud, RFB, afinidade, compound risk, temporal, decision velocity, rapporteur change, counsel network, procedural timeline, pauta anomaly, representação, grafo de investigação, agenda ministerial, contratos e SCL: `materializados com dependência de dados locais`
- análise derivada por IA sobre bundles: `futura`

## Critério de passagem de fase

Cada fase só avança se:

- as hipóteses estiverem documentadas;
- as limitações estiverem explícitas;
- as entregas estiverem versionadas;
- houver validação mínima da fase anterior;
- não houver contradição entre documentação, contratos e estado real do repositório.
