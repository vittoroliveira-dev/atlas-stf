# Registro de fontes

## Finalidade

Este registro mantém a lista oficial de fontes utilizadas no projeto, com finalidade operacional, limitações e status de validação.

## Modelo de registro

### Fonte 001
- `source_id`: STF-TRANSP-REGDIST
- `nome`: Portal de Transparência do STF — Registro e Distribuição
- `categoria`: estruturada primária
- `finalidade`: base processual-estatística do corpus inicial
- `formato`: exportação tabular
- `acesso`: público
- `cobertura_aparente`: a confirmar por exportação
- `periodicidade`: `INCERTO`
- `restricoes`: nenhuma identificada no uso consultivo inicial
- `limitacoes`: pode refletir recorte filtrado; não substitui fundamentação textual
- `ultima_validacao`: `INCERTO`
- `observacoes`: registrar filtro utilizado em cada exportação

### Fonte 002
- `source_id`: STF-JURIS
- `nome`: Pesquisa de Jurisprudência do STF
- `categoria`: textual oficial complementar
- `finalidade`: enriquecimento complementar do `curated` e validação jurídica posterior
- `formato`: consulta pública
- `acesso`: público
- `cobertura_aparente`: acórdãos, decisões monocráticas publicadas, súmulas e informativos
- `periodicidade`: contínua
- `restricoes`: coleta e parsing específicos
- `limitacoes`: cobertura operacional por tipo de peça a validar
- `ultima_validacao`: `INCERTO`
- `observacoes`: já usada como enriquecimento complementar de `process` e `decision_event`; manter casamento textual como `INCERTO` quando não houver correspondência segura

### Fonte 003
- `source_id`: STF-DJE
- `nome`: Diário de Justiça Eletrônico do STF
- `categoria`: textual oficial complementar
- `finalidade`: trilha documental e confirmação de publicação
- `formato`: publicação oficial
- `acesso`: público
- `cobertura_aparente`: atos publicados
- `periodicidade`: contínua
- `restricoes`: parsing dependente do formato
- `limitacoes`: não substitui leitura integral do processo
- `ultima_validacao`: `INCERTO`
- `observacoes`: utilizar em ciclos de aprofundamento documental

### Fonte 004
- `source_id`: CNJ-DATAJUD
- `nome`: CNJ DataJud — API Pública
- `categoria`: estruturada complementar
- `finalidade`: contexto agregado de tribunais de origem
- `formato`: API REST (Elasticsearch), JSON
- `acesso`: público (requer API key gratuita)
- `cobertura_aparente`: todos os tribunais exceto STF
- `periodicidade`: contínua (atualização diária dos tribunais)
- `restricoes`: rate limiting, sem dados do STF
- `limitacoes`: matching apenas agregado por UF/tribunal, não por processo individual
- `ultima_validacao`: 2026-03-07 (teste com API key funcional)
- `observacoes`: módulo opcional; dados salvos em `data/raw/datajud/`

### Fonte 005
- `source_id`: CGU-CEIS-CNEP-LENIENCIA
- `nome`: CGU — CEIS, CNEP e Acordos de Leniência (Portal da Transparência)
- `categoria`: estruturada complementar
- `finalidade`: detecção de partes sancionadas que litigam no STF
- `formato`: CSV bulk download (primário) + API REST (fallback)
- `acesso`: público (CSV sem auth; REST requer `CGU_API_KEY`)
- `cobertura_aparente`: sanções administrativas federais (CEIS ~22.5k, CNEP ~1.5k, Leniência ~146 registros)
- `periodicidade`: diária
- `restricoes`: matching por nome normalizado, sem CPF/CNPJ no curated
- `limitacoes`: cobertura limitada a sanções administrativas federais
- `ultima_validacao`: 2026-03-08
- `observacoes`: módulo opcional; dados em `data/raw/cgu/`

### Fonte 006
- `source_id`: TSE-DOACOES
- `nome`: TSE — Prestação de Contas Eleitorais (Receitas de Candidatos)
- `categoria`: estruturada complementar
- `finalidade`: detecção de partes doadoras de campanha que litigam no STF
- `formato`: CSV bulk download (CDN TSE)
- `acesso`: público (sem auth, sem rate limit)
- `cobertura_aparente`: doações eleitorais 2002–2024
- `periodicidade`: por ciclo eleitoral
- `restricoes`: matching por nome normalizado; variações de colunas entre anos
- `limitacoes`: cobertura limitada a receitas declaradas; doações PJ proibidas desde 2015
- `ultima_validacao`: 2026-03-08
- `observacoes`: módulo opcional; dados em `data/raw/tse/`

### Fonte 007
- `source_id`: CVM-SANCIONADORES
- `nome`: CVM — Processos Administrativos Sancionadores
- `categoria`: estruturada complementar
- `finalidade`: detecção de partes punidas no mercado de capitais que litigam no STF
- `formato`: CSV (ZIP único)
- `acesso`: público (sem auth)
- `cobertura_aparente`: processos sancionadores a partir da fase de citação
- `periodicidade`: diária
- `restricoes`: matching por nome normalizado
- `limitacoes`: não inclui inquéritos preliminares
- `ultima_validacao`: 2026-03-08
- `observacoes`: módulo opcional; integrado ao fluxo de sanções CGU; dados em `data/raw/cvm/`

### Fonte 008
- `source_id`: RFB-CNPJ
- `nome`: RFB — Dados Abertos CNPJ (Quadro Societário e Empresas)
- `categoria`: estruturada complementar
- `finalidade`: detecção de vínculos corporativos entre ministros e partes/advogados
- `formato`: CSV bulk download (ZIPs sem cabeçalho, `;`, ISO-8859-1)
- `acesso`: público (sem auth, sem rate limit)
- `cobertura_aparente`: todas as empresas brasileiras ativas e inativas
- `periodicidade`: mensal
- `restricoes`: matching por nome normalizado; volume elevado (~50M+ linhas)
- `limitacoes`: dados cadastrais não refletem participações encerradas antes da abertura dos dados
- `ultima_validacao`: 2026-03-08
- `observacoes`: módulo opcional; estratégia two-pass para filtragem; dados em `data/raw/rfb/`

## Regras

1. Nenhuma fonte entra no projeto sem registro.
2. Toda atualização relevante exige ajuste neste arquivo.
3. Toda incerteza de cobertura deve ser marcada como `INCERTO`.
