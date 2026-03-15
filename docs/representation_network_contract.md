# Representation Network -- Contrato Semantico

## Glossario

| Termo | Definicao |
|-------|-----------|
| **lawyer_entity** | Entidade canonica que representa um advogado individual. Substitui a entidade `counsel` legada, adicionando campos OAB, vinculo a escritorio e rastreabilidade de fonte. |
| **law_firm_entity** | Entidade canonica que representa um escritorio de advocacia ou sociedade de advogados (CNPJ/CNSA). Nao existia no modelo legado. |
| **representation_edge** | Aresta direcionada que conecta um advogado ou escritorio a um processo, com papel (role_type), periodo e nivel de confianca agregado. |
| **representation_event** | Evento processual atomico que evidencia a atuacao de um representante (peticao, sustentacao oral, memorial, procuracao, substabelecimento, etc.). |
| **source_evidence** | Registro de proveniencia que vincula cada extracao ao documento, campo e parser de origem, permitindo auditoria completa. |
| **counsel (legado)** | Entidade do modelo v1.x que sera gradualmente substituida por lawyer_entity. Permanece no serving para compatibilidade retroativa durante a transicao. |

## Matriz de Confianca

A confianca de cada extracao depende do sistema de origem e do metodo de extracao:

| source_system | extraction_method | confidence | Justificativa |
|---------------|-------------------|------------|---------------|
| jurisprudencia | campo_partes_regex | 0.70 | Parsing de texto semi-estruturado; ambiguidade de papel |
| jurisprudencia | campo_advogado_estruturado | 0.85 | Campo dedicado, mas sem validacao OAB |
| portal_stf | aba_partes_html | 0.80 | HTML estruturado, porem sujeito a mudancas de layout |
| portal_stf | aba_andamentos_html | 0.75 | Andamentos mencionam advogados em texto livre |
| portal_stf | firm_name_from_portal | 0.50 | Nome de escritorio inferido do portal; baixa confianca |
| cnj_cnsa | cnsa_lookup | 0.95 | Cadastro oficial do CNJ para sociedades de advogados |
| oab_api | oab_validation | 0.95 | Validacao direta na base OAB seccional |
| rfb_cnpj | cnpj_lookup | 0.90 | Quadro societario oficial da Receita Federal |
| manual | curadoria_humana | 1.00 | Verificacao manual por pesquisador |

## Regras de Identity Key

A identity key e deterministica e segue prioridades estritas:

### Advogado (lawyer_entity)

1. **OAB** (prioridade maxima): `oab:NNNNNN/UF` -- numero OAB normalizado com UF valida
2. **CPF**: `tax:NNNNNNNNNNN` -- CPF normalizado (somente digitos)
3. **Nome canonico** (fallback): `name:NOME CANONICO` -- via `canonicalize_entity_name()`

### Escritorio (law_firm_entity)

1. **CNPJ** (prioridade maxima): `tax:NNNNNNNNNNNNNN` -- CNPJ normalizado
2. **CNSA**: `cnsa:NNNN` -- numero CNSA normalizado (somente digitos)
3. **Nome canonico** (fallback): `name:NOME CANONICO` -- via `canonicalize_entity_name()`

### Regras de normalizacao

- OAB: remover pontos/espacos antes da barra, uppercase na UF, validar formato 1-6 digitos + "/" + UF valida
- CPF/CNPJ: remover caracteres nao numericos via `normalize_tax_id()`
- CNSA: remover caracteres nao numericos, manter somente digitos
- Nome canonico: `canonicalize_entity_name()` (uppercase, remove sufixos corporativos, colapsa whitespace)

## Regras Semanticas

### Advogado != Escritorio

Um advogado (pessoa fisica) NUNCA pode ser confundido com um escritorio (pessoa juridica).
Quando um nome aparece com OAB, e advogado. Quando aparece com CNPJ/CNSA, e escritorio.
Em caso de ambiguidade, criar ambas as entidades e vincular via `firm_id` no lawyer_entity.

### Amicus != Representante do Amicus

O amicus curiae e uma parte processual (party_entity), nao um representante.
O advogado que assina o memorial do amicus e um representante com `role_type = amicus_representative`.
Nao confundir a entidade amicus com o advogado que a representa.

### Substituicao de Advogado

Quando um substabelecimento e detectado, o advogado substituido recebe `end_date` na aresta,
e o novo advogado recebe uma nova aresta com `start_date`. O evento de substabelecimento
e registrado em representation_event para ambos.

### Advogado Publico

Procuradores (AGU, PGR, DPU) sao representantes com `role_type = public_attorney`.
Nao possuem OAB necessariamente, mas podem ter. A identity key segue a mesma prioridade.

## ConfirmADV -- Exclusao de Batch

O sistema **ConfirmADV** (verificacao interativa de OAB junto as seccionais) e **excluido**
do processamento em batch. Motivos:

1. APIs das seccionais OAB exigem interacao (CAPTCHA, rate limiting severo)
2. Verificacao em massa viola termos de uso das seccionais
3. Resultados sao efemeros (status OAB muda frequentemente)

O campo `oab_status` so deve ser preenchido quando:
- Houver integracao pontual com API OAB (verificacao individual sob demanda)
- Dados forem obtidos de fonte oficial em batch (ex: CNSA do CNJ)
- Curadoria manual por pesquisador

## firm_name do Portal -- Baixa Confianca

Nomes de escritorio extraidos do portal STF tem confianca **0.50** porque:

1. O portal nao distingue consistentemente advogado individual de escritorio
2. Nomes podem estar truncados ou abreviados
3. Nao ha CNPJ/CNSA associado para validacao cruzada

Sempre que possivel, cruzar com dados RFB (CNPJ) ou CNJ (CNSA) para elevar a confianca.
