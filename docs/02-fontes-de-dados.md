# Fontes de dados

## Objetivo deste documento

Registrar apenas as fontes que já possuem função operacional comprovável no repositório atual ou que já têm diretório dedicado na camada `raw/`.

## Leitura deste documento

- `primária`: fonte base do corpus estruturado.
- `complementar`: fonte opcional já integrada a algum módulo do pipeline.
- `documental`: fonte prevista para aprofundamento textual, sem papel obrigatório no pipeline-base.
- `INCERTO`: ponto não demonstrado pela documentação ou pelo código atual.

## 1. Fonte primária estruturada

### STF-TRANSP-REGDIST

- categoria: `primária`
- origem: Portal de Transparência do STF, exportações de registro e distribuição
- formato: exportação tabular preservada em `data/raw/transparencia/`
- uso atual:
  - `stage`
  - `curate process|decision-event|subject|party|counsel|links|all`
  - grupos comparáveis, baselines e alertas
- camada consumidora principal: `staging`, `curated`, `analytics`, `serving`, `api`, `web`
- limitações:
  - pode refletir recortes filtrados;
  - não substitui inteiro teor;
  - cobertura histórica exata permanece `INCERTO`
- validação operacional:
  - preservar hash, data de coleta e descrição do filtro;
  - não sobrescrever bruto;
  - manter o registro oficial em `governance/source-registry.md`

## 2. Fontes textuais oficiais complementares

### STF-JURIS

- categoria: `documental`
- origem: pesquisa pública de jurisprudência do STF
- formato: JSONL bruto em `data/raw/jurisprudencia/acordaos/` e `data/raw/jurisprudencia/decisoes/`
- uso atual:
  - enriquecimento opcional de `process` e `decision_event`
  - recuperação de campos textuais e URLs de inteiro teor
  - base de apoio para bundles de evidência e aprofundamento posterior
- comando relacionado: `uv run atlas-stf scrape decisoes|acordaos`
- limitações:
  - cobertura por tipo de peça permanece `INCERTO`;
  - ausência de match não prova ausência de documento oficial

### STF-DJE

- categoria: `documental`
- origem: Diário de Justiça Eletrônico do STF
- formato: diretório reservado em `data/raw/dje/`
- uso atual:
  - trilha documental e confirmação de publicação em ciclos futuros
- estado operacional:
  - pasta e política documental existem;
  - não há builder obrigatório acoplado ao pipeline-base

## 3. Fontes estruturadas complementares já integradas

### CNJ-DATAJUD

- categoria: `complementar`
- origem: API pública do CNJ DataJud
- formato: JSON bruto em `data/raw/datajud/`
- uso atual:
  - `uv run atlas-stf datajud fetch`
  - `uv run atlas-stf datajud build-context`
  - artefatos opcionais de `origin_context`
  - endpoints `GET /origin-context` e `GET /origin-context/{state}`
- limitações:
  - não cobre o STF;
  - matching é agregado por tribunal/UF, não por processo individual

### CGU-CEIS-CNEP-LENIENCIA

- categoria: `complementar`
- origem: Portal da Transparência da CGU
- formato: ZIP/CSV bruto em `data/raw/cgu/`
- uso atual:
  - `uv run atlas-stf cgu fetch`
  - `uv run atlas-stf cgu build-matches`
  - artefatos `sanction_match.jsonl`, `sanction_match_summary.json`, `counsel_sanction_profile.jsonl`
  - endpoints e páginas de sanções
- limitações:
  - matching principalmente por nome normalizado;
  - cobertura limitada a sanções administrativas e acordos publicados pela CGU

### TSE-DOACOES

- categoria: `complementar`
- origem: dados abertos do TSE sobre receitas de campanha
- formato: bruto em `data/raw/tse/`
- uso atual:
  - `uv run atlas-stf tse fetch`
  - `uv run atlas-stf tse build-matches`
  - artefatos `donation_match.jsonl`, `donation_match_summary.json`, `counsel_donation_profile.jsonl`
  - endpoints e páginas de doações
- limitações:
  - matching por nome normalizado;
  - anos cobertos dependem do material efetivamente baixado

### CVM-SANCIONADORES

- categoria: `complementar`
- origem: processos administrativos sancionadores da CVM
- formato: bruto em `data/raw/cvm/`
- uso atual:
  - `uv run atlas-stf cvm fetch`
  - `uv run atlas-stf cvm build-matches`
  - integrado ao fluxo de `sanction_match`
- limitações:
  - matching por nome normalizado;
  - escopo restrito ao universo sancionador publicado pela CVM

### RFB-CNPJ

- categoria: `complementar`
- origem: dados abertos CNPJ da Receita Federal
- formato: bruto em `data/raw/rfb/`
- uso atual:
  - `uv run atlas-stf rfb fetch`
  - `uv run atlas-stf rfb build-network`
  - artefatos `corporate_network.jsonl` e `corporate_network_summary.json`
  - endpoints e páginas de vínculos corporativos
- limitações:
  - alto volume de dados;
  - matching por nome normalizado;
  - vínculos históricos encerrados antes da fotografia aberta podem não aparecer

### STF-AGENDA

- categoria: `complementar`
- origem: API GraphQL do STF (agenda ministerial)
- formato: bruto em `data/raw/agenda/`
- uso atual:
  - `uv run atlas-stf agenda fetch`
  - `uv run atlas-stf agenda build-events`
  - artefatos `agenda_event.jsonl`, `agenda_coverage.jsonl`, `agenda_exposure.jsonl`
  - endpoints e páginas de agenda ministerial e exposição temporal
- limitações:
  - cobertura depende da publicação oficial de agenda pelo STF

### STF-PORTAL

- categoria: `complementar`
- origem: portal público do STF (httpx scraping)
- formato: bruto em `data/raw/stf_portal/`
- uso atual:
  - `uv run atlas-stf stf-portal fetch`
  - extração de linha do tempo processual (andamentos, sessões, vistas, sustentação oral)
  - alimenta representação processual e timeline no serving
- limitações:
  - sujeito a mudanças no HTML do portal

### DEOAB

- categoria: `complementar`
- origem: Diário Eletrônico da OAB (PDF público)
- formato: bruto em `data/raw/deoab/`
- uso atual:
  - `uv run atlas-stf deoab fetch`
  - registros de sociedades de advocacia, vínculos OAB→escritório (2019–presente)
- limitações:
  - dependente de extração de PDF (pdftotext)

### OAB-SP

- categoria: `complementar`
- origem: consulta à seccional OAB/SP (httpx + checkpoint)
- uso atual:
  - detalhes cadastrais de sociedades e advogados inscritos na OAB de São Paulo
- limitações:
  - cobertura restrita à seccional de São Paulo

## 4. Fontes contextuais externas

### EXTERNAL-EVENTS

- categoria: `documental`
- origem: fatos públicos documentados em `data/raw/external_events/`
- uso atual:
  - contextualização temporal complementar
- restrições:
  - não entram como causa automática;
  - devem permanecer separados da camada decisória principal

## Critérios de validação

- cada fonte usada deve ter diretório ou artefato correspondente no repositório;
- toda fonte com uso ativo deve aparecer em `governance/source-registry.md`;
- toda limitação de cobertura não demonstrada deve permanecer como `INCERTO`;
- nenhuma fonte complementar pode ser descrita como obrigatória se o pipeline-base continua funcionando sem ela.

## Riscos ou incertezas

- cobertura exata das exportações do STF continua `INCERTO`;
- cobertura textual da jurisprudência por tipo de peça continua `INCERTO`;
- módulos complementares dependem da materialização local dos seus dados brutos e derivados.
