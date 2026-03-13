# Visão geral do projeto

## Definição

O Atlas STF é um observatório empírico e auditável de padrões decisórios do Supremo Tribunal Federal. O estado atual do repositório combina pipeline determinístico em Python, banco de serving, API HTTP em FastAPI e dashboard web em Next.js sobre artefatos já materializados.

## Premissa central

Há valor analítico relevante no uso de dados estruturados de transparência para triagem, priorização e modelagem de padrões. No entanto, tais dados não são suficientes, isoladamente, para conclusões fortes sobre motivação, intenção ou irregularidade.

A solução correta, portanto, é separar o projeto em duas camadas:

1. camada processual-estatística;
2. camada jurídico-textual.

## Questão central

A pergunta que o sistema busca responder é:

"Houve tratamento decisório atípico, estatisticamente ou comparativamente relevante, em subconjuntos juridicamente semelhantes?"

Essa formulação substitui perguntas excessivamente fortes e pouco demonstráveis, como "houve favorecimento?", por uma abordagem mais útil e verificável.

## Tese metodológica

O sistema deve primeiro localizar padrões, outliers e mudanças de comportamento em dados estruturados. Depois, deve selecionar os casos mais relevantes para revisão jurídica com base em documentos oficiais publicados.

Isso reduz custo, melhora foco e aumenta robustez metodológica.

## Escopo atual documentado

O repositório cobre:

- ingestão do dataset inicial de transparência;
- normalização de processos, eventos decisórios, partes e advogados;
- análise descritiva por ministro, assunto, classe e período;
- definição de grupos comparáveis;
- detecção de outliers;
- materialização de banco de serving para consumo de produto;
- API HTTP para filtros, dashboard, alertas, casos, partes, advogados, ministros e auditoria;
- dashboard web auditável sobre artefatos já produzidos, com páginas de visão geral, alertas, caso, partes, advogados, ministros, auditoria, sanções, doações, vínculos, afinidade e origem;
- documentação formal de metodologia, riscos e governança;
- cruzamento com CEIS/CNEP da CGU para detecção de partes sancionadas (módulo opcional);
- cruzamento com acordos de leniência da CGU para detecção de empresas com delação premiada corporativa (módulo opcional);
- cruzamento com doações eleitorais do TSE para detecção de partes doadoras com taxa de êxito atípica (módulo opcional);
- cruzamento com processos sancionadores da CVM para detecção de partes punidas no mercado de capitais (módulo opcional);
- cruzamento com quadro societário da RFB para detecção de vínculos corporativos entre ministros e partes/advogados (módulo opcional);
- análise de afinidade ministro-advogado para detecção de pares com taxa de êxito anômala (módulo opcional);
- índice de risco composto que consolida sanções, doações, vínculos e afinidade num ranking unificado (módulo opcional);
- análise temporal de padrões decisórios ministeriais, incluindo eventos marcantes, tendências mensais e cruzamento com rede corporativa (módulo opcional).

## Superfície pública real no estado atual

### Páginas web implementadas

- `/`
- `/alertas`
- `/caso`
- `/caso/[decisionEventId]`
- `/advogados`
- `/advogados/[counselId]`
- `/partes`
- `/partes/[partyId]`
- `/ministros`
- `/ministros/[minister]`
- `/auditoria`
- `/sancoes`
- `/doacoes`
- `/vinculos`
- `/afinidade`
- `/origem`
- `/temporal`
- `/convergencia`

### Endpoints HTTP implementados

- `GET /health`
- `GET /filters/options`
- `GET /dashboard`
- `GET /alerts`
- `GET /alerts/{alert_id}`
- `GET /cases`
- `GET /cases/{decision_event_id}`
- `GET /cases/{decision_event_id}/related-alerts`
- `GET /counsels`
- `GET /counsels/{counsel_id}`
- `GET /counsels/{counsel_id}/ministers`
- `GET /parties`
- `GET /parties/{party_id}`
- `GET /parties/{party_id}/ministers`
- `GET /ministers/{minister}/flow`
- `GET /ministers/{minister}/counsels`
- `GET /ministers/{minister}/parties`
- `GET /ministers/{minister}/profile`
- `GET /ministers/{minister}/sequential`
- `GET /ministers/{minister}/bio`
- `GET /audit/assignment`
- `GET /origin-context`
- `GET /origin-context/{state}`
- `GET /sources/audit`
- `GET /sanctions`
- `GET /sanctions/red-flags`
- `GET /parties/{party_id}/sanctions`
- `GET /counsels/{counsel_id}/sanction-profile`
- `GET /donations`
- `GET /donations/red-flags`
- `GET /parties/{party_id}/donations`
- `GET /counsels/{counsel_id}/donation-profile`
- `GET /corporate-network`
- `GET /corporate-network/red-flags`
- `GET /ministers/{minister}/corporate-conflicts`
- `GET /counsel-affinity`
- `GET /counsel-affinity/red-flags`
- `GET /ministers/{minister}/counsel-affinity`
- `GET /counsels/{counsel_id}/minister-affinity`
- `GET /compound-risk`
- `GET /compound-risk/red-flags`
- `GET /compound-risk/heatmap`
- `GET /temporal-analysis`
- `GET /temporal-analysis/{minister}`

Observações contratuais já comprovadas no código:

- o detalhe de caso em `GET /cases/{decision_event_id}` é subordinado ao recorte filtrado atual;
- endpoints de biografia ministerial podem retornar ausência quando a camada correspondente não estiver materializada;
- `origin-context` depende da materialização opcional do módulo DataJud;
- a documentação acima descreve apenas superfícies implementadas no repositório atual.

## Snapshot materializado no repositório

Os arquivos de resumo atualmente presentes em `data/analytics/` mostram o seguinte estado materializado:

- grupos comparáveis: `5.153` grupos totais e `1.463` grupos válidos em `2026-03-07T02:10:47+00:00`;
- baselines: `1.463` em `2026-03-07T02:17:16+00:00`;
- alertas: `487` em `2026-03-07T02:39:36+00:00`;
- sanções: `1.854` matches em `2026-03-08T16:52:36+00:00`;
- doações: `11.972` matches em `2026-03-08T17:01:09+00:00`;
- afinidade ministro-advogado: `20.644` pares analisados em `2026-03-08T22:16:50+00:00`;
- rede corporativa: `12` vínculos materializados em `2026-03-09T00:37:26+00:00`.

Esses números representam o snapshot derivado atualmente versionado no workspace. Eles não demonstram completude do universo do STF.

## Expansões previstas

Ciclos posteriores podem incluir:

- integração com DJe;
- extração de fundamentos;
- comparação argumentativa;
- visualização mais rica sobre a interface já existente;
- análise derivada opcional por IA sobre bundles materializados.

## Entrega esperada do repositório atual

O projeto deve ser capaz de:

- estruturar o corpus inicial;
- documentar claramente suas limitações;
- produzir listas priorizadas de casos atípicos;
- explicar por que um caso foi sinalizado;
- expor recortes, alertas e detalhes de caso em API e interface auditáveis;
- permitir verificação externa eficiente, quando desejada.

## Limites assumidos

- O corpus inicial pode ser incompleto.
- O recorte exportado pode não refletir toda a série histórica disponível.
- A base estruturada não substitui a leitura do inteiro teor.
- Um desvio estatístico não equivale a uma irregularidade.

## Resultado institucional esperado

O produto final será um observatório empírico de padrões decisórios, útil para pesquisa, auditoria, jornalismo investigativo e priorização de revisão documental.
