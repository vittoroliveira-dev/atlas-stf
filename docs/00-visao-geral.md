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
- análise temporal de padrões decisórios ministeriais, incluindo eventos marcantes, tendências mensais e cruzamento com rede corporativa (módulo opcional);
- análise de velocidade decisória para detecção de anomalias de tempo de tramitação (fura-fila/parado) (módulo opcional);
- detecção de redistribuição de relatoria e resultado pós-mudança vs baseline (módulo opcional);
- rede de advogados com detecção de clusters de co-clientela (módulo opcional);
- linha do tempo processual com andamentos extraídos do portal STF e categorização TPU (módulo opcional);
- análise de anomalia de sessão por ministro — frequência de vista, duração e retirada de pauta (módulo opcional).

## Superfície pública real no estado atual

A superfície pública do sistema compreende um dashboard web com páginas por área analítica (alertas, caso, partes, advogados, ministros, sanções, doações, vínculos, afinidade, origem, temporal, velocidade, redistribuição, rede de advogados, convergência, representação, agenda e grafo de investigação) e uma API HTTP read-only em FastAPI.

O inventário atualizado de páginas e endpoints é mantido automaticamente em `README.md` e `docs/ARCHITECTURE.md`. Para a lista completa, consultar esses documentos.

Observações contratuais:

- o detalhe de caso é subordinado ao recorte filtrado atual;
- endpoints de biografia ministerial podem retornar ausência quando a camada correspondente não estiver materializada;
- `origin-context` depende da materialização opcional do módulo DataJud.

## Snapshot materializado

Os volumes operacionais atuais -- incluindo contagens de grupos, alertas, matches e vínculos -- são mantidos nos artefatos canônicos de inventário do pipeline (`data/analytics/*_summary.json`) e consolidados no snapshot de release. Para contagens atualizadas, consultar `docs/ARCHITECTURE.md` e `README.md`. Os números representam o snapshot derivado atualmente versionado no workspace e não demonstram completude do universo do STF.

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
- materializar um grafo de investigação com scoring decomposto para priorização de revisão;
- permitir revisão humana estruturada via fila de revisão com status rastreáveis;
- permitir verificação externa eficiente, quando desejada.

## Limites assumidos

- O corpus inicial pode ser incompleto.
- O recorte exportado pode não refletir toda a série histórica disponível.
- A base estruturada não substitui a leitura do inteiro teor.
- Um desvio estatístico não equivale a uma irregularidade.

## Resultado institucional esperado

O produto final será um observatório empírico de padrões decisórios, útil para pesquisa, auditoria, jornalismo investigativo e priorização de revisão documental.
