# Objetivos e escopo

## Objetivo geral

Construir uma plataforma de análise empírica capaz de identificar padrões, desvios e outliers em decisões do STF, com base em subconjuntos de casos comparáveis e trilha de evidência auditável.

## Objetivos específicos

1. Consolidar o corpus inicial de transparência em um modelo de dados estável.
2. Normalizar processos, eventos decisórios, partes e advogados.
3. Produzir estatísticas descritivas por ministro, assunto, classe e órgão.
4. Definir critérios formais de comparabilidade entre casos.
5. Criar baselines para comportamento decisório esperado.
6. Detectar outliers e mudanças de padrão.
7. Priorizar casos para revisão jurídica posterior.
8. Registrar limites metodológicos e riscos de inferência.
9. Preparar o projeto para acoplamento futuro com fontes textuais oficiais.

## Escopo incluído no estado atual

- documentação da arquitetura conceitual;
- inventário das fontes iniciais;
- dicionário de dados;
- modelo conceitual das entidades;
- metodologia de comparabilidade;
- especificação de métricas;
- especificação de score de anomalia;
- bundles de evidência para alertas materializados;
- serving database para consumo de produto;
- API HTTP para consulta de recortes, entidades, origem, sanções, doações, vínculos corporativos, afinidade ministro-advogado, risco composto e análise temporal;
- dashboard web para navegação auditável dessas superfícies já materializadas (26 páginas);
- especificação e revisão contínua de painéis e relatórios;
- regras do agente;
- trilha de governança.

## Escopo excluído do estado atual

- conclusão jurídica automática;
- classificação de motivação;
- análise probatória de intenção;
- leitura completa e massiva do inteiro teor;
- automação de acusações;
- integração total com todas as superfícies do STF;
- modelagem final de jurisprudência aprofundada.

## Entregas mínimas do repositório atual

- `README.md`
- documentos da pasta `docs/`
- registro das fontes
- dicionário de dados
- registro de riscos
- artefatos em `data/analytics/`, `data/evidence/` e `data/serving/`
- serving database builder
- API FastAPI
- dashboard web
- roadmap por fases

## Critérios de aceitação

O estado atual será considerado consistente quando:

- o escopo estiver formalmente delimitado;
- as entidades estiverem definidas;
- a metodologia de comparação estiver escrita;
- os limites do projeto estiverem explícitos;
- houver documentação suficiente para evolução posterior sem ambiguidade estrutural;
- as superfícies públicas materializadas estiverem descritas sem contradição com o código.
