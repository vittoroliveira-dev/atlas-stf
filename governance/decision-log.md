# Decision Log

## Objetivo

Este documento registra decisões relevantes de arquitetura, escopo, metodologia, governança e operação do projeto.

A função do log é permitir que qualquer pessoa entenda:
- o que foi decidido;
- por que foi decidido;
- quais alternativas existiam;
- qual impacto a decisão traz;
- se a decisão ainda está vigente.

## Regras de uso

1. Toda decisão estrutural relevante deve ser registrada aqui.
2. Toda entrada deve ter identificador único.
3. Toda decisão deve distinguir fato, hipótese e escolha.
4. Mudanças de direção devem gerar nova entrada, e não sobrescrever a antiga.
5. Quando uma decisão for substituída, isso deve ser explicitado.
6. Decisões provisórias devem ser marcadas como tal.
7. Nenhuma decisão deve depender apenas de contexto oral.

## Estrutura padrão de cada entrada

Cada decisão deve conter os campos abaixo:

- `decision_id`
- `data`
- `titulo`
- `status`
- `categoria`
- `contexto`
- `problema`
- `alternativas_consideradas`
- `decisao`
- `justificativa`
- `impactos`
- `riscos`
- `artefatos_afetados`
- `substitui`
- `substituida_por`
- `proximos_passos`

## Status permitidos

- `proposta`
- `aprovada`
- `provisoria`
- `substituida`
- `descartada`

## Categorias sugeridas

- `escopo`
- `dados`
- `metodologia`
- `modelagem`
- `governanca`
- `interface`
- `analitica`
- `operacao`
- `agente`

---

# Entradas

## DEC-001

- `decision_id`: DEC-001
- `data`: 2026-03-06
- `titulo`: O projeto será formulado como análise de tratamento decisório atípico, e não como detector de favorecimento
- `status`: aprovada
- `categoria`: escopo

### Contexto
A motivação inicial do projeto envolvia verificar possível favorecimento em decisões do STF. No entanto, os dados disponíveis na etapa inicial são predominantemente estruturados e não sustentam, sozinhos, conclusões fortes sobre intenção ou irregularidade.

### Problema
Uma formulação acusatória na raiz do projeto aumentaria risco metodológico, narrativo e interpretativo, além de induzir uso inadequado da ferramenta.

### Alternativas consideradas
1. Construir o projeto como detector de favorecimento.
2. Construir o projeto como observatório de coerência jurisprudencial.
3. Construir o projeto como sistema de análise de tratamento atípico e desvio de padrão decisório.

### Decisão
O projeto será definido como sistema de análise de tratamento decisório atípico e desvio de padrão decisório, sem workflow interno obrigatório de revisão humana.

### Justificativa
Essa formulação:
- é tecnicamente mais defensável;
- é compatível com os dados disponíveis no corpus atual;
- reduz risco de sobreleitura;
- preserva utilidade investigativa sem converter hipótese em acusação.

### Impactos
- A linguagem de produto deve ser neutra.
- Alertas devem ser classificados como atipicidade, divergência aparente ou inconclusivo.
- O sistema não pode emitir conclusões acusatórias automáticas.

### Riscos
- Usuários externos ainda podem interpretar alertas como acusações.
- Pode haver pressão para expandir escopo narrativo sem base suficiente.

### Artefatos afetados
- `README.md`
- `docs/00-visao-geral.md`
- `docs/01-objetivos-e-escopo.md`
- `docs/04-metodologia-analitica.md`
- `docs/07-riscos-e-limitacoes.md`

### Substitui
- nenhum

### Substituida por
- nenhum

### Próximos passos
- manter coerência terminológica em toda a documentação;
- impedir rótulos proibidos no agente.

---

## DEC-002

- `decision_id`: DEC-002
- `data`: 2026-03-06
- `titulo`: A base de transparência será o corpus estruturado inicial do projeto
- `status`: aprovada
- `categoria`: dados

### Contexto
O projeto já possui um conjunto inicial de planilhas extraídas do portal de transparência do STF, com informações processuais e decisórias estruturadas.

### Problema
Era necessário definir se o trabalho começaria por dados estruturados ou por coleta massiva de textos jurídicos.

### Alternativas consideradas
1. Começar pela coleta textual completa.
2. Começar exclusivamente pela jurisprudência publicada.
3. Começar pela base estruturada de transparência como índice mestre e camada estatística.

### Decisão
A base de transparência será usada como corpus inicial, índice mestre e camada processual-estatística do projeto.

### Justificativa
Essa escolha:
- acelera a estruturação do projeto;
- permite análise descritiva e comparativa imediata;
- reduz custo inicial;
- melhora a priorização de casos para revisão textual posterior.

### Impactos
- O estado atual do projeto será focado em triagem, não em análise argumentativa profunda.
- O modelo de dados deve separar claramente processo e evento decisório.
- O roadmap deve prever aprofundamento documental posterior.

### Riscos
- O recorte exportado pode ser incompleto ou filtrado.
- Os dados estruturados não capturam a fundamentação jurídica.

### Artefatos afetados
- `docs/02-fontes-de-dados.md`
- `docs/03-modelo-de-dados.md`
- `docs/09-roadmap.md`
- `governance/source-registry.md`
- `governance/data-dictionary.md`

### Substitui
- nenhum

### Substituida por
- nenhum

### Próximos passos
- registrar formalmente origem e cobertura de cada exportação;
- separar camada raw, staging e curated.

---

## DEC-003

- `decision_id`: DEC-003
- `data`: 2026-03-06
- `titulo`: O projeto será orientado à triagem e priorização de casos
- `status`: aprovada
- `categoria`: metodologia

### Contexto
Os dados atuais permitem identificar padrões e outliers, mas não sustentam revisão jurídica profunda de todo o universo de decisões.

### Problema
Era necessário definir o propósito operacional do corpus e da documentação atuais.

### Alternativas consideradas
1. Tentar fazer leitura jurídica profunda desde o início.
2. Construir apenas uma camada visual, sem lógica analítica.
3. Construir uma camada de triagem, priorização e explicação auditável.

### Decisão
O projeto será orientado à triagem e priorização de casos para verificação externa posterior, quando desejada.

### Justificativa
Essa escolha preserva foco, reduz complexidade e respeita a limitação do corpus inicial.

### Impactos
- As métricas devem privilegiar explicabilidade e utilidade de alerta.
- A camada de evidência é obrigatória antes de exposição em interface operacional.
- Qualquer verificação humana fica fora do fluxo operacional do sistema.

### Riscos
- Usuários podem esperar respostas mais fortes do que o projeto suporta.
- A utilidade percebida dependerá da qualidade dos grupos comparáveis.

### Artefatos afetados
- `docs/04-metodologia-analitica.md`
- `docs/05-definicao-de-casos-comparaveis.md`
- `docs/06-metricas-e-avaliacao.md`
- `ui-spec/evidence-panel-spec.md`

### Substitui
- nenhum

### Substituida por
- nenhum

### Próximos passos
- definir score de atipicidade;
- definir contrato de análise derivada opcional por IA.

---

## DEC-004

- `decision_id`: DEC-004
- `data`: 2026-03-06
- `titulo`: Casos comparáveis são requisito obrigatório antes de qualquer inferência
- `status`: aprovada
- `categoria`: metodologia

### Contexto
Comparações amplas ou superficiais geram alto risco de falso positivo.

### Problema
Sem critérios formais de comparabilidade, o sistema poderia produzir alertas pouco confiáveis.

### Alternativas consideradas
1. Comparar todo o histórico de cada ministro sem segmentação.
2. Comparar apenas por palavras-chave.
3. Exigir grupos comparáveis com critérios explícitos.

### Decisão
Nenhum alerta poderá ser produzido sem associação a um grupo comparável formalmente definido.

### Justificativa
Isso aumenta confiabilidade analítica e reduz inferências indevidas.

### Impactos
- O projeto precisa versionar regras de comparabilidade.
- Todo alerta deve apontar o grupo comparável usado.
- Quando a interface operacional existir, ela deve mostrar baseline e critérios do grupo.

### Riscos
- Grupos estreitos demais reduzem cobertura.
- Grupos amplos demais reduzem precisão.

### Artefatos afetados
- `docs/05-definicao-de-casos-comparaveis.md`
- `docs/06-metricas-e-avaliacao.md`
- `ui-spec/evidence-panel-spec.md`

### Substitui
- nenhum

### Substituida por
- nenhum

### Próximos passos
- definir critérios mínimos do estágio atual;
- criar convenção de versionamento para grupos.

---

## DEC-005

- `decision_id`: DEC-005
- `data`: 2026-03-06
- `titulo`: O backend analítico terá Python como linguagem principal
- `status`: aprovada
- `categoria`: operacao

### Contexto
Era necessário definir a stack principal do projeto antes de avançar para especificações mais detalhadas.

### Problema
Sem definição de stack, a documentação poderia ficar abstrata demais ou contraditória.

### Alternativas consideradas
1. Python como linguagem central.
2. TypeScript full-stack.
3. Stack poliglota mais complexa desde o início.

### Decisão
Python será a linguagem principal do backend analítico, com PostgreSQL como base principal e TypeScript restrito à interface web, se necessária.

### Justificativa
Python é mais adequado para ingestão, limpeza, análise, ETL, jobs assíncronos e futura camada textual.

### Impactos
- Documentos do projeto devem assumir Python como stack central.
- SQL/PostgreSQL entram como linguagem e base de persistência.
- TypeScript fica restrito ao frontend e ao dashboard.

### Riscos
- A equipe pode querer antecipar complexidade desnecessária de stack.
- Pode haver mistura indevida entre documentação metodológica e decisão de framework.

### Artefatos afetados
- `README.md`
- `docs/09-roadmap.md`

### Substitui
- nenhum

### Substituida por
- nenhum

### Próximos passos
- manter a arquitetura de linguagem simples no estágio atual;
- evitar decisões prematuras sobre componentes não essenciais.

---

## DEC-006

- `decision_id`: DEC-006
- `data`: 2026-03-06
- `titulo`: O projeto adotará governança explícita com trilha de auditoria
- `status`: aprovada
- `categoria`: governanca

### Contexto
O tema do projeto exige rigor metodológico e transparência interna sobre escolhas, fontes e limitações.

### Problema
Sem governança formal, decisões metodológicas poderiam se perder ou ser reinterpretadas sem rastro.

### Alternativas consideradas
1. Governança informal.
2. Documentação mínima sem log estruturado.
3. Estrutura formal com source registry, data dictionary, risk register e decision log.

### Decisão
O projeto adotará governança explícita e versionada, com trilha de auditoria mínima obrigatória.

### Justificativa
Isso melhora reprodutibilidade, revisão externa e disciplina metodológica.

### Impactos
- Toda fonte deve ser registrada.
- Toda decisão estrutural deve entrar neste log.
- Toda mudança relevante deve apontar artefatos afetados.

### Riscos
- Maior custo documental inicial.
- Resistência a registrar decisões aparentemente pequenas.

### Artefatos afetados
- `governance/source-registry.md`
- `governance/data-dictionary.md`
- `governance/risk-register.md`
- `governance/decision-log.md`
- `docs/08-governanca-e-auditoria.md`

### Substitui
- nenhum

### Substituida por
- nenhum

### Próximos passos
- manter este log atualizado;
- criar hábito de registrar mudanças metodológicas antes de expandir escopo.

---

## DEC-007

- `decision_id`: DEC-007
- `data`: 2026-03-07
- `titulo`: API HTTP, serving database e dashboard web passam a integrar o estado atual documentado do projeto
- `status`: aprovada
- `categoria`: interface

### Contexto
O repositório já contém builder de serving database, API FastAPI e dashboard Next.js consumindo endpoints reais do backend analítico.

### Problema
Parte da documentação institucional ainda tratava a camada de interface como etapa futura ou apenas especificada, o que reduzia precisão pública e enfraquecia o posicionamento open source do projeto.

### Alternativas consideradas
1. Manter a documentação tratando interface e API como futuras.
2. Expor a camada web apenas como experimento local sem status institucional.
3. Atualizar a documentação para reconhecer serving, API e dashboard como capacidades já materializadas, preservando o registro das lacunas ainda abertas.

### Decisão
Serving database, API HTTP e dashboard web passam a integrar formalmente o estado atual documentado do projeto.

### Justificativa
Essa atualização:
- reduz contradição entre código e documentação;
- melhora clareza para usuários e contribuidores;
- posiciona o projeto de forma mais precisa como sistema analítico já navegável;
- preserva a distinção entre superfície implementada e aprofundamentos ainda pendentes.

### Impactos
- README deve refletir o projeto como pipeline + API + dashboard.
- Roadmap deixa de tratar a camada de produto analítico como totalmente futura.
- UI specs passam a funcionar como documento normativo de refinamento, não apenas como hipótese.
- Checklists de interface passam a valer para uma superfície pública existente.

### Riscos
- Visitantes podem inferir maturidade superior à efetivamente disponível em áreas ainda em consolidação.
- A documentação pública precisará ser mantida com mais disciplina para não desalinhar do código novamente.

### Artefatos afetados
- `README.md`
- `docs/00-visao-geral.md`
- `docs/01-objetivos-e-escopo.md`
- `docs/08-governanca-e-auditoria.md`
- `docs/09-roadmap.md`
- `ui-spec/dashboard-spec.md`
- `ui-spec/evidence-panel-spec.md`
- `governance/audit-checklists/interface.md`

### Substitui
- nenhum

### Substituida por
- nenhum

### Próximos passos
- manter o status de cada superfície pública explicitado no README;
- documentar lacunas ainda abertas da camada visual e textual;
- evitar que novos artefatos públicos surjam sem atualização documental correspondente.
