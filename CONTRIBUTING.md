# Contribuindo

## Objetivo

Este repositório aceita contribuições em documentação, governança, modelagem de dados, ingestão, normalização e análise, desde que respeitem os limites metodológicos já registrados no projeto.

## Princípios obrigatórios

Toda contribuição deve respeitar estas regras:

1. Não inventar fatos, cobertura, schema, endpoint, licença, significado de coluna ou disponibilidade de fonte.
2. Não concluir favorecimento, corrupção, parcialidade, conluio ou intenção.
3. Não misturar hipótese com fato.
4. Marcar como `INCERTO` tudo o que não puder ser comprovado pelas fontes do projeto.
5. Preservar trilha de evidência.
6. Nunca tratar correlação como causalidade.
7. Nenhum arquivo de código novo ou alterado deve ultrapassar 500 linhas; se isso ocorrer, a contribuição deve dividir o artefato ou justificar a exceção de forma explícita.

## Tipos de contribuição aceitos

- melhoria de documentação e clareza editorial;
- revisão de governança, risco e trilha de auditoria;
- melhoria de dicionário de dados e registro de fontes;
- implementação de ingestão, normalização e análise compatíveis com a metodologia do projeto;
- testes e validações reproduzíveis.

## Tipos de contribuição não aceitos

- textos acusatórios ou conclusivos sem base documental adequada;
- automações que ampliem o escopo do projeto para juízo jurídico final;
- alterações que removam rastreabilidade do dado bruto;
- mudanças que tratem hipótese como resultado final.

## Fluxo recomendado

1. Leia `README.md`, `docs/` e `governance/`.
2. Delimite claramente o problema, o artefato afetado e o critério de validação.
3. Faça mudanças pequenas, auditáveis e reversíveis.
4. Explique no PR o que mudou, por que mudou e quais riscos permanecem.

## Padrão para documentação

Ao criar ou revisar documentos operacionais, prefira esta estrutura quando aplicável:

1. Objetivo
2. Entradas
3. Saída esperada
4. Restrições
5. Critérios de validação
6. Riscos ou incertezas

## Checklist antes de abrir PR

- a mudança respeita o escopo do projeto;
- não há linguagem acusatória;
- fatos, hipóteses e inferências estão separados;
- `INCERTO` foi usado quando necessário;
- a trilha de evidência foi preservada;
- nenhum arquivo de código novo ou alterado ultrapassa 500 linhas, salvo exceção justificada;
- não foram introduzidos marcadores vazios, exemplos fictícios ou texto genérico;
- a documentação afetada continua coerente com `README.md` e `governance/`.
