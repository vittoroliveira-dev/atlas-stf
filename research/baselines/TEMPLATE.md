# Baseline — TEMPLATE

## Como usar este modelo

Copie este arquivo para um baseline específico e substitua as orientações por conteúdo factual. Mantenha o versionamento do baseline porque ele é parte da trilha de auditoria.

## 1. Identificação

- `baseline_id`: usar identificador único
- `versao`: registrar a versão do baseline, por exemplo `1.0.0`
- `data`: registrar a data da versão
- `responsavel`: registrar a autoria
- `status`: escolher entre `rascunho`, `ativo`, `substituido`, `arquivado`

## 2. Objetivo

Descreva o padrão esperado usado como referência analítica para comparação de casos ou subconjuntos de casos.

## 3. Grupo comparável associado

Registre `comparison_group_id`, `versao_regra_grupo`, `descricao_resumida`, `janela_temporal` e `tamanho_do_grupo`.

## 4. Variáveis utilizadas

Liste as variáveis efetivamente usadas, como classe processual, assunto, ramo do direito, tipo de decisão, colegialidade, órgão julgador, período e outras variáveis relevantes.

## 5. Definição do baseline

Descreva, em linguagem simples, qual comportamento é considerado esperado dentro deste grupo comparável.

## 6. Medidas utilizadas

Registre proporção esperada, distribuição esperada, frequência histórica, comparação intra-ministro, comparação contra agregado do tribunal e observações pertinentes.

## 7. Justificativa metodológica

Explique por que este baseline é adequado para o grupo comparável definido.

## 8. Limitações

Liste limitações reais do baseline, como baixa amostra, recorte temporal restrito ou cobertura incompleta.

## 9. Riscos de falso positivo

Explique em quais cenários o baseline pode sinalizar desvio aparente sem relevância analítica suficiente.

## 10. Regras de uso

Registre quando este baseline pode ser usado, quando não deve ser usado e quais restrições precisam ser respeitadas.

## 11. Artefatos relacionados

Liste documentos de grupo comparável, relatórios, alertas e notas metodológicas vinculadas.

## 12. Histórico de alterações

Mantenha uma tabela com `data`, `versao`, `alteracao` e `responsavel`, registrando apenas mudanças reais já realizadas.
