# Checklist de auditoria — código por grupos auditáveis

## Objetivo

Definir um procedimento rígido para revisar o código em grupos auditáveis, reduzindo falso positivo, achismo e perda de contexto.

## Aplicação

Usar este checklist quando a revisão envolver mais de um subsistema, superfície pública ou cadeia de dados.

## Itens obrigatórios

- [ ] O grupo tem fronteira explícita por responsabilidade e risco.
- [ ] Todos os diretórios e arquivos do grupo foram listados antes da revisão.
- [ ] O revisor leu integralmente os arquivos usados como evidência de achado.
- [ ] O revisor verificou chamadores e dependências imediatas do trecho apontado.
- [ ] O revisor aplicou leitura arquitetural do grupo antes de procurar bugs locais.
- [ ] O revisor mapeou inputs, outputs, armazenamento, chamadas externas e limites operacionais.
- [ ] O revisor aplicou o checklist de `find-bugs` ao grupo: injeção, autenticação, autorização, divulgação indevida, DoS, lógica e estados.
- [ ] O revisor aplicou o checklist de `security-review` ao grupo: segredos, validação, transporte, persistência, exposição de erro e links externos.
- [ ] Cada achado contém arquivo, linha, evidência positiva, contraprova revisada e teste adjacente.
- [ ] Cada achado informa se já existe teste cobrindo ou se a lacuna permanece.
- [ ] Tudo que não pôde ser comprovado foi marcado como `INCERTO`.
- [ ] Nenhum julgamento de intenção, favorecimento ou causalidade foi registrado.

## Critérios de bloqueio

- Interromper a revisão se o grupo misturar responsabilidades incompatíveis sem registrar a fronteira.
- Interromper a publicação do achado se o trecho não tiver sido lido em contexto.
- Interromper a conclusão se a evidência depender apenas de documentação, comentário ou hipótese.
- Interromper a aprovação se a API, o README e a interface contradisserem o estado materializado sem reconciliação explícita.

## Resultado

- `status`: aprovada, aprovada com ressalvas, reprovada ou inconclusiva
- `responsavel`: preencher na execução
- `data`: preencher na execução
- `observacoes`: incluir comandos, testes e referências usados para confirmar ou derrubar hipóteses
