# Checklist de auditoria — dados e ingestão

## Objetivo

Validar se a ingestão dos dados preservou origem, integridade e rastreabilidade.

## Aplicação

Usar este checklist sempre que um novo arquivo, exportação ou lote de dados for incorporado ao projeto.

## Itens obrigatórios

- [ ] A origem do arquivo foi registrada.
- [ ] A data da coleta foi registrada.
- [ ] O nome original do arquivo foi preservado.
- [ ] O arquivo foi armazenado na camada `raw/`.
- [ ] O hash ou identificador técnico do arquivo foi registrado.
- [ ] O filtro ou recorte da exportação foi registrado, quando conhecido.
- [ ] Foi verificado se o arquivo é duplicado de outro já existente.
- [ ] A quantidade de linhas e colunas foi registrada.
- [ ] Os tipos aparentes de campo foram inspecionados.
- [ ] A existência de colunas-chave foi confirmada.
- [ ] O join com outras tabelas esperadas foi testado, quando aplicável.
- [ ] Campos ausentes ou inconsistentes foram registrados.
- [ ] A cobertura do arquivo foi tratada como `INCERTA` quando não comprovada.

## Critérios de bloqueio

A etapa deve ser interrompida se:
- a origem não estiver registrada;
- o arquivo não puder ser relacionado à camada `raw/`;
- as chaves mínimas não existirem;
- houver suspeita forte de duplicidade não resolvida.

## Resultado

- Status:
- Responsável:
- Data:
- Observações:
