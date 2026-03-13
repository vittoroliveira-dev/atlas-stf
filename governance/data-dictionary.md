# Dicionário de dados

## Objetivo

Descrever o significado operacional dos campos da base inicial, sem assumir semântica não comprovada.

## Tabela principal

### `idFatoDecisao`
Identificador do fato decisório na origem.
Uso: chave técnica do evento decisório.

### `Processo`
Identificador do processo.
Uso: chave lógica de ligação entre tabelas.

### `Relator atual`
Nome do relator atual associado ao registro.
Uso: perfil por ministro e agrupamento institucional.

### `Meio Processo`
Campo de classificação operacional da origem.
Uso: manter valor bruto; semântica refinada depende de validação.

### `Origem decisão`
Indica a origem da decisão conforme a fonte.
Uso: análise descritiva e possível feature comparativa.

### `Ambiente julgamento`
Classificação do ambiente do julgamento.
Uso: manter valor bruto; normalização posterior.

### `Data de autuação`
Data de autuação do processo.
Uso: linha do tempo processual.

### `Data baixa`
Data de baixa, quando disponível.
Uso: análise temporal e encerramento.

### `Indicador colegiado`
Campo bruto associado à colegialidade.
Uso: derivar flag `is_collegiate` após normalização.

### `Ano da decisão`
Ano do evento decisório.
Uso: agregação temporal.

### `Data da decisão`
Data do evento decisório.
Uso: ordenação temporal e janela analítica.

### `Tipo decisão`
Tipo do evento decisório.
Uso: composição de grupos comparáveis.

### `Andamento decisão`
Classificação do andamento ou resultado.
Uso: feature central para baseline e atipicidade.

### `Observação do andamento`
Texto complementar do andamento.
Uso: manter bruto; possível enriquecimento posterior.

### `Ramo direito`
Classificação do ramo do direito.
Uso: agrupamento temático.

### `Assuntos do processo`
Descrição bruta dos assuntos.
Uso: normalização temática posterior.

### `Indicador de tramitação`
Campo bruto sobre tramitação.
Uso: manter bruto até validação formal.

### `Órgão julgador`
Órgão julgador associado ao registro.
Uso: agrupamento e comparabilidade.

### `Descrição Procedência Processo`
Descrição da procedência.
Uso: feature contextual.

### `Descrição Órgão Origem`
Descrição do órgão de origem.
Uso: feature contextual.

## Tabela complementar

### `Polo ativo`
Parte(s) do polo ativo.
Uso: identificação de atores.

### `Polo passivo`
Parte(s) do polo passivo.
Uso: identificação de atores.

### `Advogado polo ativo`
Advogado(s) do polo ativo.
Uso: rede de atores e frequência.

### `Advogado polo passivo`
Advogado(s) do polo passivo.
Uso: rede de atores e frequência.

## Tabela de sanções (CEIS/CNEP/CVM)

Dados de sanções são armazenados na mesma tabela `serving_sanction_match`, distinguidos por `sanction_source`.

### `match_id`
Identificador determinístico do cruzamento (hash de party_id + sanction_id).
Uso: chave primária do match.

### `sanction_source`
Origem da sanção: `ceis`, `cnep`, `cvm` ou `leniencia`.
Uso: discriminar tipo de cadastro e fonte.

### `sanction_type`
Tipo de sanção aplicada (ex: "Inidoneidade", "Impedimento").
Uso: classificação da gravidade.

### `red_flag`
Booleano indicando taxa de êxito atípica da parte sancionada.
Uso: priorização de revisão.

### Campos CVM-específicos (mapeamento)
- `numero_processo` → `sanction_id`
- `assunto` → `sanction_type`
- `data_abertura` → `sanction_start_date`
- `nome_acusado` → `entity_name`
- `ementa` → `sanction_description`
- `sanctioning_body` = "CVM" (fixo)

### Campos Leniência-específicos (mapeamento)
- `CNPJ DO SANCIONADO` → `entity_cnpj_cpf`
- `NOME INFORMADO PELO ÓRGÃO SANCIONADOR` → `entity_name` (fallback: `RAZÃO SOCIAL`)
- `NÚMERO DO PROCESSO` → `sanction_id`
- `DATA INÍCIO ACORDO` → `sanction_start_date`
- `DATA CONCLUSÃO ACORDO` → `sanction_end_date`
- `ÓRGÃO SANCIONADOR` → `sanctioning_body`
- `SITUAÇÃO DO ACORDO` → `sanction_type`

## Tabela de doações eleitorais (TSE)

### `match_id`
Identificador determinístico do cruzamento (hash de party_id + donor_cpf_cnpj).
Uso: chave primária do match.

### `donor_cpf_cnpj`
CPF ou CNPJ do doador conforme declaração ao TSE.
Uso: identificação do doador.

### `total_donated_brl`
Valor total doado em reais (soma de todas as doações do doador).
Uso: dimensionamento do volume financeiro.

### `donation_count`
Número de registros de doação encontrados para o doador.
Uso: frequência de participação eleitoral.

### `election_years`
Lista de anos eleitorais em que o doador contribuiu.
Uso: cobertura temporal da atividade de doação.

### `red_flag`
Booleano indicando taxa de êxito atípica da parte doadora no STF.
Uso: priorização de revisão.

## Tabela de rede corporativa (RFB)

Dados de vínculos societários são armazenados na tabela `serving_corporate_conflict`.

### `conflict_id`
Identificador determinístico do vínculo (hash de minister_name + cnpj_basico + linked_entity_id).
Uso: chave primária do conflito.

### `minister_name`
Nome do ministro do STF que é sócio da empresa.
Uso: identificação do ministro no vínculo.

### `linked_entity_type`
Tipo da entidade vinculada: `party` ou `counsel`.
Uso: discriminar se o co-sócio é parte ou advogado no STF.

### `linked_entity_name`
Nome normalizado da parte ou advogado co-sócio.
Uso: identificação da entidade vinculada.

### `red_flag`
Booleano indicando taxa de êxito atípica quando ministro é relator e co-sócio é parte.
Uso: priorização de revisão.

## Tabela de afinidade ministro-advogado

Dados de afinidade são armazenados na tabela `serving_counsel_affinity`.

### `affinity_id`
Identificador determinístico do par (hash de rapporteur + counsel_id).
Uso: chave primária do par.

### `rapporteur`
Nome do ministro relator.
Uso: identificação do ministro no par.

### `counsel_id`
Identificador do advogado.
Uso: identificação do advogado no par.

### `pair_favorable_rate`
Taxa de decisões favoráveis do par (ministro, advogado).
Uso: comparação com baselines.

### `pair_delta_vs_minister`
Diferença entre taxa do par e baseline do ministro.
Uso: quantificação do desvio.

### `red_flag`
Booleano indicando delta > 0.15 e >= 5 casos.
Uso: priorização de revisão.

## Regras

1. Nunca alterar ou apagar o valor bruto.
2. Toda interpretação refinada deve gerar campo derivado.
3. Quando a semântica for incerta, registrar `INCERTO`.
