# Glossário

## Alerta
Registro analítico que indica um caso ou subconjunto que pode merecer verificação externa ou leitura documental adicional.

## Atipicidade
Desvio observável em relação a um baseline definido.

## Baseline
Padrão esperado em um grupo comparável.

## Camada raw
Nível em que os dados são preservados exatamente como recebidos.

## Camada staging
Nível em que os dados passam por limpeza e padronização mínima.

## Camada curated
Nível em que entidades canônicas são produzidas.

## Casos comparáveis
Conjunto de casos agrupados segundo critérios explícitos de proximidade processual e decisória.

## Corpus inicial
Conjunto de arquivos estruturados recebidos do portal de transparência.

## Decisão-evento
Registro de um fato decisório vinculado a um processo.

## Descritivo
Tipo de análise focada em distribuição, contagem e perfil.

## Divergência aparente
Diferença observável que ainda não foi validada por camada textual complementar ou verificação externa.

## Evidência
Conjunto de informações que sustenta uma análise ou alerta.

## Grupo comparável
Subconjunto formalizado de casos usados para comparação.

## Inconclusivo
Rótulo usado quando os dados não sustentam conclusão útil.

## INCERTO
Marcador obrigatório para afirmações ou campos não comprovados.

## Outlier
Observação que se afasta de forma relevante do padrão esperado.

## Parte
Pessoa física, jurídica ou ente processual associado ao processo.

## Priorização
Ordenação de casos com base em relevância para aprofundamento documental ou inspeção externa.

## Processo
Unidade jurídica principal do corpus.

## Verificação externa
Leitura adicional, opcional e fora do fluxo do sistema, feita por quem desejar inspecionar um alerta com mais profundidade.

## Score de atipicidade
Medida sintética de desvio em relação ao baseline.

## Trilha de auditoria
Conjunto de registros que permite reconstruir origem, regra e racional de uma saída.

## Sanção (sanction_match)
Registro de cruzamento entre uma parte processual do STF e uma sanção pública (CEIS, CNEP, Leniência da CGU ou processo sancionador da CVM).

## Doação (donation_match)
Registro de cruzamento entre uma parte processual do STF e um doador de campanha eleitoral registrado no TSE.

## Vínculo corporativo (corporate_conflict)
Registro de co-participação societária entre um ministro do STF e uma parte ou advogado, detectado via dados abertos de CNPJ da Receita Federal.

## Afinidade ministro-advogado (counsel_affinity)
Par (ministro, advogado) cuja taxa de vitória observada se afasta significativamente do baseline individual de cada um, derivado apenas de dados curated internos.

## Risco composto (compound_risk)
Índice consolidado que agrega evidências de sanções, doações, vínculos corporativos e afinidade num ranking unificado por entidade.

## Análise temporal (temporal_analysis)
Análise de padrões decisórios ministeriais ao longo do tempo, incluindo tendências mensais, eventos significativos e cruzamento com rede corporativa.

## Red flag
Indicador binário de que uma entidade ou relação apresenta combinação de sinais que merece atenção prioritária. Não equivale a irregularidade comprovada.

## Serving database
Banco SQLite derivado (24 tabelas) que materializa artefatos curated e analytics para consumo pela API e pelo dashboard.

## Contexto de origem (origin_context)
Agregação estatística de tribunais de origem derivada da API CNJ DataJud, usada para contextualizar a procedência dos processos que chegam ao STF.
