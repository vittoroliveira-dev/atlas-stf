# Camada raw — jurisprudência

## Objetivo

Preservar o material bruto coletado da pesquisa pública de jurisprudência do STF, sem transformação destrutiva.

## Estrutura

```text
jurisprudencia/
├── decisoes/
└── acordaos/
```

## Conteúdo esperado

- arquivos `JSONL` particionados por mês;
- um diretório para decisões monocráticas;
- um diretório para acórdãos;
- trilha de auditoria da coleta no mesmo nível operacional da extração.

## Uso atual no projeto

Esta fonte já pode ser usada como enriquecimento complementar da camada `curated`, especialmente para:

- `juris_inteiro_teor_url`;
- `juris_doc_id`;
- `juris_decisao_texto`;
- `juris_ementa_texto`;
- `juris_partes`;
- `juris_legislacao_citada`;
- `juris_procedencia`;
- `juris_classe_extenso`;
- contagem e presença de documentos por processo.

## Limites

- a cobertura por tipo de peça continua `INCERTA`;
- o casamento entre processo estruturado e documento textual depende de normalização do código processual e, no caso de eventos decisórios, de coincidência de data;
- ausência de match não deve ser tratada como ausência real de documento oficial.

## Regras

1. Não editar os `JSONL` brutos manualmente.
2. Não sobrescrever partições sem manter trilha de auditoria.
3. Sempre separar `decisoes` e `acordaos`.
4. Toda inferência derivada desta camada deve preservar a referência textual de origem quando houver.
