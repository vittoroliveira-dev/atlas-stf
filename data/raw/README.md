# Camada raw

## Objetivo

Preservar os dados originais exatamente como foram recebidos ou coletados, sem transformação destrutiva.

## Estrutura

```text
raw/
├── transparencia/
├── jurisprudencia/
├── dje/
├── datajud/          # agregacoes da API CNJ DataJud (opcional)
└── external_events/
```

## Regras

1. Não renomear arquivos de forma opaca.
2. Registrar origem, data da coleta e hash.
3. Não alterar conteúdo bruto.
4. Se houver nova exportação, manter versionamento ou diretório por data.
5. Sempre registrar se o recorte é total ou parcial, quando isso for conhecido.

## Observação

A camada raw é a referência máxima para auditoria de origem.
