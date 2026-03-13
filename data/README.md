# Dados do projeto

## Objetivo

Esta pasta organiza todo o ciclo de vida dos dados do projeto, desde o material bruto de origem até os conjuntos derivados usados em análise, serving e evidência.

## Princípios

1. O dado bruto nunca deve ser sobrescrito.
2. Toda transformação deve ser rastreável.
3. Toda camada deve ter função clara.
4. Toda saída analítica deve poder ser reconstruída a partir das camadas anteriores.

## Estrutura

```text
data/
├── raw/
├── staging/
├── curated/
├── analytics/
├── evidence/
└── serving/
```

## Camadas

### `raw/`

Armazena dados exatamente como foram recebidos ou coletados.

### `staging/`

Armazena dados limpos e padronizados minimamente, ainda muito próximos da origem.

### `curated/`

Armazena entidades canônicas do projeto, como processo, decisão, parte e advogado.

### `analytics/`

Armazena features derivadas, grupos comparáveis, baselines, alertas e saídas analíticas.

### `evidence/`

Armazena bundles JSON e relatórios Markdown por alerta materializado.

### `serving/`

Armazena o banco derivado consumido pela API e pelo dashboard. O caminho canônico atual é `data/serving/atlas_stf.db`.

## Regras

- nenhuma camada posterior pode apagar evidência da camada anterior;
- toda transformação deve ser documentada;
- toda incerteza deve ser preservada em vez de mascarada;
- `serving/` e `evidence/` são artefatos derivados; não substituem `raw/`, `staging/`, `curated/` ou `analytics/` como trilha primária.
