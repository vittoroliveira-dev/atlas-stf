# Gold Set — Protocolo

## Onde fica

- Gold set: `data/analytics/gold_set_matches.jsonl`
- Sumário: `data/analytics/gold_set_summary.json`

## O que é

Conjunto de matches reais do Atlas STF com **dois níveis de rotulagem**:

1. **Rótulo heurístico** (`heuristic_label`) — sugestão algorítmica derivada do pipeline
2. **Rótulo final** (`final_label`) — decisão adjudicada por uma de três vias:
   - `evidence_deterministic` — fatos verificáveis independentes (CPF, nome legal PJ)
   - `operator_delegated_curatorial` — revisão curatorial delegada com critérios conservadores
   - `human_review` — revisão humana literal

Só o rótulo final constitui a verdade de referência do gold set.

## O que NÃO é

- Não é conjunto heurístico renomeado como "gold"
- Não contém dados sintéticos, mocks ou exemplos inventados
- Não substitui revisão humana por auto-rotulagem circular

## Tipos de adjudicação

| Tipo | Significado | Quem decide |
|---|---|---|
| `evidence_deterministic` | O rótulo segue de fatos independentes verificáveis (CPF, nome único PJ, nomes claramente diferentes) | Algorítmico com evidência externa |
| `operator_delegated_curatorial` | Revisão curatorial delegada — cada registro inspecionado individualmente com critérios conservadores documentados | Agente instrúido pelo operador |
| `heuristic_provisional` | Sugestão do pipeline, **pendente revisão** | Ninguém ainda — aguarda revisão |
| `human_review` | Humano revisou o caso e decidiu o rótulo final | Revisor identificado |

Registros `heuristic_provisional` têm `final_label: null` e não contam para o gate mínimo.

## Independência metodológica

O rótulo final NÃO pode depender da heurística do pipeline auditado:

- **CPF como identificador**: O CPF é emitido pela Receita Federal. Se doador tem CPF e nome idêntico ao da entidade, a identidade é confirmada por fonte externa.
- **Nome corporativo PJ**: Razão social é registro legal único no Brasil.
- **Nomes claramente diferentes**: SAMIR ≠ JAMIL é fato linguístico, não heurística.
- **Múltiplos candidatos**: Irresolvibilidade é fato estrutural.

Para tudo que depende de julgamento (fuzzy matching, bordeline, counsel, SCL), é necessária **revisão humana**.

## Comandos

```bash
# Gerar gold set a partir dos dados de produção
uv run python scripts/build_gold_set.py generate

# Revisão humana interativa dos registros pendentes
uv run python scripts/build_gold_set.py review

# Sumário do estado atual
uv run python scripts/build_gold_set.py summary

# Via Makefile
make gold-set
```

## Mínimo obrigatório

- Total ≥ **100 registros** (`MINIMUM_GOLD_SET_SIZE`)
- Estratos obrigatórios: `counsel_match`, `levenshtein_dist1`, `scl_degree2`
- O pipeline falha (`FAIL`) se qualquer requisito for violado

## Campos do registro

| Campo | Tipo | Descrição |
|---|---|---|
| `case_id` | string | Identificador estável (`gs-NNNN`) |
| `stratum` | string | Estrato do match |
| `source` | string | Arquivo de origem |
| `match_id` | string | ID do match original |
| `donor_identity_key` | string | Chave de identidade do doador |
| `donor_name` | string | Nome do doador |
| `entity_id` | string | ID da entidade candidata |
| `entity_name` | string | Nome da entidade candidata |
| `match_strategy` | string | Estratégia de matching |
| `match_score` | number | Score do match |
| `has_tax_id` | boolean | Se há CPF/CNPJ |
| `heuristic_label` | string | Sugestão algorítmica |
| `heuristic_basis` | string | Base da sugestão |
| `final_label` | string\|null | Rótulo final (null = pendente) |
| `adjudication_type` | string | Tipo de adjudicação |
| `adjudicator` | string | Quem/o que adjudicou |
| `adjudication_evidence` | string | Evidência da decisão |
| `adjudication_date` | string | Data da adjudicação |
| `labeling_rule` | string | Referência ao código da heurística |

## Estratos

### Doações (party)
| Estrato | Adjudicação padrão |
|---|---|
| `exact_with_cpf` | evidence_deterministic |
| `exact_name_fallback` | evidence_deterministic |
| `canonical_with_cpf` | evidence_deterministic |
| `canonical_no_cpf` | evidence_deterministic |
| `jaccard_high` | heuristic_provisional |
| `jaccard_borderline` | heuristic_provisional |
| `levenshtein_dist1` | heuristic_provisional |
| `levenshtein_dist2` | heuristic_provisional |
| `ambiguous_multi` | evidence_deterministic |

### Counsel
| Estrato | Adjudicação padrão |
|---|---|
| `counsel_match` | heuristic_provisional |

### Sanções
| Estrato | Adjudicação padrão |
|---|---|
| `sanction_match` | heuristic_provisional |

### SCL
| Estrato | Adjudicação padrão |
|---|---|
| `scl_degree2` | heuristic_provisional |

## Fluxo de revisão humana

1. Gerar gold set: `uv run python scripts/build_gold_set.py generate`
2. Revisar pendentes: `uv run python scripts/build_gold_set.py review`
   - O revisor vê cada caso com toda evidência
   - Confirma ou altera o rótulo heurístico
   - A decisão é registrada com nome, data e evidência
3. Verificar: `uv run python scripts/build_gold_set.py summary`

## Limitações

- 0% dos registros passou por `human_review` literal — a adjudicação foi feita por `evidence_deterministic` (60 registros) e `operator_delegated_curatorial` (115 registros)
- A revisão curatorial delegada seguiu critérios conservadores documentados, mas não substitui revisão humana formal
- A cobertura de counsel e SCL depende da existência de dados de produção
- Registros `scl_degree2` (15) são todos `ambiguous` por natureza do vínculo indireto
- O gold set pode ser aprimorado via `uv run python scripts/build_gold_set.py review` para elevar registros a `human_review`
