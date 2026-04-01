# Benchmark — Minister Flow (Phase 2)

## O que mede

Tempo de computação da Phase 2 do serving build: materialização dos
minister flows (162K+ combinações de filtros sobre ~411K cases).

## Como rodar

```bash
# Full benchmark (todas as 162K+ tasks)
make benchmark-flow

# Quick smoke (primeiras N tasks)
uv run python scripts/benchmark_minister_flow.py --tasks 1000

# Custom output path
uv run python scripts/benchmark_minister_flow.py --output data/benchmarks/custom.json
```

## Artefato de saída

JSON em `data/benchmarks/minister_flow.json` com:

| Campo | Descrição |
|---|---|
| `commit` | Hash curto do commit |
| `timestamp` | ISO 8601 UTC |
| `machine` | Hostname |
| `workers` | Número de workers (`ATLAS_FLOW_WORKERS`, default 1) |
| `scenario` | `full` ou `subset_N` |
| `durations_seconds` | Tempo por sub-etapa (load, index, enumerate, hist_cache, compute, total) |
| `throughput` | flows/s e ms/flow |
| `memory_mb` | RSS início e fim |
| `output_equivalence` | Fingerprint SHA256 dos outputs para checagem de equivalência |

## Número de referência

**Referência principal**: duração do campo `compute` no cenário `full` com `workers: 1`.

Esse é o número comparável entre runs porque:
- Exclui I/O de DB (load) e overhead de indexação (fixos)
- Exclui ORM insert (não faz parte da computação)
- Usa mesmo scope (todas as tasks disponíveis)

## Comparação com baseline

| Cenário | Fonte | Tempo | Contexto |
|---|---|---|---|
| **Baseline (pré-otimização)** | `data/serving_build.log` | 13.253s | Inclui ORM insert. Serial. |
| **Otimizado (compute only)** | `data/benchmarks/minister_flow.json` | ~945s | Sem ORM insert. Serial. |

**Ganho comparável**: ~14× no compute puro. O baseline inclui ORM insert overhead
(estimado em ~5%), portanto o speedup end-to-end no serving build é ~13×.

### Comparações que NÃO devem ser feitas

- Não comparar subset com full (escopos diferentes)
- Não comparar workers=1 com workers=4 como se fosse ganho da otimização
- Não comparar benchmark (sem ORM) com build log (com ORM) sem qualificar

## Equivalência de output

O fingerprint `output_equivalence.fingerprint` é um SHA256 truncado de
`key:event_count:historical_event_count` por flow. Dois runs com mesmo
fingerprint produzem os mesmos resultados agregados.

**Nível de equivalência**: determinístico para contagens (event_count,
historical_event_count). Não verifica JSON completo dos flows (daily_counts,
segment_flows) — estes dependem da ordem de iteração em dicts internos.

## Limitações

- Benchmark mede compute isolado, não o build completo
- RSS reportado é do processo Python, não do pico do fork pool
- Sem repetição automática (rodar N vezes manualmente para variância)
- Fingerprint verifica contagens, não equivalência byte-a-byte total
