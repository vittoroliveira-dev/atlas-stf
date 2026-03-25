# Manifesto unificado de fetch

- **Status:** accepted
- **Data:** 2026-03-25

## Contexto

Cada fonte externa (TSE, CGU, CVM, RFB, DataJud) mantinha seu próprio sistema de
checkpoint (`_checkpoint.json`) com formato ad-hoc, sem auditabilidade e sem
visibilidade cruzada. Não havia como saber o estado de todas as fontes de uma vez,
migrar formatos, ou planejar downloads antes de executá-los.

## Decisão

Manifesto JSON por fonte (`_manifest_{source}.json`) como única fonte de verdade
para proveniência de downloads. Implementado em `src/atlas_stf/fetch/`:

- **Model** (`_manifest_model.py`) — tipos imutáveis: `FetchUnit`, `RemoteState`,
  `SourceManifest`, `FetchPlan`, `PlanItem`, `RefreshPolicy`, `PolicySnapshot`
- **Store** (`_manifest_store.py`) — `save_manifest_locked()` (com FetchLock) e
  `write_manifest_unlocked()` (para uso dentro de lock externo)
- **Discovery** (`_discovery.py`) — enumeração de unidades por fonte
- **Probe** (`_remote_probe.py`) — HEAD/Range para capturar `RemoteState` fresco
- **Planner** (`_manifest_planner.py`) — plan read-only, sem side effects
- **Executor** (`_executor.py`) — despacha `PlanItem` para runners por fonte
- **Migration** (`_migration.py`) — transacional: marker → write → validate → remove legacy
- **Adapters** (`_adapter.py`) — Protocol `FetchSourceAdapter` com 7 implementações

Decisões derivadas:
- `plan_id` é SHA-256 do payload canônico (excluindo `created_at`)
- `save_manifest_locked()` adquire/libera lock internamente (non-reentrant)
- DataJud usa `supports_deferred_run=False` — `fetch run --plan` rejeita planos pré-gerados
- `allow_weak_skip=False` para CGU e RFB (size-only match não é prova suficiente)
- Serialização determinística: `sort_keys=True`, items por `source→unit_id`

## Consequências

### Positivas

- Estado de todas as fontes visível com `atlas-stf fetch status`
- Planejamento separado de execução (`fetch plan` → review → `fetch run`)
- Migração transacional com recovery automático de interrupção
- Adapters uniformes — nova fonte = nova classe, registrar no dict

### Negativas

- `write_manifest_unlocked()` exposto como API interna — risco de uso sem lock
- Adapters delegam para discovery/probe existentes — indireção extra sem lógica nova
- RFB usa bridge dict↔manifest (passes mutam dict internamente)

## Evidência no código

- `src/atlas_stf/fetch/` — 9 módulos, ~1400 LOC
- `tests/fetch/` — 107 testes
- Checkpoints legados removidos de: tse, cgu, cvm, rfb, datajud
- `cgu/_checkpoint.py` deletado
- CLI: `atlas-stf fetch {plan,status,run,migrate}`
