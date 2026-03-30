.PHONY: install help setup clean clean-all \
       lint format format-check lint-fix typecheck deadcode check \
       test ci web-ci reproduce \
       manifest-raw profile-staging validate-staging \
       audit-stage audit-curated audit-analytics audit audit-builder-validation validate-pipeline \
       staging curate \
       curate-process curate-decision-event curate-subject curate-party \
       curate-counsel curate-representation curate-entity-identifier curate-entity-reconciliation curate-links \
       curate-movement curate-session-event \
       analytics evidence scrape format \
       _ag-groups _ag-rapporteur _ag-assignment _ag-sequential _ag-temporal _ag-counsel \
       _ag-baseline _ag-alerts _ag-ml-outlier _ag-compound-risk \
       _ag-velocity _ag-rapporteur-change _ag-counsel-network \
       _ag-procedural-timeline _ag-pauta-anomaly _ag-representation-graph \
       _ag-representation-recurrence _ag-representation-windows \
       _ag-amicus-network _ag-firm-cluster \
       _ag-agenda-exposure agenda-fetch agenda-build agenda \
       _ag-heavy _ag-light \
       _check-env _ensure-no-heavy-fetch _stop-portal-fetch \
       pipeline-safe pipeline-contained \
       cgu cgu-fetch cgu-matches cgu-corporate-links \
       tse tse-fetch tse-matches tse-fetch-expenses tse-expenses \
       tse-party-org-fetch tse-party-org tse-counterparties tse-donor-links tse-empirical-report \
       cvm cvm-fetch cvm-matches \
       rfb rfb-fetch rfb-network rfb-groups \
       datajud datajud-fetch datajud-context \
       transparencia-fetch \
       stf-portal stf-portal-fetch oab-validate deoab deoab-fetch oab-sp oab-sp-fetch oab-sp-lookup \
       external-fetch fetch-all external-matches external-data \
       serving-build pipeline server-api web-dev web-build web-typecheck \
       docker-build docker-up \
       status runs tail-run explain-run resume resume-last

.DEFAULT_GOAL := install

ATLAS_STF_DB_URL ?= sqlite+pysqlite:///data/serving/atlas_stf.db
CLI = uv run python -m atlas_stf

# ===========================
# Setup e Limpeza
# ===========================
install: ## Instala dependencias Python (uv sync)
	uv sync

help: ## Mostra todos os targets disponíveis
	@grep -E '^[a-zA-Z0-9_.-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F ':.*## ' '{printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}' | sort

setup: install ## Configura ambiente completo (Python + Node + Playwright)
	cd web && npm ci
	uv run playwright install --with-deps chromium

clean: ## Remove artefatos de build e cache
	rm -rf build/ dist/ wheels/ *.egg-info/ .ruff_cache/ .pytest_cache/ htmlcov/ .coverage
	rm -rf web/.next web/tsconfig.tsbuildinfo web/tsconfig.typecheck.tsbuildinfo
	find src/ tests/ -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

clean-all: clean ## Remove build + dados gerados (preserva data/raw)
	rm -rf data/staging/ data/curated/ data/analytics/ data/evidence/ data/serving/

# ===========================
# Qualidade — rode com: make check -j3
# ===========================
lint: ## Verifica lint (ruff check)
	uv run ruff check src/ tests/

format: ## Formata codigo (ruff format)
	uv run ruff format src/ tests/

format-check: ## Verifica formatacao sem alterar (para CI)
	uv run ruff format --check src/ tests/

lint-fix: ## Corrige lint auto-fixaveis (ruff --fix)
	uv run ruff check --fix src/ tests/

typecheck: ## Verificacao de tipos (pyright strict)
	uv run pyright src/

deadcode: ## Detecta codigo morto (vulture)
	uv run vulture src/ --min-confidence 80

check: lint typecheck deadcode ## Lint + typecheck + deadcode

audit-integrity: ## Auditoria de integridade (fontes canônicas + propagação + fallback + frontend)
	cd $(CURDIR) && uv run python scripts/audit_integrity.py

audit-integrity-quick: ## Auditoria rápida (sem dados reais)
	cd $(CURDIR) && uv run python scripts/audit_integrity.py --quick

audit-pipeline: ## Auditoria de contratos do pipeline (artefatos reais)
	cd $(CURDIR) && PYTHONPATH=scripts uv run python scripts/audit_pipeline_contracts.py

audit-runtime: ## Auditoria com execução real de builders em amostra
	cd $(CURDIR) && PYTHONPATH=scripts uv run python scripts/audit_integrity.py --runtime

# ===========================
# Testes (paralelos via pytest-xdist -n 4)
# ===========================
test: ## Executa testes (pytest -n 4)
	uv run pytest

# ===========================
# CI e Reprodutibilidade
# ===========================
ci: check test web-ci ## Simula CI local (espelha GitHub Actions)
	@echo "CI local OK."

web-ci: ## CI do frontend (npm ci + lint + typecheck + build)
	cd web && npm ci
	cd web && npm run lint
	cd web && npm run typecheck
	cd web && npm run build

reproduce: ## Reproduz pipeline completo a partir de data/raw (sequencial)
	$(MAKE) clean-all
	$(MAKE) staging
	$(MAKE) curate
	$(MAKE) analytics
	$(MAKE) external-matches
	$(MAKE) evidence
	$(MAKE) serving-build
	@echo "Pipeline reproduzido com sucesso."

# ===========================
# Data Prep e Auditoria
# ===========================
manifest-raw: ## Gera manifesto dos CSVs brutos
	$(CLI) manifest raw

profile-staging: ## Relatorio de profiling do staging
	$(CLI) profile staging

validate-staging: ## Valida datasets do staging
	$(CLI) validate staging

audit-stage: ## Auditoria do staging
	$(CLI) audit stage

audit-curated: ## Auditoria do curated
	$(CLI) audit curated

audit-analytics: ## Auditoria do analytics
	$(CLI) audit analytics

audit: audit-stage audit-curated audit-analytics ## Auditoria completa (stage + curated + analytics)

audit-builder-validation: ## Audita cobertura de schema validation nos builders analytics
	python3 scripts/audit_builder_validation.py --scope analytics

validate-pipeline: ## Valida integridade referencial e cobertura dos artefatos do pipeline
	uv run python -m atlas_stf.validation.pipeline_integrity --scope all

# ===========================
# Staging e Curated
# ===========================
staging: ## Pipeline de staging (limpeza CSV)
	$(CLI) stage

curate: ## Curadoria de todas as entidades
	$(CLI) curate all

curate-process: ## Curadoria: processos
	$(CLI) curate process

curate-decision-event: ## Curadoria: eventos de decisao
	$(CLI) curate decision-event

curate-subject: ## Curadoria: assuntos
	$(CLI) curate subject

curate-party: ## Curadoria: partes
	$(CLI) curate party

curate-counsel: ## Curadoria: advogados
	$(CLI) curate counsel

curate-entity-identifier: ## Curadoria: identificadores de entidade
	$(CLI) curate entity-identifier

curate-entity-reconciliation: ## Curadoria: reconciliacao de entidades
	$(CLI) curate entity-reconciliation

curate-representation: ## Curadoria: rede de representacao
	$(CLI) curate representation

curate-links: ## Curadoria: vinculos processuais
	$(CLI) curate links

curate-movement: ## Curadoria: movimentacoes
	$(CLI) curate movement

curate-session-event: ## Curadoria: sessoes
	$(CLI) curate session-event

# ===========================
# Analytics — dependencias declaradas, rode com: make analytics -j6
#
# Grafo de dependencias:
#   groups --> baseline --> alerts --+--> ml-outlier
#                                   +--> compound-risk (+ counsel, velocity, rapporteur-change)
#   minister-flow: materializado pelo serving builder (nao precisa de passo separado)
#   rapporteur, assignment, sequential, temporal, counsel: independentes
#   velocity, rapporteur-change, counsel-network: independentes
# ===========================
_ag-groups:
	$(CLI) analytics build-groups

_ag-rapporteur:
	$(CLI) analytics rapporteur-profile

_ag-assignment:
	$(CLI) analytics assignment-audit

_ag-sequential:
	$(CLI) analytics sequential

_ag-temporal:
	$(CLI) analytics build-temporal-analysis

_ag-counsel:
	$(CLI) analytics counsel-affinity

_ag-baseline: _ag-groups
	$(CLI) analytics build-baseline

_ag-alerts: _ag-baseline
	$(CLI) analytics build-alerts

_ag-ml-outlier: _ag-alerts
	$(CLI) analytics ml-outlier

_ag-velocity:
	$(CLI) analytics decision-velocity

_ag-rapporteur-change:
	$(CLI) analytics rapporteur-change

_ag-counsel-network:
	$(CLI) analytics counsel-network

_ag-procedural-timeline:
	$(CLI) analytics procedural-timeline

_ag-pauta-anomaly:
	$(CLI) analytics pauta-anomaly

_ag-representation-graph:
	$(CLI) analytics representation-graph

_ag-representation-recurrence:
	$(CLI) analytics representation-recurrence

_ag-representation-windows:
	$(CLI) analytics representation-windows

_ag-amicus-network:
	$(CLI) analytics amicus-network

_ag-firm-cluster:
	$(CLI) analytics firm-cluster

_ag-agenda-exposure:
	$(CLI) analytics agenda-exposure

agenda-fetch: ## Baixa agenda ministerial (GraphQL STF)
	$(CLI) agenda fetch

agenda-build: ## Build eventos de agenda (requer agenda-fetch já executado)
	$(CLI) agenda build-events

agenda: agenda-fetch agenda-build _ag-agenda-exposure ## Pipeline agenda completo

_ag-compound-risk: _ag-alerts _ag-counsel _ag-velocity _ag-rapporteur-change
	$(CLI) analytics compound-risk

_ag-light: ## Analytics leves (paralelizaveis com -j4)
_ag-light: _ag-groups _ag-rapporteur _ag-assignment _ag-sequential _ag-counsel \
           _ag-baseline _ag-alerts _ag-ml-outlier _ag-velocity _ag-rapporteur-change \
           _ag-counsel-network _ag-procedural-timeline _ag-pauta-anomaly \
           _ag-representation-graph _ag-representation-recurrence _ag-representation-windows \
           _ag-amicus-network _ag-firm-cluster _ag-agenda-exposure

_ag-heavy: _ag-alerts _ag-counsel _ag-velocity _ag-rapporteur-change ## Analytics pesados (sequencial)
_ag-heavy: _ag-compound-risk

analytics: ## Todos os builders analiticos (use -j6)
analytics: _ag-light _ag-temporal _ag-heavy

evidence: ## Bundles de evidencia para alertas
	$(CLI) evidence build-all

transparencia-fetch: ## Baixa CSVs do painel STF
	$(CLI) transparencia fetch --headless --ignore-tls

scrape: transparencia-fetch ## Scraping completo (transparencia + jurisprudencia)
	$(CLI) scrape decisoes --ignore-tls
	$(CLI) scrape acordaos --ignore-tls

# ===========================
# Fontes externas — rode com: make external-data -j4
# ===========================
cgu-fetch: ## Baixa dados CGU (CEIS/CNEP/Leniencia)
	$(CLI) cgu fetch

cgu-matches: ## Build sanction matches CGU (requer cgu-fetch já executado)
	$(CLI) cgu build-matches

cgu-corporate-links: ## Build vinculos corporativos de sancionados (requer cgu-matches + rfb-fetch já executados)
	$(CLI) cgu build-corporate-links

cgu: cgu-fetch cgu-matches cgu-corporate-links ## Pipeline CGU completo

tse-fetch: ## Baixa doacoes eleitorais TSE
	$(CLI) tse fetch

tse-matches: ## Build donation matches TSE (requer tse-fetch já executado)
	$(CLI) tse build-matches

tse: tse-fetch tse-matches ## Pipeline TSE completo (doacoes)

tse-party-org-fetch: ## Baixa financas de orgaos partidarios TSE
	$(CLI) tse fetch-party-org

tse-fetch-expenses: ## Baixa despesas de campanha TSE
	$(CLI) tse fetch-expenses

tse-expenses: ## Pipeline TSE despesas (requer tse-fetch-expenses já executado)

tse-party-org: tse-party-org-fetch ## Pipeline TSE orgaos partidarios

tse-counterparties: ## Build contrapartes de pagamento (requer tse-party-org-fetch já executado)
	$(CLI) tse build-counterparties

tse-donor-links: ## Build vinculos corporativos de doadores (requer tse-fetch + rfb-fetch já executados)
	$(CLI) tse build-donor-links

tse-empirical-report: ## Relatorio empirico de qualidade TSE (requer tse-matches já executado)
	$(CLI) tse empirical-report

cvm-fetch: ## Baixa dados CVM
	$(CLI) cvm fetch

cvm-matches: ## Build sanction matches CVM (requer cvm-fetch já executado)
	$(CLI) cvm build-matches

cvm: cvm-fetch cvm-matches ## Pipeline CVM completo

rfb-fetch: ## Baixa dados RFB (CNPJ)
	$(CLI) rfb fetch

rfb-groups: ## Build grupos economicos (requer rfb-fetch já executado)
	$(CLI) rfb build-groups

rfb-network: ## Build rede corporativa (requer rfb-fetch + rfb-groups já executados)
	$(CLI) rfb build-network

rfb: rfb-fetch rfb-groups rfb-network ## Pipeline RFB completo

datajud-fetch: ## Baixa dados DataJud
	$(CLI) datajud fetch

datajud-context: ## Build contexto de origem DataJud (requer datajud-fetch já executado)
	$(CLI) datajud build-context

datajud: datajud-fetch datajud-context ## Pipeline DataJud completo

stf-portal-fetch: ## Baixa linha do tempo portal STF
	$(CLI) stf-portal fetch --ignore-tls --rate-limit 0.8 --tab-concurrency 3

stf-portal: stf-portal-fetch ## Pipeline portal STF

oab-validate: ## Valida numeros OAB
	$(CLI) oab validate --provider null

deoab-fetch: ## Baixa e parseia diários OAB (sociedades de advocacia)
	$(CLI) deoab fetch

deoab: deoab-fetch ## Pipeline DEOAB completo

oab-sp-fetch: ## Busca detalhes de sociedades na OAB/SP
	$(CLI) oab-sp fetch

oab-sp-lookup: ## Busca advogados no cadastro OAB/SP
	$(CLI) oab-sp lookup

oab-sp: oab-sp-fetch oab-sp-lookup ## Pipeline OAB/SP completo (sociedades + advogados)

external-fetch: cgu-fetch tse-fetch cvm-fetch rfb-fetch ## Baixa todas as fontes externas (CGU/TSE/CVM/RFB)
fetch-all: scrape external-fetch tse-fetch-expenses tse-party-org-fetch stf-portal-fetch agenda-fetch deoab-fetch ## Baixa TUDO (STF + externas + agenda + DEOAB)
external-matches: cgu-matches tse-matches cvm-matches rfb-network ## Build todos os matches externos
external-data: cgu tse cvm rfb ## Pipeline completo de fontes externas

# ===========================
# Verificacao de ambiente
# ===========================

define _PY_CHECK_ENV
import os, sys, subprocess
lines = open('/proc/meminfo').readlines()
info = {l.split(':')[0]: int(l.split()[1]) for l in lines if len(l.split()) >= 2}
avail_gb = info.get('MemAvailable', 0) / 1048576
swap_free_gb = info.get('SwapFree', 0) / 1048576
swap_total_gb = info.get('SwapTotal', 0) / 1048576
print(f'RAM disponivel: {avail_gb:.1f} GB')
print(f'Swap: {swap_free_gb:.1f} / {swap_total_gb:.1f} GB livre')
my_pid = str(os.getpid())
result = subprocess.run(['pgrep', '-af', 'stf-portal.*fetch|curate.*all'], capture_output=True, text=True)
for p in result.stdout.strip().splitlines():
    if p and not p.startswith(my_pid + ' '):
        print(f'Processo pesado: {p}')
if avail_gb < 10:
    print('RAM disponivel < 10 GB. Pare processos pesados antes de continuar.')
    sys.exit(1)
print(f'RAM suficiente para pipeline ({avail_gb:.1f} GB disponivel).')
endef
export _PY_CHECK_ENV

define _PY_ENSURE_NO_HEAVY_FETCH
import os, subprocess, sys
my_pid = str(os.getpid())
result = subprocess.run(['pgrep', '-af', 'stf-portal.*fetch'], capture_output=True, text=True)
lines = [l for l in result.stdout.strip().splitlines() if l and not l.startswith(my_pid + ' ')]
if lines:
    print('Portal fetch ativo — pare antes de rodar pipeline-safe:')
    print('\n'.join(lines))
    print('Use: make _stop-portal-fetch')
    sys.exit(1)
print('Nenhum portal fetch ativo.')
endef
export _PY_ENSURE_NO_HEAVY_FETCH

define _PY_STOP_PORTAL_FETCH
import os, sys
pid_path = 'data/raw/stf_portal/.fetch.pid'
if not os.path.exists(pid_path):
    print('Portal fetch nao esta rodando.')
    sys.exit(0)
pid = int(open(pid_path).read().strip())
cmdline = ''
try:
    cmdline = open(f'/proc/{pid}/cmdline').read()
except FileNotFoundError:
    pass
if not cmdline or ('stf-portal' not in cmdline and 'stf_portal' not in cmdline):
    print(f'PID {pid}: stale, removendo.')
    os.unlink(pid_path)
    sys.exit(0)
os.kill(pid, 15)
print(f'SIGTERM enviado para PID {pid}.')
endef
export _PY_STOP_PORTAL_FETCH

_check-env: ## Verifica RAM e processos pesados antes de rodar pipeline
	@python3 -c "$$_PY_CHECK_ENV"

_ensure-no-heavy-fetch: ## Falha se portal fetch estiver ativo
	@python3 -c "$$_PY_ENSURE_NO_HEAVY_FETCH"

_stop-portal-fetch: ## Para o portal fetch de forma segura (valida PID + cmdline)
	@python3 -c "$$_PY_STOP_PORTAL_FETCH"

# ===========================
# Serving e Pipeline — rode com: make pipeline -j6
# ===========================
serving-build: ## Materializa banco SQLite para API (ATLAS_FLOW_WORKERS=N para override, default=min(4,cpus))
	$(CLI) serving build --database-url "$(ATLAS_STF_DB_URL)"

pipeline-safe: _check-env _ensure-no-heavy-fetch ## Pipeline seguro (staging -> serving, com isolamento de memoria)
	@echo "=== Fase 1: Staging ==="
	$(MAKE) staging
	@echo "=== Fase 2: Curate ==="
	$(MAKE) curate
	@echo "=== Fase 3: Analytics leves (-j4) + temporal (se RAM > 20 GB) ==="
	@avail=$$(awk '/MemAvailable/ {print int($$2/1048576)}' /proc/meminfo); \
	if [ "$$avail" -ge 20 ]; then \
		echo "RAM $$avail GB — temporal roda em paralelo com leves"; \
		$(MAKE) -j4 _ag-light _ag-temporal; \
	else \
		echo "RAM $$avail GB < 20 GB — temporal roda sequencial apos leves"; \
		$(MAKE) -j4 _ag-light; \
		$(MAKE) _ag-temporal; \
	fi
	@echo "=== Fase 4: Analytics pesados (sequencial) ==="
	$(MAKE) _ag-heavy
	@echo "=== Fase 5: Matches externos ==="
	$(MAKE) external-matches
	$(MAKE) tse-counterparties tse-donor-links tse-empirical-report
	@echo "=== Fase 6: Evidence + Serving ==="
	$(MAKE) evidence
	$(MAKE) serving-build
	@echo "Pipeline completo concluido."

pipeline-contained: _check-env _ensure-no-heavy-fetch ## Pipeline com contencao cgroup (experimental)
	$(MAKE) staging
	systemd-run --user --scope -p MemoryMax=12G --same-dir $(MAKE) curate
	$(MAKE) -j4 _ag-light
	systemd-run --user --scope -p MemoryMax=10G --same-dir $(MAKE) _ag-heavy
	$(MAKE) external-matches
	$(MAKE) tse-counterparties tse-donor-links tse-empirical-report
	$(MAKE) evidence serving-build

pipeline: scrape staging curate analytics external-data evidence serving-build ## Pipeline completo (scrape -> serving)
	@echo "Pipeline completo. Rode 'make server-api' e 'make web-dev' para subir."

# ===========================
# Servidores
# ===========================
server-api: ## Inicia servidor API (FastAPI + Uvicorn)
	ATLAS_STF_DATABASE_URL="$(ATLAS_STF_DB_URL)" $(CLI) api serve --host 127.0.0.1 --port 8000

web-dev: ## Inicia dev server frontend
	cd web && ATLAS_STF_API_BASE_URL="http://127.0.0.1:8000" npm run dev

web-build: ## Build de producao do frontend
	cd web && npm run build

web-typecheck: ## Typecheck do frontend
	cd web && npm run typecheck

# ===========================
# Docker
# ===========================
docker-build: ## Build das imagens Docker
	docker compose build

docker-up: ## Sobe containers Docker
	docker compose up --build

# ===========================
# Operacional — monitoramento de runs
# ===========================
status: ## Mostra runs ativos
	$(CLI) status $(if $(BUILDER),--builder $(BUILDER),)

runs: ## Lista execucoes recentes
	$(CLI) runs $(if $(BUILDER),--builder $(BUILDER),)

tail-run: ## Acompanha eventos de um run (requer RUN=...)
	$(CLI) tail-run $(RUN)

explain-run: ## Mostra manifesto de um run (requer RUN=...)
	$(CLI) explain-run $(RUN)

resume: ## Retoma run do checkpoint (requer RUN=...)
	$(CLI) resume --run-id $(RUN)

resume-last: ## Retoma ultimo run falhado de um builder (requer BUILDER=...)
	$(CLI) resume --builder $(BUILDER)
