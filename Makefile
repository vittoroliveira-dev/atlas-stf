.PHONY: install help setup clean clean-all \
       lint format format-check lint-fix typecheck deadcode check \
       test ci web-ci reproduce \
       manifest-raw profile-staging validate-staging \
       audit-stage audit-curated audit-analytics audit \
       staging curate \
       curate-process curate-decision-event curate-subject curate-party \
       curate-counsel curate-representation curate-entity-identifier curate-entity-reconciliation curate-links \
       analytics evidence scrape format \
       _ag-groups _ag-rapporteur _ag-assignment _ag-sequential _ag-temporal _ag-counsel \
       _ag-baseline _ag-alerts _ag-ml-outlier _ag-compound-risk \
       _ag-velocity _ag-rapporteur-change _ag-counsel-network \
       _ag-procedural-timeline _ag-pauta-anomaly _ag-representation-graph \
       _ag-representation-recurrence _ag-representation-windows \
       _ag-amicus-network _ag-firm-cluster \
       _ag-agenda-exposure agenda-fetch agenda-build agenda \
       cgu cgu-fetch cgu-matches cgu-corporate-links \
       tse tse-fetch tse-matches tse-fetch-expenses tse-expenses \
       tse-party-org-fetch tse-party-org tse-counterparties tse-donor-links tse-empirical-report \
       cvm cvm-fetch cvm-matches \
       rfb rfb-fetch rfb-network rfb-groups \
       datajud datajud-fetch datajud-context \
       transparencia-fetch \
       stf-portal stf-portal-fetch oab-validate \
       external-fetch fetch-all external-matches external-data \
       serving-build pipeline serve-api web-dev web-build web-typecheck \
       docker-build docker-up

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

agenda-build: agenda-fetch ## Build eventos de agenda
	$(CLI) agenda build-events

agenda: agenda-fetch agenda-build _ag-agenda-exposure ## Pipeline agenda completo

_ag-compound-risk: _ag-alerts _ag-counsel _ag-velocity _ag-rapporteur-change
	$(CLI) analytics compound-risk

analytics: ## Todos os builders analiticos (use -j6)
analytics: _ag-groups _ag-rapporteur _ag-assignment _ag-sequential _ag-temporal _ag-counsel \
           _ag-baseline _ag-alerts _ag-ml-outlier _ag-velocity _ag-rapporteur-change \
           _ag-counsel-network _ag-procedural-timeline _ag-pauta-anomaly \
           _ag-representation-graph _ag-representation-recurrence _ag-representation-windows \
           _ag-amicus-network _ag-firm-cluster _ag-agenda-exposure _ag-compound-risk

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

cgu-matches: cgu-fetch ## Build sanction matches CGU
	$(CLI) cgu build-matches

cgu-corporate-links: cgu-matches rfb-fetch ## Build vinculos corporativos de sancionados
	$(CLI) cgu build-corporate-links

cgu: cgu-fetch cgu-matches cgu-corporate-links ## Pipeline CGU completo

tse-fetch: ## Baixa doacoes eleitorais TSE
	$(CLI) tse fetch

tse-matches: tse-fetch ## Build donation matches TSE
	$(CLI) tse build-matches

tse: tse-fetch tse-matches ## Pipeline TSE completo (doacoes)

tse-party-org-fetch: ## Baixa financas de orgaos partidarios TSE
	$(CLI) tse fetch-party-org

tse-fetch-expenses: ## Baixa despesas de campanha TSE
	$(CLI) tse fetch-expenses

tse-expenses: tse-fetch-expenses ## Pipeline TSE despesas

tse-party-org: tse-party-org-fetch ## Pipeline TSE orgaos partidarios

tse-counterparties: tse-party-org-fetch ## Build contrapartes de pagamento
	$(CLI) tse build-counterparties

tse-donor-links: tse-fetch rfb-fetch ## Build vinculos corporativos de doadores
	$(CLI) tse build-donor-links

tse-empirical-report: tse-matches ## Relatorio empirico de qualidade TSE
	$(CLI) tse empirical-report

cvm-fetch: ## Baixa dados CVM
	$(CLI) cvm fetch

cvm-matches: cvm-fetch ## Build sanction matches CVM
	$(CLI) cvm build-matches

cvm: cvm-fetch cvm-matches ## Pipeline CVM completo

rfb-fetch: ## Baixa dados RFB (CNPJ)
	$(CLI) rfb fetch

rfb-groups: rfb-fetch ## Build grupos economicos
	$(CLI) rfb build-groups

rfb-network: rfb-fetch rfb-groups ## Build rede corporativa
	$(CLI) rfb build-network

rfb: rfb-fetch rfb-groups rfb-network ## Pipeline RFB completo

datajud-fetch: ## Baixa dados DataJud
	$(CLI) datajud fetch

datajud-context: datajud-fetch ## Build contexto de origem DataJud
	$(CLI) datajud build-context

datajud: datajud-fetch datajud-context ## Pipeline DataJud completo

stf-portal-fetch: ## Baixa linha do tempo portal STF
	$(CLI) stf-portal fetch --ignore-tls

stf-portal: stf-portal-fetch ## Pipeline portal STF

oab-validate: ## Valida numeros OAB
	$(CLI) oab validate --provider null

external-fetch: cgu-fetch tse-fetch cvm-fetch rfb-fetch ## Baixa todas as fontes externas (CGU/TSE/CVM/RFB)
fetch-all: scrape external-fetch tse-fetch-expenses tse-party-org-fetch stf-portal-fetch agenda-fetch ## Baixa TUDO (STF + externas + agenda)
external-matches: cgu-matches tse-matches cvm-matches rfb-network ## Build todos os matches externos
external-data: cgu tse cvm rfb ## Pipeline completo de fontes externas

# ===========================
# Serving e Pipeline — rode com: make pipeline -j6
# ===========================
serving-build: ## Materializa banco SQLite para API
	$(CLI) serving build --database-url "$(ATLAS_STF_DB_URL)"

pipeline: scrape staging curate analytics external-data evidence serving-build ## Pipeline completo (scrape -> serving)
	@echo "Pipeline completo. Rode 'make serve-api' e 'make web-dev' para subir."

# ===========================
# Servidores
# ===========================
serve-api: ## Inicia servidor API (FastAPI + Uvicorn)
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
