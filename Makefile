.PHONY: install lint typecheck deadcode check test \
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
       cgu cgu-fetch cgu-matches tse tse-fetch tse-matches cvm cvm-fetch cvm-matches \
       rfb rfb-fetch rfb-network rfb-groups \
       datajud datajud-fetch datajud-context \
       stf-portal stf-portal-fetch oab-validate \
       external-fetch external-matches external-data \
       serving-build pipeline serve-api web-dev web-build web-typecheck \
       docker-build docker-up

ATLAS_STF_DB_URL ?= sqlite+pysqlite:///data/serving/atlas_stf.db
CLI = uv run python -m atlas_stf

install:
	uv sync

# ===========================
# Qualidade — rode com: make check -j3
# ===========================
lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

typecheck:
	uv run pyright src/

deadcode:
	uv run vulture src/ --min-confidence 80

check: lint typecheck deadcode

# ===========================
# Testes (paralelos via pytest-xdist -n auto)
# ===========================
test:
	uv run pytest

# ===========================
# Data Prep e Auditoria
# ===========================
manifest-raw:
	$(CLI) manifest raw

profile-staging:
	$(CLI) profile staging

validate-staging:
	$(CLI) validate staging

audit-stage:
	$(CLI) audit stage

audit-curated:
	$(CLI) audit curated

audit-analytics:
	$(CLI) audit analytics

audit: audit-stage audit-curated audit-analytics

# ===========================
# Staging e Curated
# ===========================
staging:
	$(CLI) stage

curate:
	$(CLI) curate all

curate-process:
	$(CLI) curate process

curate-decision-event:
	$(CLI) curate decision-event

curate-subject:
	$(CLI) curate subject

curate-party:
	$(CLI) curate party

curate-counsel:
	$(CLI) curate counsel

curate-entity-identifier:
	$(CLI) curate entity-identifier

curate-entity-reconciliation:
	$(CLI) curate entity-reconciliation

curate-representation:
	$(CLI) curate representation

curate-links:
	$(CLI) curate links

# ===========================
# Analytics — dependências declaradas, rode com: make analytics -j6
#
# Grafo de dependências:
#   groups ──→ baseline ──→ alerts ──┬→ ml-outlier
#                                    └→ compound-risk (+ counsel, velocity, rapporteur-change)
#   minister-flow: materializado pelo serving builder (não precisa de passo separado)
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

agenda-fetch:
	$(CLI) agenda fetch

agenda-build: agenda-fetch
	$(CLI) agenda build-events

agenda: agenda-fetch agenda-build _ag-agenda-exposure

_ag-compound-risk: _ag-alerts _ag-counsel _ag-velocity _ag-rapporteur-change
	$(CLI) analytics compound-risk

analytics: _ag-groups _ag-rapporteur _ag-assignment _ag-sequential _ag-temporal _ag-counsel \
           _ag-baseline _ag-alerts _ag-ml-outlier _ag-velocity _ag-rapporteur-change \
           _ag-counsel-network _ag-procedural-timeline _ag-pauta-anomaly \
           _ag-representation-graph _ag-representation-recurrence _ag-representation-windows \
           _ag-amicus-network _ag-firm-cluster _ag-agenda-exposure _ag-compound-risk

evidence:
	$(CLI) evidence build-all

scrape:
	ATLAS_STF_SCRAPER_IGNORE_HTTPS_ERRORS=true $(CLI) scrape decisoes
	ATLAS_STF_SCRAPER_IGNORE_HTTPS_ERRORS=true $(CLI) scrape acordaos

# ===========================
# Fontes externas — rode com: make external-data -j4
# ===========================
cgu-fetch:
	$(CLI) cgu fetch

cgu-matches: cgu-fetch
	$(CLI) cgu build-matches

cgu: cgu-fetch cgu-matches

tse-fetch:
	$(CLI) tse fetch

tse-matches: tse-fetch
	$(CLI) tse build-matches

tse: tse-fetch tse-matches

cvm-fetch:
	$(CLI) cvm fetch

cvm-matches: cvm-fetch
	$(CLI) cvm build-matches

cvm: cvm-fetch cvm-matches

rfb-fetch:
	$(CLI) rfb fetch

rfb-groups: rfb-fetch
	$(CLI) rfb build-groups

rfb-network: rfb-fetch rfb-groups
	$(CLI) rfb build-network

rfb: rfb-fetch rfb-groups rfb-network

datajud-fetch:
	$(CLI) datajud fetch

datajud-context: datajud-fetch
	$(CLI) datajud build-context

datajud: datajud-fetch datajud-context

stf-portal-fetch:
	$(CLI) stf-portal fetch

stf-portal: stf-portal-fetch

oab-validate:
	$(CLI) oab validate --provider null

external-fetch: cgu-fetch tse-fetch cvm-fetch rfb-fetch
external-matches: cgu-matches tse-matches cvm-matches rfb-network
external-data: cgu tse cvm rfb

# ===========================
# Serving e Pipeline — rode com: make pipeline -j6
# ===========================
serving-build:
	uv run atlas-stf serving build --database-url "$(ATLAS_STF_DB_URL)"

pipeline: serving-build
	@echo "Pipeline completo. Rode 'make serve-api' e 'make web-dev' para subir."

# Ordem do pipeline via dependências:
#   staging → curate → analytics (paralelo interno) + external-data (paralelo)
#   → evidence → serving-build
staging: | scrape
curate: | staging
analytics: | curate
external-data: | curate
evidence: | analytics external-data
serving-build: | evidence

# ===========================
# Servidores
# ===========================
serve-api:
	ATLAS_STF_DATABASE_URL="$(ATLAS_STF_DB_URL)" uv run atlas-stf api serve --host 127.0.0.1 --port 8000

web-dev:
	cd web && ATLAS_STF_API_BASE_URL="http://127.0.0.1:8000" npm run dev

web-build:
	cd web && npm run build

web-typecheck:
	cd web && npm run typecheck

docker-build:
	docker compose build

docker-up:
	docker compose up --build
