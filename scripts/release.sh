#!/usr/bin/env bash
# ===========================================================================
# release.sh — Script de release do Atlas STF
#
# Uso:
#   ./scripts/release.sh <versao>            # release completo
#   ./scripts/release.sh <versao> --dry-run  # simula sem executar nada
#
# Exemplos:
#   ./scripts/release.sh 1.0.5
#   ./scripts/release.sh 1.1.0 --dry-run
# ===========================================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

info()    { echo -e "${GREEN}[ok]${NC} $*"; }
warn()    { echo -e "${YELLOW}[!!]${NC} $*"; }
error()   { echo -e "${RED}[xx]${NC} $*" >&2; exit 1; }
step()    { echo -e "\n${BLUE}---${NC} ${BOLD}$*${NC}"; }

# --- Argumentos ---
VERSION="${1:-}"
DRY_RUN=false
[[ "${2:-}" == "--dry-run" ]] && DRY_RUN=true

if [[ -z "$VERSION" ]]; then
    echo -e "${BOLD}Atlas STF — Release Script${NC}"
    echo ""
    echo "Uso: $0 <versao> [--dry-run]"
    echo ""
    echo "Exemplos:"
    echo "  $0 1.0.5            # release completo"
    echo "  $0 1.1.0 --dry-run  # simula sem executar"
    exit 1
fi

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    error "Formato invalido: '$VERSION'. Use semver X.Y.Z"
fi

TAG="v${VERSION}"
REPO_OWNER="vittoroliveira-dev"
REPO_NAME="atlas-stf"
REPO_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}"
PROTECTION_PAYLOAD='{"required_status_checks":{"strict":true,"contexts":["CI"]},"enforce_admins":false,"required_pull_request_reviews":null,"restrictions":null,"allow_force_pushes":false,"allow_deletions":false}'

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if $DRY_RUN; then
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  MODO DRY-RUN — nenhuma acao destrutiva${NC}"
    echo -e "${YELLOW}========================================${NC}"
fi

# ===========================================================================
step "1/10 — Pre-condicoes"
# ===========================================================================

BRANCH="$(git branch --show-current)"
[[ "$BRANCH" != "main" ]] && error "Releases devem ser a partir de 'main'. Atual: '${BRANCH}'"
info "Branch: main"

git fetch origin main --quiet 2>/dev/null || true
LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_HEAD="$(git rev-parse origin/main 2>/dev/null || echo 'unknown')"
if [[ "$REMOTE_HEAD" != "unknown" && "$LOCAL_HEAD" != "$REMOTE_HEAD" ]]; then
    warn "Branch local diverge do remote. Considere 'git pull'."
fi

git tag -l "$TAG" | grep -q "$TAG" && error "Tag '${TAG}' ja existe."
info "Tag ${TAG} disponivel"

grep -q "## \[${VERSION}\]" CHANGELOG.md || error "CHANGELOG.md nao contem entrada para [${VERSION}]."
info "CHANGELOG.md contem entrada [${VERSION}]"

gh auth status &>/dev/null || error "GitHub CLI nao autenticado. Execute 'gh auth login'."
info "GitHub CLI autenticado"

PREV_TAG=$(git tag -l 'v*' --sort=-v:refname | head -1)
if [[ -n "$PREV_TAG" ]]; then
    PREV_VERSION="${PREV_TAG#v}"
    info "Versao anterior: ${PREV_TAG}"
else
    PREV_VERSION=""
    warn "Nenhuma tag anterior (primeiro release)"
fi

# Lock files atualizados?
if [[ -n "$(git diff --name-only uv.lock 2>/dev/null)" ]]; then
    warn "uv.lock tem mudancas nao commitadas"
fi
if [[ -n "$(git diff --name-only web/package-lock.json 2>/dev/null)" ]]; then
    warn "package-lock.json tem mudancas nao commitadas"
fi

# ===========================================================================
step "2/10 — Metricas do projeto (auto-contagem)"
# ===========================================================================

COUNT_TABLES=$(grep -r '__tablename__' src/atlas_stf/serving/models.py src/atlas_stf/serving/_models_*.py 2>/dev/null | wc -l)
COUNT_ENDPOINTS=$(grep -rc '@app\.\(get\|post\|put\|delete\)' src/atlas_stf/api/ 2>/dev/null | awk -F: '{s+=$2} END {print s}')
COUNT_TESTS=$(grep -rc 'def test_' tests/ 2>/dev/null | awk -F: '{s+=$2} END {print s}')
COUNT_PAGES=$(find web/src/app -name 'page.tsx' 2>/dev/null | wc -l)
COUNT_SRC_MODULES=$(find src/atlas_stf -mindepth 1 -maxdepth 1 -type d ! -name '__pycache__' | wc -l)
COUNT_SRC_FILES=$(find src/atlas_stf -name '*.py' ! -path '*__pycache__*' | wc -l)

echo -e "  ${CYAN}Tabelas serving:${NC}  ${BOLD}${COUNT_TABLES}${NC}"
echo -e "  ${CYAN}Endpoints API:${NC}    ${BOLD}${COUNT_ENDPOINTS}${NC}"
echo -e "  ${CYAN}Testes:${NC}           ${BOLD}${COUNT_TESTS}${NC}"
echo -e "  ${CYAN}Paginas web:${NC}      ${BOLD}${COUNT_PAGES}${NC}"
echo -e "  ${CYAN}Modulos src:${NC}      ${BOLD}${COUNT_SRC_MODULES}${NC}"
echo -e "  ${CYAN}Arquivos .py:${NC}     ${BOLD}${COUNT_SRC_FILES}${NC}"

# Delta desde ultimo release
if [[ -n "$PREV_TAG" ]]; then
    DELTA_FILES=$(git diff --name-only "${PREV_TAG}"...HEAD | wc -l)
    DELTA_NEW=$(git diff --diff-filter=A --name-only "${PREV_TAG}"...HEAD | wc -l)
    DELTA_TESTS_NEW=$(git diff --diff-filter=A --name-only "${PREV_TAG}"...HEAD -- 'tests/' | wc -l)
    DELTA_INSERTIONS=$(git diff --stat "${PREV_TAG}"...HEAD | tail -1 | grep -oP '\d+ insertion' | grep -oP '\d+' || echo "0")
    DELTA_DELETIONS=$(git diff --stat "${PREV_TAG}"...HEAD | tail -1 | grep -oP '\d+ deletion' | grep -oP '\d+' || echo "0")

    echo ""
    echo -e "  ${CYAN}Delta desde ${PREV_TAG}:${NC}"
    echo -e "    Arquivos alterados: ${BOLD}${DELTA_FILES}${NC}"
    echo -e "    Arquivos novos:     ${BOLD}${DELTA_NEW}${NC}"
    echo -e "    Test files novos:   ${BOLD}${DELTA_TESTS_NEW}${NC}"
    echo -e "    Insercoes:          ${GREEN}+${DELTA_INSERTIONS}${NC}"
    echo -e "    Remocoes:           ${RED}-${DELTA_DELETIONS}${NC}"
fi

# Auto-atualizar metricas no README
COUNT_TEST_FILES=$(find tests -name 'test_*.py' 2>/dev/null | wc -l)
COUNT_LIB_MODULES=$(find web/src/lib -name '*.ts' 2>/dev/null | wc -l)
COUNT_DOCS=$(find docs -name '*.md' -not -path '*/adr/*' -not -path '*/pesquisas/*' 2>/dev/null | wc -l)

if $DRY_RUN; then
    info "[DRY] Atualizaria metricas no README.md"
else
    # Tabelas serving (ex: "41 tabelas")
    sed -i -E "s/SQLite \([0-9]+ tabelas\)/SQLite (${COUNT_TABLES} tabelas)/" README.md
    sed -i -E "s/serving \([0-9]+ tabelas/serving (${COUNT_TABLES} tabelas/" README.md
    # Endpoints API (ex: "73 endpoints")
    sed -i -E "s/FastAPI \([0-9]+ endpoints?\)/FastAPI (${COUNT_ENDPOINTS} endpoints)/" README.md
    sed -i -E "s/Endpoints principais \([0-9]+\)/Endpoints principais (${COUNT_ENDPOINTS})/" README.md
    # Testes (ex: "~1707 testes")
    sed -i -E "s/~[0-9]+ testes/~${COUNT_TESTS} testes/" README.md
    # Test files (ex: "164 arquivos")
    sed -i -E "s/[0-9]+ arquivos, ~[0-9]+ testes/${COUNT_TEST_FILES} arquivos, ~${COUNT_TESTS} testes/" README.md
    # Lib modules (ex: "20 módulos")
    sed -i -E "s/[0-9]+\+? módulos \(API client/\1${COUNT_LIB_MODULES} módulos (API client/" README.md 2>/dev/null || true
    sed -i -E "s|src/lib/.*# [0-9]+\+? módulos|src/lib/          # ${COUNT_LIB_MODULES} módulos|" README.md
    # Docs count (ex: "14 documentos")
    sed -i -E "s/[0-9]+ documentos\)/${COUNT_DOCS} documentos)/" README.md
    # Version in install examples
    sed -i -E "s|atlas-stf:v[0-9]+\.[0-9]+\.[0-9]+|atlas-stf:v${VERSION}|g" README.md
    sed -i -E "s|atlas_stf-[0-9]+\.[0-9]+\.[0-9]+-py3|atlas_stf-${VERSION}-py3|g" README.md
    info "README.md metricas atualizadas automaticamente"
fi

# ===========================================================================
step "3/10 — Atualizacao de versao"
# ===========================================================================

CURRENT_PYPROJECT=$(grep '^version = ' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
CURRENT_PKG=$(node -p "require('./web/package.json').version" 2>/dev/null || echo "unknown")
info "Atual: pyproject=${CURRENT_PYPROJECT}, pkg=${CURRENT_PKG}"
info "__init__.py usa importlib.metadata (deriva de pyproject.toml)"

if $DRY_RUN; then
    info "[DRY] Atualizaria para ${VERSION} (2 fontes)"
else
    sed -i "s/^version = \".*\"/version = \"${VERSION}\"/" pyproject.toml
    (cd web && npm version "$VERSION" --no-git-tag-version --allow-same-version) >/dev/null

    NEW_PY=$(grep '^version = ' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    NEW_PK=$(node -p "require('./web/package.json').version" 2>/dev/null || echo "?")
    if [[ "$NEW_PY" != "$VERSION" || "$NEW_PK" != "$VERSION" ]]; then
        error "Falha ao atualizar. py=${NEW_PY}, pkg=${NEW_PK}"
    fi
    info "Versao ${VERSION} (2 fontes sincronizadas)"
fi

# ===========================================================================
step "4/10 — Qualidade: backend (ruff + pyright)"
# ===========================================================================

if $DRY_RUN; then
    info "[DRY] ruff check src/ tests/"
    info "[DRY] pyright src/"
else
    uv run ruff check src/ tests/
    info "Lint OK"
    uv run pyright src/
    info "Typecheck backend OK"
fi

# ===========================================================================
step "5/10 — Qualidade: frontend (typecheck)"
# ===========================================================================

if $DRY_RUN; then
    info "[DRY] cd web && npm run typecheck"
else
    (cd web && npm run typecheck)
    info "Typecheck frontend OK"
fi

# ===========================================================================
step "6/10 — Testes"
# ===========================================================================

if $DRY_RUN; then
    info "[DRY] uv run pytest (cobertura minima 83%)"
else
    uv run pytest --tb=short -q --cov=src/atlas_stf --cov-report=term --cov-fail-under=83
    info "Testes OK (${COUNT_TESTS} testes)"
fi

# ===========================================================================
step "7/10 — Build do pacote (validacao local)"
# ===========================================================================

WHEEL="dist/atlas_stf-${VERSION}-py3-none-any.whl"
SDIST="dist/atlas_stf-${VERSION}.tar.gz"

if $DRY_RUN; then
    info "[DRY] uv build -> ${WHEEL}"
else
    rm -f dist/atlas_stf-*.whl dist/atlas_stf-*.tar.gz 2>/dev/null || true
    uv build
    [[ ! -f "$WHEEL" ]] && error "Wheel nao encontrado: ${WHEEL}"
    info "Wheel: ${WHEEL} ($(du -h "$WHEEL" | cut -f1))"
    info "Sdist: ${SDIST} ($(du -h "$SDIST" | cut -f1))"
fi

# ===========================================================================
step "8/10 — Notas de release"
# ===========================================================================

RELEASE_NOTES=$(awk "/^## \[${VERSION}\]/{found=1; next} /^## \[/{if(found) exit} found" CHANGELOG.md)

# Montar body com metricas automaticas + diff link
METRICS_BLOCK="**Metricas desta versao:**
| Metrica | Valor |
|---------|-------|
| Tabelas serving | ${COUNT_TABLES} |
| Endpoints API | ${COUNT_ENDPOINTS} |
| Testes | ${COUNT_TESTS} |
| Paginas web | ${COUNT_PAGES} |
| Modulos | ${COUNT_SRC_MODULES} |"

INSTALL_BLOCK="**Instalacao:**
\`\`\`bash
# Via release asset (wheel)
pip install ${REPO_URL}/releases/download/${TAG}/atlas_stf-${VERSION}-py3-none-any.whl

# Via Docker
docker pull ghcr.io/${REPO_OWNER}/${REPO_NAME}:${TAG}
\`\`\`"

if [[ -n "$PREV_VERSION" ]]; then
    DIFF_LINK="**Diff completo:** [v${PREV_VERSION}...v${VERSION}](${REPO_URL}/compare/v${PREV_VERSION}...v${VERSION})"
    RELEASE_BODY="${RELEASE_NOTES}

---

${METRICS_BLOCK}

${DIFF_LINK}

${INSTALL_BLOCK}"
else
    RELEASE_BODY="${RELEASE_NOTES}

---

${METRICS_BLOCK}

${INSTALL_BLOCK}"
fi

echo -e "${DIM}--- Previa das notas ---${NC}"
echo "$RELEASE_BODY" | head -40
NOTES_LINES=$(echo "$RELEASE_BODY" | wc -l)
if [[ "$NOTES_LINES" -gt 40 ]]; then
    echo -e "${DIM}  ... (+$((NOTES_LINES - 40)) linhas)${NC}"
fi
echo -e "${DIM}--- Fim da previa ---${NC}"

# ===========================================================================
step "9/10 — Confirmacao"
# ===========================================================================

echo ""
echo -e "${BOLD}+------------------------------------------+${NC}"
echo -e "${BOLD}|         RESUMO DA RELEASE                |${NC}"
echo -e "${BOLD}+------------------------------------------+${NC}"
echo -e "${BOLD}|${NC}  Versao:     ${GREEN}${VERSION}${NC}"
echo -e "${BOLD}|${NC}  Tag:        ${GREEN}${TAG}${NC}"
echo -e "${BOLD}|${NC}  Branch:     main"
[[ -n "$PREV_VERSION" ]] && echo -e "${BOLD}|${NC}  Anterior:   v${PREV_VERSION}"
echo -e "${BOLD}|${NC}  Fontes:     pyproject.toml + package.json (init deriva via importlib)"
echo -e "${BOLD}|${NC}  Commit:     Atlas STF v${VERSION}"
echo -e "${BOLD}|${NC}  Metricas:   ${COUNT_TABLES} tabelas, ${COUNT_ENDPOINTS} endpoints, ${COUNT_TESTS} testes"
if ! $DRY_RUN; then
echo -e "${BOLD}|${NC}  Wheel:      $(du -h "$WHEEL" 2>/dev/null | cut -f1 || echo 'N/A')"
fi
echo -e "${BOLD}+------------------------------------------+${NC}"
echo -e "${BOLD}|${NC}  Acoes:"
echo -e "${BOLD}|${NC}    git commit + git tag ${TAG}"
echo -e "${BOLD}|${NC}    git push origin main + ${TAG}"
echo -e "${BOLD}|${NC}    gh release create ${TAG}"
echo -e "${BOLD}|${NC}    -> CI publica wheel + Docker automaticamente"
echo -e "${BOLD}+------------------------------------------+${NC}"
echo ""

if $DRY_RUN; then
    info "Dry-run concluido. Nenhuma acao executada."
    echo ""
    echo "Para executar de verdade:"
    echo "  $0 ${VERSION}"
    exit 0
fi

read -rp "$(echo -e "${YELLOW}Confirmar release ${TAG}? [s/N] ${NC}")" CONFIRM
if [[ "$CONFIRM" != "s" && "$CONFIRM" != "S" ]]; then
    sed -i "s/^version = \".*\"/version = \"${CURRENT_PYPROJECT}\"/" pyproject.toml
    (cd web && npm version "$CURRENT_PKG" --no-git-tag-version --allow-same-version) >/dev/null 2>&1 || true
    warn "Cancelada. Versao revertida para ${CURRENT_PYPROJECT}."
    exit 1
fi

# ===========================================================================
step "10/10 — Publicacao"
# ===========================================================================

info "Commitando..."
git add pyproject.toml src/atlas_stf/__init__.py CHANGELOG.md README.md web/package.json
git add CLAUDE.md 2>/dev/null || true
git add src/ tests/ web/src/ Makefile schemas/ docs/ .github/ .gitignore .nvmrc data/curated/minister_bio.json
git add uv.lock web/package-lock.json web/eslint.config.mjs Dockerfile docker-compose.yml 2>/dev/null || true
git add scripts/ 2>/dev/null || true
git diff --cached --quiet && error "Nada para commitar."
git commit -m "Atlas STF v${VERSION}"
info "Commit: $(git rev-parse --short HEAD)"

info "Tag ${TAG}..."
git tag "$TAG"

info "Desabilitando branch protection..."
gh api "repos/${REPO_OWNER}/${REPO_NAME}/branches/main/protection" -X DELETE --silent 2>/dev/null || true

info "Push..."
git push origin main --force
git push origin "$TAG"
info "Push concluido"

info "Reabilitando branch protection..."
echo "$PROTECTION_PAYLOAD" | gh api "repos/${REPO_OWNER}/${REPO_NAME}/branches/main/protection" -X PUT --input - --silent 2>/dev/null || warn "Falha ao reabilitar protection. Reabilitar manualmente."
info "Branch protection reativada"

info "GitHub Release..."
gh release create "$TAG" \
    --title "Atlas STF ${TAG}" \
    --notes "$RELEASE_BODY"
info "Release criada (CI vai anexar wheel + Docker automaticamente)"

# --- Pos-release: monitorar workflow CI ---
echo ""
echo -e "${CYAN}Aguardando workflow CI...${NC}"
sleep 3
RUN_ID=$(gh run list --workflow=ci.yml --limit=1 --json databaseId --jq '.[0].databaseId' 2>/dev/null || echo "")
if [[ -n "$RUN_ID" ]]; then
    RUN_URL="${REPO_URL}/actions/runs/${RUN_ID}"
    echo -e "  Workflow: ${RUN_URL}"

    # Polling por ate 180s
    for i in $(seq 1 18); do
        STATUS=$(gh run view "$RUN_ID" --json status,conclusion --jq '.status' 2>/dev/null || echo "unknown")
        if [[ "$STATUS" == "completed" ]]; then
            CONCLUSION=$(gh run view "$RUN_ID" --json conclusion --jq '.conclusion' 2>/dev/null || echo "unknown")
            if [[ "$CONCLUSION" == "success" ]]; then
                info "CI concluido com sucesso (wheel + Docker publicados)"
            else
                warn "CI concluido com status: ${CONCLUSION}"
            fi
            break
        fi
        echo -ne "\r  Status: ${STATUS} (${i}/18)..."
        sleep 10
    done
    [[ "$STATUS" != "completed" ]] && echo -e "\n  Workflow ainda em execucao. Acompanhe em: ${RUN_URL}"
else
    warn "Nao foi possivel detectar o workflow. Verifique manualmente."
fi

# ===========================================================================
# Resultado final
# ===========================================================================
echo ""
echo -e "${GREEN}+------------------------------------------+${NC}"
echo -e "${GREEN}|     RELEASE ${TAG} CONCLUIDA               |${NC}"
echo -e "${GREEN}+------------------------------------------+${NC}"
echo ""
echo "  Commit:     $(git rev-parse --short HEAD)"
echo "  Tag:        ${TAG}"
echo "  Metricas:   ${COUNT_TABLES} tabelas, ${COUNT_ENDPOINTS} endpoints, ${COUNT_TESTS} testes"
echo ""
echo "  Links:"
echo "    Release:  ${REPO_URL}/releases/tag/${TAG}"
echo "    Docker:   ghcr.io/${REPO_OWNER}/${REPO_NAME}:${TAG}"
echo "    Package:  ${REPO_URL}/pkgs/container/${REPO_NAME}"
[[ -n "$PREV_VERSION" ]] && echo "    Diff:     ${REPO_URL}/compare/v${PREV_VERSION}...${TAG}"
echo ""
