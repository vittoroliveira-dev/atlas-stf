#!/usr/bin/env bash
# ===========================================================================
# release.sh — Publish-only: taguear e publicar uma release já preparada.
#
# Pré-requisitos (executados pelo fluxo de preparação local):
#   - Working tree limpa
#   - Commit HEAD contém toda a release (versão, docs, CHANGELOG)
#   - CHANGELOG.md contém entrada para a versão
#   - pyproject.toml e package.json na versão correta
#   - build/release-notes-<versao>.md preenchido
#
# Uso:
#   ./scripts/release.sh <versao>
#   ./scripts/release.sh <versao> --dry-run
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
    echo -e "${BOLD}Atlas STF — Release Publish${NC}"
    echo ""
    echo "Uso: $0 <versao> [--dry-run]"
    echo ""
    echo "Publica uma release já preparada (commit pronto, docs atualizados)."
    echo "A preparação é feita pelo fluxo local antes de chamar este script."
    exit 1
fi

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    error "Formato invalido: '$VERSION'. Use semver X.Y.Z"
fi

TAG="v${VERSION}"
REPO_OWNER="vittoroliveira-dev"
REPO_NAME="atlas-stf"
REPO_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if $DRY_RUN; then
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  MODO DRY-RUN — nenhuma acao destrutiva${NC}"
    echo -e "${YELLOW}========================================${NC}"
fi

# ===========================================================================
step "1/4 — Pre-condicoes de publicacao"
# ===========================================================================

BRANCH="$(git branch --show-current)"
[[ "$BRANCH" != "main" ]] && error "Releases devem ser a partir de 'main'. Atual: '${BRANCH}'"
info "Branch: main"

if [[ -n "$(git status --porcelain)" ]]; then
    error "Working tree suja. A preparação deve ter commitado tudo antes de publicar."
fi
info "Working tree limpa"

CURRENT_PYPROJECT=$(grep '^version = ' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
[[ "$CURRENT_PYPROJECT" != "$VERSION" ]] && error "pyproject.toml: '${CURRENT_PYPROJECT}', esperado '${VERSION}'."
info "pyproject.toml: ${VERSION}"

CURRENT_PKG=$(node -p "require('./web/package.json').version" 2>/dev/null || echo "unknown")
[[ "$CURRENT_PKG" != "$VERSION" ]] && error "package.json: '${CURRENT_PKG}', esperado '${VERSION}'."
info "package.json: ${VERSION}"

grep -q "## \[${VERSION}\]" CHANGELOG.md || error "CHANGELOG.md nao contem entrada para [${VERSION}]."
info "CHANGELOG.md: [${VERSION}]"

RELEASE_NOTES_FILE="build/release-notes-${VERSION}.md"
[[ ! -f "$RELEASE_NOTES_FILE" ]] && error "Release notes nao encontrado: ${RELEASE_NOTES_FILE}"
[[ ! -s "$RELEASE_NOTES_FILE" ]] && error "Release notes vazio: ${RELEASE_NOTES_FILE}"
info "Release notes: ${RELEASE_NOTES_FILE}"

git tag -l "$TAG" | grep -q "$TAG" && error "Tag '${TAG}' ja existe."
info "Tag ${TAG} disponivel"

gh auth status &>/dev/null || error "GitHub CLI nao autenticado. Execute 'gh auth login'."
info "GitHub CLI autenticado"

PREV_TAG=$(git tag -l 'v*' --sort=-v:refname | head -1)
PREV_VERSION=""
if [[ -n "$PREV_TAG" ]]; then
    PREV_VERSION="${PREV_TAG#v}"
    info "Versao anterior: ${PREV_TAG}"
fi

# ===========================================================================
step "2/4 — Confirmacao"
# ===========================================================================

# Ler release notes já preparadas
RELEASE_BODY=$(cat "$RELEASE_NOTES_FILE")

# Adicionar links de instalação e diff
INSTALL_BLOCK="**Instalacao:**
\`\`\`bash
pip install ${REPO_URL}/releases/download/${TAG}/atlas_stf-${VERSION}-py3-none-any.whl
docker pull ghcr.io/${REPO_OWNER}/${REPO_NAME}:${TAG}
\`\`\`"

if [[ -n "$PREV_VERSION" ]]; then
    DIFF_LINK="**Diff completo:** [v${PREV_VERSION}...v${VERSION}](${REPO_URL}/compare/v${PREV_VERSION}...v${VERSION})"
    RELEASE_BODY="${RELEASE_BODY}

---

${DIFF_LINK}

${INSTALL_BLOCK}"
else
    RELEASE_BODY="${RELEASE_BODY}

---

${INSTALL_BLOCK}"
fi

echo -e "${DIM}--- Previa das notas ---${NC}"
echo "$RELEASE_BODY" | head -40
NOTES_LINES=$(echo "$RELEASE_BODY" | wc -l)
[[ "$NOTES_LINES" -gt 40 ]] && echo -e "${DIM}  ... (+$((NOTES_LINES - 40)) linhas)${NC}"
echo -e "${DIM}--- Fim da previa ---${NC}"

echo ""
echo -e "${BOLD}+------------------------------------------+${NC}"
echo -e "${BOLD}|         RESUMO DA RELEASE                |${NC}"
echo -e "${BOLD}+------------------------------------------+${NC}"
echo -e "${BOLD}|${NC}  Versao:     ${GREEN}${VERSION}${NC}"
echo -e "${BOLD}|${NC}  Tag:        ${GREEN}${TAG}${NC}"
echo -e "${BOLD}|${NC}  Branch:     main"
[[ -n "$PREV_VERSION" ]] && echo -e "${BOLD}|${NC}  Anterior:   v${PREV_VERSION}"
echo -e "${BOLD}|${NC}  Commit:     $(git rev-parse --short HEAD)"
echo -e "${BOLD}+------------------------------------------+${NC}"
echo ""

if $DRY_RUN; then
    info "Dry-run concluido. Nenhuma acao executada."
    exit 0
fi

read -rp "$(echo -e "${YELLOW}Confirmar release ${TAG}? [s/N] ${NC}")" CONFIRM
if [[ "$CONFIRM" != "s" && "$CONFIRM" != "S" ]]; then
    warn "Cancelada."
    exit 1
fi

# ===========================================================================
step "3/4 — Publicacao"
# ===========================================================================

info "Tag ${TAG}..."
git tag "$TAG"

info "Push tag..."
git push origin "$TAG"
info "Push concluido"

info "GitHub Release..."
gh release create "$TAG" \
    --title "Atlas STF ${TAG}" \
    --notes "$RELEASE_BODY"
info "Release criada (CI vai anexar wheel + Docker automaticamente)"

# ===========================================================================
step "4/4 — Monitoramento CI"
# ===========================================================================

sleep 3
RUN_ID=$(gh run list --workflow=ci.yml --limit=1 --json databaseId --jq '.[0].databaseId' 2>/dev/null || echo "")
if [[ -n "$RUN_ID" ]]; then
    RUN_URL="${REPO_URL}/actions/runs/${RUN_ID}"
    echo -e "  Workflow: ${RUN_URL}"
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

echo ""
echo -e "${GREEN}  RELEASE ${TAG} CONCLUIDA${NC}"
echo "  Commit: $(git rev-parse --short HEAD) | Tag: ${TAG}"
echo "  Release: ${REPO_URL}/releases/tag/${TAG}"
echo "  Docker:  ghcr.io/${REPO_OWNER}/${REPO_NAME}:${TAG}"
[[ -n "$PREV_VERSION" ]] && echo "  Diff:    ${REPO_URL}/compare/v${PREV_VERSION}...${TAG}"
echo ""
