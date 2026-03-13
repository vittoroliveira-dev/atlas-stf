#!/usr/bin/env bash
set -euo pipefail

CLI="uv run python -m atlas_stf"
LOG="data/pipeline.log"
mkdir -p data

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

# --- CGU build-matches ---
log "Running: cgu build-matches"
$CLI cgu build-matches 2>&1 | tee -a "$LOG"
log "DONE: cgu build-matches"

# --- TSE build-matches ---
log "Running: tse build-matches"
$CLI tse build-matches 2>&1 | tee -a "$LOG"
log "DONE: tse build-matches"

# --- CVM build-matches ---
log "Running: cvm build-matches"
$CLI cvm build-matches 2>&1 | tee -a "$LOG"
log "DONE: cvm build-matches"

# --- RFB build-network ---
log "Running: rfb build-network"
$CLI rfb build-network 2>&1 | tee -a "$LOG"
log "DONE: rfb build-network"

# --- Serving ---
log "Running: serving build"
uv run atlas-stf serving build --database-url "sqlite+pysqlite:///data/serving/atlas_stf.db" 2>&1 | tee -a "$LOG"
log "DONE: serving build"

log "Build matches + serving completo!"
