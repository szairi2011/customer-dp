#!/usr/bin/env bash
# Valide le projet sur le poste : environnement uv, lint, tests unitaires, puis pipeline local complet.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

SKIP_PIPELINE=false
while (($#)); do
  case "$1" in
    --skip-pipeline) SKIP_PIPELINE=true; shift ;;
    -h|--help) echo "Usage : scripts/test_local.sh [--skip-pipeline]"; exit 0 ;;
    *) die "option inconnue : $1" ;;
  esac
done
need_cmd uv "installez uv : https://docs.astral.sh/uv/"
cd "$PROJECT_ROOT"

log "1/4 uv sync --locked (Python 3.8)"; uv sync --locked
log "2/4 ruff"; uv run ruff check .
log "3/4 pytest"; uv run pytest
if $SKIP_PIPELINE; then log "pipeline local ignoré"; exit 0; fi
log "4/4 pipeline local (sorties dans .local/output)"
rm -rf "${PROJECT_ROOT:?}/.local/output"   # sorties locales jetables uniquement
"${PROJECT_ROOT}/scripts/run.sh" --env local --layer all --batch-date 2026-03-10
"${PROJECT_ROOT}/scripts/run.sh" --env local --layer verify --expect silver_rows=9 rejected_rows=5 gold_rows=3
log "validation locale réussie"
