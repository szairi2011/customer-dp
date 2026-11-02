#!/usr/bin/env bash
# Construit les artefacts versionnés dans dist/<version>/ :
#   - customer_dp-<version>-py3-none-any.whl   artefact publié (traçabilité, installation éventuelle)
#   - pyenv-py38-<version>.tar.gz              code + dépendances hors cluster, extrait par YARN (--archives)
#   - entrypoint.py, manifest.yaml
# pyspark et py4j sont fournis par le cluster : ils ne sont jamais embarqués.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

usage() { echo "Usage : scripts/package.sh [--version X.Y.Z]   (défaut : version de pyproject.toml)"; }
VERSION=""
while (($#)); do
  case "$1" in
    --version) VERSION="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "option inconnue : $1" ;;
  esac
done

DECLARED="$(read_version)"
VERSION="${VERSION:-$DECLARED}"
require_version "$VERSION"
[[ "$VERSION" == "$DECLARED" ]] || die "la version demandée ($VERSION) diffère de pyproject.toml ($DECLARED) : mettez à jour pyproject.toml"
need_cmd uv "installez uv : https://docs.astral.sh/uv/"
need_cmd tar "utilitaire requis"
need_cmd sha256sum "utilitaire requis"

cd "$PROJECT_ROOT"
OUT="dist/${VERSION}"
WHEEL="$(wheel_name "$VERSION")"
ARCHIVE="$(env_archive_name "$VERSION")"
rm -rf "$OUT"; mkdir -p "$OUT"

log "1/4 wheel"
uv build --wheel --out-dir "$OUT" --quiet
[[ -f "${OUT}/${WHEEL}" ]] || die "wheel attendue absente : ${OUT}/${WHEEL}"

log "2/4 environnement Python 3.8 (wheel + éventuelles dépendances hors pyspark/py4j)"
STAGE="$(mktemp -d)"; trap 'rm -rf "$STAGE"' EXIT
uv export --no-dev --no-emit-project --locked --no-hashes --quiet \
  | grep -viE '^(pyspark|py4j)==' > "${STAGE}/requirements.txt" || true
uv pip install --quiet --python 3.8 --target "${STAGE}/pyenv" --no-deps "${OUT}/${WHEEL}"
if [[ -s "${STAGE}/requirements.txt" ]]; then
  uv pip install --quiet --python 3.8 --target "${STAGE}/pyenv" --no-deps -r "${STAGE}/requirements.txt"
fi
find "${STAGE}/pyenv" -name '__pycache__' -type d -prune -exec rm -rf {} +
rm -f "${STAGE}/pyenv/.lock"
# Contenu à la racine de l'archive : YARN l'extrait dans le dossier alias « pyenv ».
tar -czf "${OUT}/${ARCHIVE}" -C "${STAGE}/pyenv" .

log "3/4 fichiers d'exécution"
cp scripts/entrypoint.py "${OUT}/entrypoint.py"

log "4/4 manifeste et contrôles"
CONTENT="$(tar -tzf "${OUT}/${ARCHIVE}")"
grep -q 'customer_dp/__init__.py' <<<"$CONTENT" || die "customer_dp absent de ${ARCHIVE}"
if grep -qE '^\./(pyspark|py4j)/' <<<"$CONTENT"; then die "pyspark/py4j ne doivent pas être embarqués"; fi
{
  echo "name: customer-dp"
  echo "version: ${VERSION}"
  echo "built_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit: $(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "python: \"3.8\""
  echo "spark: \"3.3.2\""
  echo "constraints: \"pyspark et py4j fournis par le cluster; Python 3.8 sur driver et executors\""
  echo "artifacts:"
  for f in "$WHEEL" "$ARCHIVE" entrypoint.py; do
    echo "  - {file: ${f}, sha256: $(sha256sum "${OUT}/${f}" | cut -d' ' -f1)}"
  done
} > "${OUT}/manifest.yaml"
ls -l "$OUT"
log "artefacts prêts : ${OUT}"
