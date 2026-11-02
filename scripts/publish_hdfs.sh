#!/usr/bin/env bash
# Publie dist/<version>/ vers HDFS : <HDFS_ARTIFACT_ROOT>/customer-dp/<version>/
# La publication est atomique (dossier temporaire puis renommage) et refuse d'écraser une version existante.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

usage() {
  cat <<USAGE
Usage : scripts/publish_hdfs.sh [--env PROFIL] --version X.Y.Z [--force] [--dry-run] [--set CLE=VALEUR]
  --force    remplace une version déjà publiée (déconseillé : une version est immuable)
USAGE
}

ENV_ARG="" VERSION="" FORCE=false DRY_RUN=false SETS=()
while (($#)); do
  case "$1" in
    --env) ENV_ARG="${2:?}"; shift 2 ;;
    --version) VERSION="${2:?}"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --set) SETS+=("${2:?}"); shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "option inconnue : $1" ;;
  esac
done
require_version "$VERSION"
resolve_profile "$ENV_ARG"; load_profile
((${#SETS[@]})) && apply_overrides "${SETS[@]}"
require_vars HDFS_ARTIFACT_ROOT

SRC="${PROJECT_ROOT}/dist/${VERSION}"
FILES=("$(wheel_name "$VERSION")" "$(env_archive_name "$VERSION")" entrypoint.py manifest.yaml)
for f in "${FILES[@]}"; do [[ -f "${SRC}/${f}" ]] || die "artefact manquant : ${SRC}/${f} (exécutez scripts/package.sh)"; done

DEST="${HDFS_ARTIFACT_ROOT%/}/customer-dp/${VERSION}"
TMP="${DEST}.tmp.$$"
log "publication de ${SRC} -> ${DEST}"
if $DRY_RUN; then
  printf 'hdfs dfs -put %s -> %s puis rename vers %s\n' "${FILES[*]}" "$TMP" "$DEST"; exit 0
fi

need_cmd hdfs "client Hadoop requis"
ensure_kerberos
if hdfs dfs -test -d "$DEST"; then
  $FORCE || die "la version ${VERSION} existe déjà : ${DEST} (--force pour la remplacer)"
fi
hdfs dfs -mkdir -p "$TMP"
for f in "${FILES[@]}"; do hdfs dfs -put -f "${SRC}/${f}" "${TMP}/${f}" || { hdfs dfs -rm -r -skipTrash "$TMP" >/dev/null; die "échec du dépôt de ${f}"; }; done
if $FORCE && hdfs dfs -test -d "$DEST"; then hdfs dfs -rm -r -skipTrash "$DEST" >/dev/null; fi
hdfs dfs -mv "$TMP" "$DEST"
hdfs dfs -ls "$DEST"
log "version ${VERSION} publiée : ${DEST}"
