#!/usr/bin/env bash
# Déploie le DAG Airflow 2.11 : rendu depuis le profil, validation Python, copie atomique.
#   --mode ssh   : VM Airflow distante (scp + ssh), paramètres AIRFLOW_HOST/PORT/USER du profil
#   --mode local : dossier DAG partagé, accessible depuis ce poste (AIRFLOW_DAGS_DIR)
# En mode client Spark, l'environnement Python extrait est aussi déposé sur la VM Airflow (le driver y tourne).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

usage() {
  cat <<USAGE
Usage : scripts/deploy_dag.sh --airflow-env PROFIL_AIRFLOW --version X.Y.Z [--mode ssh|local] [--dry-run]
  --airflow-env  profil conf/profiles/<nom>.env côté Airflow (ex. airflow-cdp, airflow-hadoop)
  --render-only  FICHIER  écrit le DAG rendu dans FICHIER et s'arrête (aucune copie)
Le profil Airflow référence le profil d'exécution Spark via AIRFLOW_RUNTIME_PROFILE.
USAGE
}

AIRFLOW_ENV="" VERSION="" MODE="ssh" DRY_RUN=false RENDER_ONLY="" SETS=()
while (($#)); do
  case "$1" in
    --airflow-env) AIRFLOW_ENV="${2:?}"; shift 2 ;;
    --version) VERSION="${2:?}"; shift 2 ;;
    --mode) MODE="${2:?}"; shift 2 ;;
    --render-only) RENDER_ONLY="${2:?}"; shift 2 ;;
    --set) SETS+=("${2:?}"); shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "option inconnue : $1" ;;
  esac
done
require_version "$VERSION"
[[ -n "$AIRFLOW_ENV" ]] || { usage >&2; die "--airflow-env est obligatoire"; }
[[ "$MODE" == ssh || "$MODE" == local ]] || die "--mode doit valoir ssh ou local"
need_cmd python3 "requis pour rendre et valider le DAG"

# Profil Airflow puis profil d'exécution Spark qu'il référence.
resolve_profile "$AIRFLOW_ENV"; load_profile
require_vars AIRFLOW_RUNTIME_PROFILE AIRFLOW_DAGS_DIR
resolve_profile "$AIRFLOW_RUNTIME_PROFILE"; load_profile
((${#SETS[@]})) && apply_overrides "${SETS[@]}"
require_vars SPARK_SUBMIT SPARK_MASTER DEPLOY_MODE PYTHON_DRIVER PYTHON_EXECUTOR HADOOP_CONF_DIR HDFS_ARTIFACT_ROOT
# Les chemins distants sont transmis à ssh : jeu de caractères restreint.
for v in AIRFLOW_DAGS_DIR AIRFLOW_RELEASE_ROOT; do
  [[ -z "${!v:-}" || ( "${!v}" =~ ^/[A-Za-z0-9_./-]+$ && "${!v}" != *..* ) ]] || die "$v invalide (chemin absolu simple attendu) : ${!v}"
done

export VERSION ARTIFACT_ARCHIVE; ARTIFACT_ARCHIVE="$(env_archive_name "$VERSION")"
ARTIFACT_URI="$(hdfs_uri "${HDFS_ARTIFACT_ROOT%/}/customer-dp/${VERSION}")"; export ARTIFACT_URI
CLIENT_RELEASE_DIR=""
if [[ "$DEPLOY_MODE" == client ]]; then
  require_vars AIRFLOW_RELEASE_ROOT
  CLIENT_RELEASE_DIR="${AIRFLOW_RELEASE_ROOT%/}/${VERSION}"
fi
export CLIENT_RELEASE_DIR

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
DAG_FILE="customer_dp_dag.py"
log "1/4 rendu du DAG (soumission : ${AIRFLOW_SUBMIT_MODE:-?}, Spark ${DEPLOY_MODE})"
python3 "${PROJECT_ROOT}/scripts/render_dag.py" "${PROJECT_ROOT}/dags/${DAG_FILE}" "${WORK}/${DAG_FILE}"
log "2/4 validation de la syntaxe Python"
python3 -m py_compile "${WORK}/${DAG_FILE}"
if [[ -n "$RENDER_ONLY" ]]; then cp "${WORK}/${DAG_FILE}" "$RENDER_ONLY"; log "DAG rendu : $RENDER_ONLY"; exit 0; fi

SSH_TARGET="${AIRFLOW_USER:+${AIRFLOW_USER}@}${AIRFLOW_HOST:-}"
CTRL="${WORK}/ssh-ctl"
ssh_opts=(-o ControlMaster=auto -o "ControlPath=${CTRL}" -o ControlPersist=60 -p "${AIRFLOW_PORT:-22}")
scp_opts=(-o ControlMaster=auto -o "ControlPath=${CTRL}" -o ControlPersist=60 -P "${AIRFLOW_PORT:-22}")
remote() { if [[ "$MODE" == ssh ]]; then ssh "${ssh_opts[@]}" "$SSH_TARGET" "$@"; else bash -c "$*"; fi; }
push() { # push SOURCE DEST
  if [[ "$MODE" == ssh ]]; then scp -q "${scp_opts[@]}" "$1" "${SSH_TARGET}:$2"; else cp "$1" "$2"; fi
}
if [[ "$MODE" == ssh ]]; then require_vars AIRFLOW_HOST; need_cmd ssh "client SSH requis"; need_cmd scp "client SSH requis"; fi

SRC_DIST="${PROJECT_ROOT}/dist/${VERSION}"
if [[ "$DEPLOY_MODE" == client ]]; then
  for f in "$ARTIFACT_ARCHIVE" entrypoint.py; do [[ -f "${SRC_DIST}/${f}" ]] || die "artefact manquant : ${SRC_DIST}/${f} (scripts/package.sh)"; done
fi

if $DRY_RUN; then
  log "dry-run : aucune copie. Actions prévues (mode ${MODE}, cible ${SSH_TARGET:-local}) :"
  [[ -n "$CLIENT_RELEASE_DIR" ]] && echo "  - déposer et extraire l'environnement Python dans ${CLIENT_RELEASE_DIR}"
  echo "  - copier le DAG vers ${AIRFLOW_DAGS_DIR}/${DAG_FILE} (fichier temporaire puis mv atomique)"
  exit 0
fi

log "3/4 copie vers ${SSH_TARGET:-le dossier local}"
if [[ -n "$CLIENT_RELEASE_DIR" ]]; then
  # Environnement d'abord : le DAG ne doit jamais référencer une release absente.
  TMP_REL="${CLIENT_RELEASE_DIR}.tmp.$$"
  remote "mkdir -p '${TMP_REL}/pyenv'"
  push "${SRC_DIST}/entrypoint.py" "${TMP_REL}/entrypoint.py"
  push "${SRC_DIST}/${ARTIFACT_ARCHIVE}" "${TMP_REL}/${ARTIFACT_ARCHIVE}"
  remote "tar -xzf '${TMP_REL}/${ARTIFACT_ARCHIVE}' -C '${TMP_REL}/pyenv' && rm -rf '${CLIENT_RELEASE_DIR}' && mv '${TMP_REL}' '${CLIENT_RELEASE_DIR}'"
fi
remote "mkdir -p '${AIRFLOW_DAGS_DIR}'"
# Le suffixe .tmp évite que le scheduler lise un fichier partiellement écrit (seuls les *.py sont analysés).
push "${WORK}/${DAG_FILE}" "${AIRFLOW_DAGS_DIR}/${DAG_FILE}.tmp"
remote "mv -f '${AIRFLOW_DAGS_DIR}/${DAG_FILE}.tmp' '${AIRFLOW_DAGS_DIR}/${DAG_FILE}'"

log "4/4 DAG déployé : ${AIRFLOW_DAGS_DIR}/${DAG_FILE} (dag_id=${DAG_ID})"
cat <<NEXT
Vérifications post-déploiement (sur la VM Airflow) :
  1. attendre le prochain scan du dossier DAG, puis : airflow dags list-import-errors   (aucune erreur attendue)
  2. airflow dags list | grep ${DAG_ID}
  3. déclencher une exécution manuelle : airflow dags trigger ${DAG_ID}
  4. vérifier les 3 tâches, puis activer la planification : airflow dags unpause ${DAG_ID}
NEXT
