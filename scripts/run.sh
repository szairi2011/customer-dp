#!/usr/bin/env bash
# Exécute une ou plusieurs couches du data product selon le profil d'environnement.
#   local        : python direct (PySpark installé par uv)
#   cdp-dev/...  : spark-submit sur YARN avec les artefacts versionnés (voir package.sh / publish_hdfs.sh)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

usage() {
  cat <<USAGE
Usage : scripts/run.sh [--env PROFIL] --layer bronze|silver|gold|verify|all [options]

  --env PROFIL        profil conf/profiles/PROFIL.env (défaut : CUSTOMER_DP_ENV puis détection automatique)
  --layer COUCHE      couche à exécuter ; 'all' = bronze, silver, gold
  --version X.Y.Z     version publiée à exécuter (obligatoire avec spark-submit)
  --batch-date DATE   date de lot Bronze AAAA-MM-JJ (défaut : aujourd'hui)
  --start-date DATE   début de période Gold (rejeu)       --end-date DATE   fin de période Gold
  --expect CLE=N...   volumes attendus pour 'verify' : silver_rows, rejected_rows, gold_rows
  --set CLE=VALEUR    surcharge une variable du profil (répétable), ex. CUSTOMER_DP_OUTPUT_ROOT=...
  --dry-run           affiche la commande sans l'exécuter ni télécharger d'artefact
  -h, --help          cette aide
USAGE
}

ENV_ARG="" LAYER="" VERSION="" BATCH_DATE="" START_DATE="" END_DATE="" DRY_RUN=false
SETS=() EXPECTS=()
while (($#)); do
  case "$1" in
    --env) ENV_ARG="${2:?--env attend une valeur}"; shift 2 ;;
    --layer) LAYER="${2:?--layer attend une valeur}"; shift 2 ;;
    --version) VERSION="${2:?--version attend une valeur}"; shift 2 ;;
    --batch-date) BATCH_DATE="${2:?}"; shift 2 ;;
    --start-date) START_DATE="${2:?}"; shift 2 ;;
    --end-date) END_DATE="${2:?}"; shift 2 ;;
    --expect) shift; while (($#)) && [[ "$1" != --* ]]; do EXPECTS+=("$1"); shift; done ;;
    --set) SETS+=("${2:?}"); shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "option inconnue : $1" ;;
  esac
done

case "$LAYER" in
  bronze|silver|gold|verify) LAYERS=("$LAYER") ;;
  all) LAYERS=(bronze silver gold) ;;
  *) usage >&2; die "--layer doit valoir bronze, silver, gold, verify ou all" ;;
esac
for d in "$BATCH_DATE" "$START_DATE" "$END_DATE"; do
  [[ -z "$d" || "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "date invalide : $d (format AAAA-MM-JJ)"
done

resolve_profile "$ENV_ARG"
load_profile
((${#SETS[@]})) && apply_overrides "${SETS[@]}"
LAUNCHER="${LAUNCHER:-spark-submit}"

# layer_args COUCHE : arguments applicatifs de la couche (la configuration vient des variables CUSTOMER_DP_*).
layer_args() {
  local layer="$1"
  ARGS=("$layer")
  case "$layer" in
    bronze) [[ -n "$BATCH_DATE" ]] && ARGS+=(--batch-date "$BATCH_DATE") ;;
    gold)
      [[ -n "$START_DATE" ]] && ARGS+=(--start-date "$START_DATE")
      [[ -n "$END_DATE" ]] && ARGS+=(--end-date "$END_DATE") ;;
    verify) ((${#EXPECTS[@]})) && ARGS+=(--expect "${EXPECTS[@]}") ;;
  esac
  return 0
}

print_cmd() { printf '%q ' "$@"; echo; }

run_python() {
  require_vars CUSTOMER_DP_INPUT_ROOT CUSTOMER_DP_OUTPUT_ROOT
  need_cmd uv "installez uv : https://docs.astral.sh/uv/"
  for layer in "${LAYERS[@]}"; do
    layer_args "$layer"
    cmd=(uv run --locked --project "$PROJECT_ROOT" python "${PROJECT_ROOT}/scripts/entrypoint.py" "${ARGS[@]}")
    log "couche=${layer} launcher=python"
    if $DRY_RUN; then print_cmd "${cmd[@]}"; else "${cmd[@]}"; fi
  done
}

run_spark_submit() {
  require_vars SPARK_SUBMIT SPARK_MASTER DEPLOY_MODE PYTHON_DRIVER PYTHON_EXECUTOR HADOOP_CONF_DIR HDFS_ARTIFACT_ROOT \
    CUSTOMER_DP_INPUT_ROOT CUSTOMER_DP_OUTPUT_ROOT
  require_version "$VERSION"
  [[ "$DEPLOY_MODE" == client || "$DEPLOY_MODE" == cluster ]] || die "DEPLOY_MODE invalide : $DEPLOY_MODE"
  if ! $DRY_RUN; then
    [[ -x "$SPARK_SUBMIT" ]] || command -v "$SPARK_SUBMIT" >/dev/null 2>&1 || die "spark-submit introuvable : $SPARK_SUBMIT"
    ensure_kerberos
  fi

  local artifact_uri; artifact_uri="$(hdfs_uri "${HDFS_ARTIFACT_ROOT%/}/customer-dp/${VERSION}")"
  local archive; archive="$(env_archive_name "$VERSION")"
  local cache="${PROJECT_ROOT}/.local/cache/${VERSION}"
  local app_file driver_pythonpath

  if [[ "$DEPLOY_MODE" == client ]]; then
    # Client : le driver tourne ici ; il lit les artefacts extraits dans le cache local.
    app_file="${cache}/entrypoint.py"; driver_pythonpath="${cache}/pyenv"
    $DRY_RUN || fetch_artifacts "$artifact_uri" "$archive" "$cache"
  else
    app_file="${artifact_uri}/entrypoint.py"; driver_pythonpath=""
  fi

  export HADOOP_CONF_DIR YARN_CONF_DIR="$HADOOP_CONF_DIR"
  export PYSPARK_PYTHON="$PYTHON_EXECUTOR" PYSPARK_DRIVER_PYTHON="$PYTHON_DRIVER"
  [[ -n "$driver_pythonpath" ]] && export PYTHONPATH="${driver_pythonpath}${PYTHONPATH:+:${PYTHONPATH}}"

  local common=("$SPARK_SUBMIT" --master "$SPARK_MASTER" --deploy-mode "$DEPLOY_MODE")
  [[ -n "${SPARK_DRIVER_MEMORY:-}" ]] && common+=(--driver-memory "$SPARK_DRIVER_MEMORY")
  [[ -n "${SPARK_EXECUTOR_MEMORY:-}" ]] && common+=(--executor-memory "$SPARK_EXECUTOR_MEMORY")
  [[ -n "${SPARK_EXECUTOR_CORES:-}" ]] && common+=(--executor-cores "$SPARK_EXECUTOR_CORES")
  [[ -n "${SPARK_NUM_EXECUTORS:-}" ]] && common+=(--num-executors "$SPARK_NUM_EXECUTORS")
  [[ -n "${SPARK_JARS:-}" ]] && common+=(--jars "$SPARK_JARS")
  # Le code Python voyage dans une archive extraite par YARN (--archives), jamais via --py-files.
  common+=(--archives "${artifact_uri}/${archive}#pyenv"
    --conf "spark.pyspark.python=${PYTHON_EXECUTOR}" --conf "spark.pyspark.driver.python=${PYTHON_DRIVER}"
    --conf "spark.executorEnv.PYTHONPATH=pyenv" --conf "spark.yarn.appMasterEnv.PYTHONPATH=pyenv")
  while IFS= read -r item; do common+=("$item"); done < <(spark_conf_args)
  if [[ "$DEPLOY_MODE" == cluster ]]; then
    # En mode cluster, le driver est sur YARN : les paramètres CUSTOMER_DP_* lui sont transmis explicitement.
    for name in $(compgen -v CUSTOMER_DP_); do common+=(--conf "spark.yarn.appMasterEnv.${name}=${!name}"); done
  fi

  for layer in "${LAYERS[@]}"; do
    layer_args "$layer"
    cmd=("${common[@]}" --name "customer-dp-${layer}-${VERSION}" "$app_file" "${ARGS[@]}")
    log "couche=${layer} version=${VERSION} mode=${DEPLOY_MODE} master=${SPARK_MASTER}"
    if $DRY_RUN; then print_cmd "${cmd[@]}"; else "${cmd[@]}"; fi
  done
}

# fetch_artifacts URI ARCHIVE CACHE : copie (dist/ local, sinon HDFS) puis extrait l'environnement Python.
fetch_artifacts() {
  local uri="$1" archive="$2" cache="$3" file local_dist="${PROJECT_ROOT}/dist/${VERSION}"
  mkdir -p "$cache"
  for file in entrypoint.py "$archive"; do
    if [[ -f "${local_dist}/${file}" ]]; then
      if ! cmp -s "${local_dist}/${file}" "${cache}/${file}"; then cp "${local_dist}/${file}" "${cache}/${file}"; rm -rf "${cache}/pyenv"; fi
    elif [[ ! -f "${cache}/${file}" ]]; then
      need_cmd hdfs "client Hadoop requis pour télécharger ${uri}/${file}"
      hdfs dfs -get "${uri}/${file}" "${cache}/${file}" || die "téléchargement impossible : ${uri}/${file}"
    fi
  done
  if [[ ! -d "${cache}/pyenv" ]]; then
    mkdir -p "${cache}/pyenv.tmp" && tar -xzf "${cache}/${archive}" -C "${cache}/pyenv.tmp" && mv "${cache}/pyenv.tmp" "${cache}/pyenv"
  fi
}

case "$LAUNCHER" in
  python) run_python ;;
  spark-submit) run_spark_submit ;;
  *) die "LAUNCHER invalide : $LAUNCHER (python|spark-submit)" ;;
esac
log "terminé"
