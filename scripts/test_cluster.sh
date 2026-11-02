#!/usr/bin/env bash
# Teste une version publiée sur le cluster, en isolation : jamais d'écriture dans les chemins de production.
# Sorties : <HDFS_ARTIFACT_ROOT>/customer-dp-test/<préfixe>/ ; base Hive : <CUSTOMER_DP_DATABASE>_test (si --hive).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

usage() {
  cat <<USAGE
Usage : scripts/test_cluster.sh [--env PROFIL] --version X.Y.Z [options]
  --layer COUCHE      bronze|silver|gold|all (défaut : all, avec vérification des volumes du jeu d'exemple)
  --test-prefix NOM   sous-dossier de test (défaut : run-<horodatage>)
  --config-set K=V    surcharge supplémentaire du profil (répétable)
  --hive              enregistre aussi les tables dans la base Hive de test
  --keep              conserve les sorties de test (sinon supprimées en fin de test)
  --dry-run           affiche les étapes sans rien exécuter
Pré-requis : version publiée (scripts/publish_hdfs.sh) et ticket Kerberos valide.
USAGE
}

ENV_ARG="" VERSION="" LAYER="all" PREFIX="" KEEP=false HIVE=false DRY_RUN=false SETS=()
while (($#)); do
  case "$1" in
    --env) ENV_ARG="${2:?}"; shift 2 ;;
    --version) VERSION="${2:?}"; shift 2 ;;
    --layer) LAYER="${2:?}"; shift 2 ;;
    --test-prefix) PREFIX="${2:?}"; shift 2 ;;
    --config-set) SETS+=("${2:?}"); shift 2 ;;
    --hive) HIVE=true; shift ;;
    --keep) KEEP=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "option inconnue : $1" ;;
  esac
done
require_version "$VERSION"
[[ "$LAYER" =~ ^(bronze|silver|gold|all)$ ]] || die "--layer doit valoir bronze, silver, gold ou all"
PREFIX="${PREFIX:-run-$(date +%Y%m%d%H%M%S)}"
[[ "$PREFIX" =~ ^[A-Za-z0-9._-]+$ ]] || die "préfixe invalide : $PREFIX"

resolve_profile "$ENV_ARG"; load_profile
[[ "${LAUNCHER:-spark-submit}" == "spark-submit" ]] || die "test_cluster.sh exige un profil de cluster (reçu : ${PROFILE}, launcher=${LAUNCHER})"
require_vars HDFS_ARTIFACT_ROOT SPARK_SUBMIT

TEST_ROOT="${HDFS_ARTIFACT_ROOT%/}/customer-dp-test/${PREFIX}"
TEST_IN="${TEST_ROOT}/input"; TEST_OUT="${TEST_ROOT}/output"
TEST_DB="${CUSTOMER_DP_DATABASE:-customer_dp}_test"
[[ "$TEST_OUT" != "${CUSTOMER_DP_OUTPUT_ROOT:-}" && "$TEST_ROOT" == *"/customer-dp-test/"* ]] || die "chemin de test non isolé : ${TEST_ROOT}"

RUN=("${PROJECT_ROOT}/scripts/run.sh" --env "$PROFILE" --version "$VERSION"
  --set "CUSTOMER_DP_INPUT_ROOT=${TEST_IN}" --set "CUSTOMER_DP_OUTPUT_ROOT=${TEST_OUT}"
  --set "CUSTOMER_DP_DATABASE=${TEST_DB}" --set "CUSTOMER_DP_HIVE_ENABLED=${HIVE}")
for s in "${SETS[@]:-}"; do [[ -n "$s" ]] && RUN+=(--set "$s"); done
$DRY_RUN && RUN+=(--dry-run)

step() { log "== $*"; }
if $DRY_RUN; then
  step "dry-run : racine de test ${TEST_ROOT}, base ${TEST_DB}"
  "${RUN[@]}" --layer "$LAYER" --batch-date 2026-03-10
  exit 0
fi

need_cmd hdfs "client Hadoop requis"
ensure_kerberos
ARTIFACT_URI="${HDFS_ARTIFACT_ROOT%/}/customer-dp/${VERSION}"
hdfs dfs -test -f "${ARTIFACT_URI}/$(env_archive_name "$VERSION")" || die "version non publiée : ${ARTIFACT_URI} (scripts/publish_hdfs.sh)"

cleanup() {
  if $KEEP; then log "sorties conservées : ${TEST_ROOT}"
  else hdfs dfs -rm -r -skipTrash "$TEST_ROOT" >/dev/null 2>&1 || true; log "sorties de test supprimées"; fi
  if $HIVE; then
    # Base de test uniquement (suffixe _test garanti plus haut) ; les tables sont externes : seules les métadonnées sont supprimées.
    if command -v hive >/dev/null 2>&1 && hive -e "DROP DATABASE IF EXISTS ${TEST_DB} CASCADE" >/dev/null 2>&1; then
      log "base Hive de test supprimée : ${TEST_DB}"
    else
      log "à supprimer manuellement si besoin : DROP DATABASE IF EXISTS ${TEST_DB} CASCADE"
    fi
  fi
}
trap cleanup EXIT

step "1/5 dépôt des données d'exemple dans ${TEST_IN}"
hdfs dfs -mkdir -p "$TEST_IN"
hdfs dfs -put -f "${PROJECT_ROOT}/data/sample/orders.csv" "${PROJECT_ROOT}/data/sample/customers.csv" "$TEST_IN/"

step "2/5 exécution (${LAYER})"
"${RUN[@]}" --layer "$LAYER" --batch-date 2026-03-10

step "3/5 présence des sorties"
# _SUCCESS n'est pas garanti selon le committer : on vérifie la présence de fichiers Parquet.
check_out() {
  hdfs dfs -ls -R "${TEST_OUT}/$1" 2>/dev/null | grep -q '\.parquet$' || die "sortie absente ou vide : ${TEST_OUT}/$1"
  log "ok $1"
}
case "$LAYER" in
  bronze) check_out bronze/bronze_orders; check_out bronze/bronze_customers ;;
  silver) check_out silver/silver_orders; check_out silver/silver_orders_rejected ;;
  gold) check_out gold/gold_daily_sales ;;
  all) for t in bronze/bronze_orders bronze/bronze_customers silver/silver_orders silver/silver_orders_rejected gold/gold_daily_sales; do check_out "$t"; done ;;
esac

if [[ "$LAYER" == all ]]; then
  step "4/5 contrôles de qualité et volumes attendus du jeu d'exemple"
  "${RUN[@]}" --layer verify --expect silver_rows=9 rejected_rows=5 gold_rows=3
  step "5/5 rejeu (mêmes volumes attendus, sans doublon)"
  "${RUN[@]}" --layer all --batch-date 2026-03-10
  "${RUN[@]}" --layer gold --start-date 2026-03-02 --end-date 2026-03-02
  "${RUN[@]}" --layer verify --expect silver_rows=9 rejected_rows=5 gold_rows=3
fi
log "test cluster réussi (version ${VERSION}, profil ${PROFILE})"
