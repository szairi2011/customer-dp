#!/usr/bin/env bash
# Dépose les CSV d'entrée dans le répertoire HDFS du profil sélectionné.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

usage() {
  cat <<USAGE
Usage : scripts/init_hdfs.sh [--env PROFIL] [--orders FICHIER] [--customers FICHIER] [--set CLE=VALEUR] [--dry-run]
  --env PROFIL       profil conf/profiles/PROFIL.env (défaut : CUSTOMER_DP_ENV puis détection automatique)
  --orders FICHIER   CSV des commandes (défaut : data/sample/orders.csv)
  --customers FICHIER CSV des clients (défaut : data/sample/customers.csv)
  --set CLE=VALEUR   surcharge une valeur du profil (répétable)
  --dry-run          affiche les commandes sans contacter HDFS
USAGE
}

ENV_ARG="" DRY_RUN=false
ORDERS="${PROJECT_ROOT}/data/sample/orders.csv"
CUSTOMERS="${PROJECT_ROOT}/data/sample/customers.csv"
SETS=()
while (($#)); do
  case "$1" in
    --env) ENV_ARG="${2:?--env attend une valeur}"; shift 2 ;;
    --orders) ORDERS="${2:?--orders attend un fichier}"; shift 2 ;;
    --customers) CUSTOMERS="${2:?--customers attend un fichier}"; shift 2 ;;
    --set) SETS+=("${2:?--set attend CLE=VALEUR}"); shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "option inconnue : $1" ;;
  esac
done

resolve_profile "$ENV_ARG"
load_profile
((${#SETS[@]})) && apply_overrides "${SETS[@]}"
require_vars CUSTOMER_DP_INPUT_ROOT

for file in "$ORDERS" "$CUSTOMERS"; do
  [[ -f "$file" ]] || die "fichier source introuvable : $file"
done

INPUT_ROOT="${CUSTOMER_DP_INPUT_ROOT%/}"
log "dépôt des CSV dans ${INPUT_ROOT} (les fichiers HDFS de mêmes noms seront remplacés)"
if $DRY_RUN; then
  printf 'hdfs dfs -mkdir -p %q\n' "$INPUT_ROOT"
  printf 'hdfs dfs -put -f %q %q %q/\n' "$ORDERS" "$CUSTOMERS" "$INPUT_ROOT"
  printf 'hdfs dfs -ls %q\n' "$INPUT_ROOT"
  exit 0
fi

need_cmd hdfs "client Hadoop requis"
ensure_kerberos
hdfs dfs -mkdir -p "$INPUT_ROOT"
hdfs dfs -put -f "$ORDERS" "$CUSTOMERS" "${INPUT_ROOT}/"
hdfs dfs -ls "$INPUT_ROOT"
log "initialisation des données HDFS terminée"
