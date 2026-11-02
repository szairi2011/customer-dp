#!/usr/bin/env bash
# Bibliothèque commune (à sourcer) : résolution du profil d'environnement et utilitaires.
# Un profil = un fichier conf/profiles/<nom>.env ; les scripts ne contiennent aucune valeur d'environnement.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_DIR="${PROJECT_ROOT}/conf/profiles"
HDFS_USER="${PLATFORM_HDFS_USER:-${USER:-$(id -un)}}"
export PROJECT_ROOT HDFS_USER

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }
die() { echo "ERREUR : $*" >&2; exit 1; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "commande introuvable : $1 ($2)"; }

# detect_profile : cdp-dev si les parcels Cloudera sont présents, hadoop si un client Hadoop existe, sinon local.
detect_profile() {
  if [[ -d /opt/cloudera/parcels/SPARK3 ]]; then echo "cdp-dev"
  elif [[ -n "${HADOOP_CONF_DIR:-}" ]] || command -v hdfs >/dev/null 2>&1; then echo "hadoop"
  else echo "local"; fi
}

# resolve_profile [valeur --env] : définit PROFILE et PROFILE_SOURCE (option > CUSTOMER_DP_ENV > détection).
resolve_profile() {
  if [[ -n "${1:-}" ]]; then PROFILE="$1"; PROFILE_SOURCE="--env"
  elif [[ -n "${CUSTOMER_DP_ENV:-}" ]]; then PROFILE="$CUSTOMER_DP_ENV"; PROFILE_SOURCE="CUSTOMER_DP_ENV"
  else PROFILE="$(detect_profile)"; PROFILE_SOURCE="détection automatique"; fi
  [[ "$PROFILE" =~ ^[A-Za-z0-9._-]+$ ]] || die "nom de profil invalide : $PROFILE"
  PROFILE_FILE="${PROFILE_DIR}/${PROFILE}.env"
  [[ -f "$PROFILE_FILE" ]] || die "profil introuvable : $PROFILE_FILE (profils disponibles : $(cd "$PROFILE_DIR" && ls ./*.env 2>/dev/null | sed 's#./##;s#\.env$##' | grep -v '\.local$' | tr '\n' ' '))"
}

# load_profile : source le profil, puis la surcharge locale non versionnée si elle existe.
load_profile() {
  set -a
  # shellcheck disable=SC1090
  source "$PROFILE_FILE"
  if [[ -f "${PROFILE_DIR}/${PROFILE}.local.env" ]]; then
    # shellcheck disable=SC1090
    source "${PROFILE_DIR}/${PROFILE}.local.env"
  fi
  set +a
  log "profil=${PROFILE} (source : ${PROFILE_SOURCE})"
}

# apply_overrides KEY=VALUE... : surcharges explicites passées par --set (après le profil).
apply_overrides() {
  local item
  for item in "$@"; do
    [[ "$item" =~ ^[A-Za-z_][A-Za-z0-9_]*=.*$ ]] || die "--set attend CLE=VALEUR (reçu : $item)"
    export "${item%%=*}=${item#*=}"
  done
}

# require_vars VAR... : échoue en nommant le profil à compléter.
require_vars() {
  local missing=() name
  for name in "$@"; do [[ -n "${!name:-}" ]] || missing+=("$name"); done
  if ((${#missing[@]})); then
    die "variable(s) obligatoire(s) non renseignée(s) pour le profil '${PROFILE}' : ${missing[*]}
       -> renseignez-les dans ${PROFILE_FILE} (ou ${PROFILE_DIR}/${PROFILE}.local.env)"
  fi
}

# read_version : version déclarée dans pyproject.toml.
read_version() { sed -n 's/^version = "\(.*\)"/\1/p' "${PROJECT_ROOT}/pyproject.toml" | head -1; }

# require_version VERSION : format X.Y.Z[suffixe] (évite tout chemin imprévu).
require_version() {
  [[ -n "${1:-}" ]] || die "--version est obligatoire"
  [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+([A-Za-z0-9.+-]*)$ ]] || die "version invalide : $1"
}

# spark_conf_args : une option --conf par ligne de SPARK_CONF.
spark_conf_args() {
  local line
  while IFS= read -r line; do
    line="${line//[$'\t\r']/}"; [[ -z "${line// /}" || "$line" == \#* ]] && continue
    printf '%s\n' "--conf" "$line"
  done <<<"${SPARK_CONF:-}"
}

# ensure_kerberos : vérifie ou obtient un ticket selon KERBEROS_MODE (jamais d'affichage de secret).
ensure_kerberos() {
  case "${KERBEROS_MODE:-none}" in
    none) ;;
    ticket) klist -s 2>/dev/null || die "aucun ticket Kerberos valide : exécutez kinit" ;;
    keytab)
      require_vars KERBEROS_PRINCIPAL KERBEROS_KEYTAB
      [[ -r "$KERBEROS_KEYTAB" ]] || die "keytab illisible : $KERBEROS_KEYTAB"
      kinit -kt "$KERBEROS_KEYTAB" "$KERBEROS_PRINCIPAL" || die "kinit a échoué" ;;
    *) die "KERBEROS_MODE invalide : ${KERBEROS_MODE} (none|ticket|keytab)" ;;
  esac
}

# Nom des artefacts d'une version.
env_archive_name() { echo "pyenv-py38-$1.tar.gz"; }
wheel_name() { echo "customer_dp-$1-py3-none-any.whl"; }

# hdfs_uri CHEMIN : qualifie un chemin HDFS absolu avec le système de fichiers par défaut.
# spark-submit résout sinon un chemin sans schéma sur le disque local (file:). Surcharge : HDFS_DEFAULT_FS.
hdfs_uri() {
  local path="$1"
  [[ "$path" == /* ]] || { printf '%s' "$path"; return; }
  if [[ -z "${HDFS_DEFAULT_FS:-}" ]]; then
    HDFS_DEFAULT_FS="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || true)"
  fi
  if [[ -z "$HDFS_DEFAULT_FS" ]]; then
    ${DRY_RUN:-false} || die "fs.defaultFS introuvable : renseignez HDFS_DEFAULT_FS dans ${PROFILE_FILE} ou installez le client hdfs"
    HDFS_DEFAULT_FS="hdfs://<fs.defaultFS>"
  fi
  printf '%s%s' "${HDFS_DEFAULT_FS%/}" "$path"
}
