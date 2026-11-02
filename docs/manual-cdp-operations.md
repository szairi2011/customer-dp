# Opérations CDP sans scripts wrapper

Ce guide complète le [README](../README.md) avec les commandes sous-jacentes aux scripts HDFS et Airflow. Les exemples utilisent `cdp-dev`, `airflow-cdp` et la version `0.1.0`. Pour lancer une couche directement avec `spark3-submit`, voir le [guide Spark](./manual-cdp-spark-submit.md).

## Charger le profil CDP

Depuis la racine du projet, sur la gateway CDP :

```bash
export HDFS_USER="${PLATFORM_HDFS_USER:-${USER:-$(id -un)}}"
set -a
source conf/profiles/cdp-dev.env
set +a
```

Le profil fournit les chemins HDFS, les options Spark/YARN, le binaire `spark3-submit` et les paramètres Python. Vérifiez le ticket Kerberos (`klist`) avant les opérations HDFS.

## Déposer les CSV

`scripts/init_hdfs.sh --env cdp-dev` exécute ces commandes. `-f` remplace les fichiers portant les mêmes noms :

```bash
hdfs dfs -mkdir -p "$CUSTOMER_DP_INPUT_ROOT"
hdfs dfs -put -f data/sample/orders.csv data/sample/customers.csv "$CUSTOMER_DP_INPUT_ROOT/"
hdfs dfs -ls "$CUSTOMER_DP_INPUT_ROOT"
```

Remplacez les deux chemins locaux par vos propres CSV si nécessaire. Les noms HDFS doivent rester `orders.csv` et `customers.csv`.

## Construire et publier une version

`scripts/package.sh` produit dans `dist/0.1.0/` :

- le wheel `customer_dp-0.1.0-py3-none-any.whl` ;
- l'archive `pyenv-py38-0.1.0.tar.gz` avec le code et les dépendances Python hors `pyspark`/`py4j` (fournis par le cluster) ;
- `entrypoint.py` et `manifest.yaml`.

Le script fait les étapes nécessaires (`uv build`, création d'un environnement Python 3.8, puis archive tar). Il est recommandé de le garder comme commande de build :

```bash
scripts/package.sh
```

`scripts/publish_hdfs.sh` vérifie les quatre artefacts, les copie dans un dossier temporaire HDFS, puis renomme ce dossier vers la destination finale. Ce renommage évite qu'un job lise une publication incomplète. Une version publiée est immuable ; choisissez une nouvelle version au lieu d'écraser une version existante.

Équivalent HDFS direct, après le build ci-dessus et le chargement du profil :

```bash
set -euo pipefail
VERSION=0.1.0
SRC="dist/${VERSION}"
DEST="${HDFS_ARTIFACT_ROOT%/}/customer-dp/${VERSION}"
TMP="${DEST}.tmp.$$"

if hdfs dfs -test -d "$DEST"; then
  echo "Version déjà publiée : $DEST (choisissez une nouvelle version)." >&2
  exit 1
fi

hdfs dfs -mkdir -p "$TMP"
hdfs dfs -put -f \
  "$SRC/customer_dp-${VERSION}-py3-none-any.whl" \
  "$SRC/pyenv-py38-${VERSION}.tar.gz" \
  "$SRC/entrypoint.py" \
  "$SRC/manifest.yaml" \
  "$TMP/"
hdfs dfs -mv "$TMP" "$DEST"
hdfs dfs -ls "$DEST"
```

Si une commande de copie échoue, supprimez uniquement le dossier temporaire correspondant (`$TMP`) avant de réessayer. Ne supprimez pas le dossier d'une version publiée.

## Déployer le DAG Airflow CDP

Le profil `airflow-cdp` pointe vers le runtime `cdp-dev`, utilise SSH vers la VM Airflow et déploie en mode Spark client. `scripts/deploy_dag.sh` rend et valide le DAG, copie l'archive Python et `entrypoint.py` dans le répertoire de release, puis remplace le fichier DAG par renommage atomique.

Pour effectuer manuellement le transfert tout en laissant le projet rendre le DAG avec les deux profils :

```bash
VERSION=0.1.0
DAG_LOCAL="/tmp/customer_dp_dag.py"
scripts/deploy_dag.sh --airflow-env airflow-cdp --version "$VERSION" --render-only "$DAG_LOCAL"
```

Dans le profil CDP actuel, les valeurs SSH et les répertoires sont `AIRFLOW_HOST=172.28.3.226`, `AIRFLOW_PORT=22`, `AIRFLOW_DAGS_DIR=/root/airflow/dags/customer-dp` et `AIRFLOW_RELEASE_ROOT=/root/airflow/dags/customer-dp/releases`. `AIRFLOW_USER` est vide par défaut : SSH utilisera l'utilisateur courant.

Les commandes SSH/SCP sous-jacentes (pour une nouvelle version) sont :

```bash
AIRFLOW_USER="${AIRFLOW_USER:-$USER}"
TARGET="${AIRFLOW_USER}@172.28.3.226"
PORT=22
DAGS_DIR="/root/airflow/dags/customer-dp"
RELEASE="/root/airflow/dags/customer-dp/releases/${VERSION}"
RELEASE_TMP="${RELEASE}.tmp.$$"
ARCHIVE="pyenv-py38-${VERSION}.tar.gz"

# La release est versionnée : refusez de remplacer une release existante.
ssh -p "$PORT" "$TARGET" "test ! -e '$RELEASE' || { echo 'Release exists' >&2; exit 1; }; mkdir -p '$RELEASE_TMP/pyenv'"
scp -P "$PORT" "dist/${VERSION}/entrypoint.py" "$TARGET:$RELEASE_TMP/entrypoint.py"
scp -P "$PORT" "dist/${VERSION}/${ARCHIVE}" "$TARGET:$RELEASE_TMP/${ARCHIVE}"
ssh -p "$PORT" "$TARGET" "tar -xzf '$RELEASE_TMP/${ARCHIVE}' -C '$RELEASE_TMP/pyenv' && mv '$RELEASE_TMP' '$RELEASE'"

# Copie du DAG dans un fichier temporaire, puis renommage dans le dossier surveillé par Airflow.
ssh -p "$PORT" "$TARGET" "mkdir -p '$DAGS_DIR'"
scp -P "$PORT" "$DAG_LOCAL" "$TARGET:$DAGS_DIR/customer_dp_dag.py.tmp"
ssh -p "$PORT" "$TARGET" "mv -f '$DAGS_DIR/customer_dp_dag.py.tmp' '$DAGS_DIR/customer_dp_dag.py'"
```

Le scheduler Airflow détecte le fichier DAG. Sur la VM, vérifiez puis activez-le :

```bash
airflow dags list-import-errors
airflow dags list | grep customer_dp
airflow dags trigger customer_dp
airflow dags unpause customer_dp
```

Le DAG est créé en pause. Le profil `airflow-cdp` utilise `AIRFLOW_SUBMIT_MODE=provider` et la connexion Airflow `spark3_default`; vérifiez que cette connexion existe avant le déclenchement.
