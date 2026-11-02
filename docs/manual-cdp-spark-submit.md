# Lancer les jobs CDP avec `spark3-submit`

Ce guide montre comment soumettre une couche directement depuis la gateway CDP, sans `scripts/run.sh`.
Les exemples utilisent le profil `cdp-dev` et la version `0.1.0`.

## Pré-requis

Depuis la racine du projet :

1. Préparez et publiez les artefacts une fois :

   ```bash
   scripts/package.sh
   scripts/publish_hdfs.sh --env cdp-dev --version 0.1.0
   ```

2. Déposez les CSV d'entrée dans HDFS avec `scripts/init_hdfs.sh --env cdp-dev`, si ce n'est pas déjà fait.
3. Vérifiez votre ticket Kerberos avec `klist` (obtenez-en un avec `kinit` si nécessaire).

## Préparer l'environnement local du driver

`spark3-submit` démarre le driver sur la gateway avec le profil `cdp-dev`. Le script de lancement habituel télécharge automatiquement les artefacts du driver ; ici, on le fait explicitement.

```bash
export PROJECT_ROOT="$PWD"
export HDFS_USER="${PLATFORM_HDFS_USER:-${USER:-$(id -un)}}"
set -a
source conf/profiles/cdp-dev.env
set +a

export YARN_CONF_DIR="$HADOOP_CONF_DIR"
export PYSPARK_PYTHON="$PYTHON_EXECUTOR"
export PYSPARK_DRIVER_PYTHON="$PYTHON_DRIVER"

VERSION=0.1.0
ARTIFACT_DIR="${HDFS_ARTIFACT_ROOT%/}/customer-dp/${VERSION}"
HDFS_FS="$(hdfs getconf -confKey fs.defaultFS)"
ARTIFACT_URI="${HDFS_FS%/}${ARTIFACT_DIR}"
DRIVER_DIR="${PROJECT_ROOT}/.local/manual/${VERSION}"

mkdir -p "${DRIVER_DIR}/pyenv"
hdfs dfs -get -f "${ARTIFACT_URI}/entrypoint.py" "${DRIVER_DIR}/entrypoint.py"
hdfs dfs -get -f "${ARTIFACT_URI}/pyenv-py38-${VERSION}.tar.gz" "${DRIVER_DIR}/pyenv-py38-${VERSION}.tar.gz"
tar -xzf "${DRIVER_DIR}/pyenv-py38-${VERSION}.tar.gz" -C "${DRIVER_DIR}/pyenv"
export PYTHONPATH="${DRIVER_DIR}/pyenv${PYTHONPATH:+:${PYTHONPATH}}"
```

## Soumettre Bronze

Cette commande est l'équivalent direct de `scripts/run.sh --env cdp-dev --version 0.1.0 --layer bronze`. Les options Spark et les chemins correspondent au profil `cdp-dev`.

```bash
/opt/cloudera/parcels/SPARK3/bin/spark3-submit \
  --master "$SPARK_MASTER" \
  --deploy-mode "$DEPLOY_MODE" \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --executor-memory "$SPARK_EXECUTOR_MEMORY" \
  --executor-cores "$SPARK_EXECUTOR_CORES" \
  --num-executors "$SPARK_NUM_EXECUTORS" \
  --jars "$SPARK_JARS" \
  --archives "${ARTIFACT_URI}/pyenv-py38-${VERSION}.tar.gz#pyenv" \
  --conf "spark.pyspark.python=${PYTHON_EXECUTOR}" \
  --conf "spark.pyspark.driver.python=${PYTHON_DRIVER}" \
  --conf "spark.executorEnv.PYTHONPATH=pyenv" \
  --conf "spark.yarn.appMasterEnv.PYTHONPATH=pyenv" \
  --conf "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict" \
  --conf "spark.hadoop.hadoop.security.authentication=kerberos" \
  --conf "spark.sql.shuffle.partitions=40" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  --conf "spark.yarn.executor.memoryOverhead=1g" \
  --conf "spark.network.timeout=300s" \
  --conf "spark.rpc.askTimeout=300s" \
  --conf "spark.executor.heartbeatInterval=60s" \
  --name "customer-dp-bronze-${VERSION}" \
  "${DRIVER_DIR}/entrypoint.py" bronze --batch-date 2026-03-10
```

Each layer is a separate submission. To run Silver on CDP, reuse the Bronze command and replace its final two lines with:

```bash
  --name "customer-dp-silver-${VERSION}" \
  "${DRIVER_DIR}/entrypoint.py" silver
```

That submits `entrypoint.py silver`, which dispatches to `customer_dp.silver.main`. Gold and verify are submitted the same way, using `gold` or `verify` as the final argument (and the corresponding application name).

To compare this command with the one assembled by the wrapper, run `scripts/run.sh --env cdp-dev --version 0.1.0 --layer bronze --dry-run`.
