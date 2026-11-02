# customer-dp

Data product PySpark (commandes/clients) : **Bronze → Silver → Gold**. Python 3.8, Spark 3.3.2, YARN, Airflow 2.11.
Les scripts sont portables : un **profil** (`conf/profiles/<nom>.env`) décrit l'environnement (`local`, `cdp-dev`, `hadoop`).

## 1. Installer

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh    # uv
uv python install 3.8                              # Python 3.8
git clone <url>/customer-dp.git && cd customer-dp
uv sync --locked                                   # environnement .venv
```

## 2. Vérifier en local

```bash
scripts/test_local.sh        # ruff + 19 tests + pipeline complet → « validation locale réussie »
```

## 3. Notebook

VS Code (extensions **Python** et **Jupyter**) → ouvrir `notebooks/01_customer_data_pipeline.ipynb` → *Select Kernel* → `.venv` → *Run All*.
Spark local, données d'exemple, sorties temporaires. Pour valider sur le cluster, utiliser les scripts (§6).

## 4. Profils

| Profil | Usage |
|---|---|
| `local` | poste, sans cluster |
| `cdp-dev` | gateway CDP (valeurs d'alm-dp) |
| `hadoop` | Hadoop open source : **modèle**, renseigner les valeurs `OBLIGATOIRE` |
| `airflow-cdp` / `airflow-hadoop` | VM Airflow (`172.28.3.226` pour CDP) |

Choix : `--env <nom>`, sinon `CUSTOMER_DP_ENV`, sinon détection automatique. Nouvel environnement : copier `hadoop.env` en `<nom>.env`.
Valeurs propres au poste : `<nom>.local.env` (ignoré par Git). Pas de secret dans les profils (Kerberos : ticket existant).

## 5. Préparer les données d'entrée sur HDFS

Avant `scripts/run.sh` sur un cluster, les fichiers source doivent être présents dans le répertoire d'entrée HDFS configuré par `CUSTOMER_DP_INPUT_ROOT` dans le profil. Pour `cdp-dev`, ce répertoire est `/user/<utilisateur-HDFS>/data-products/customer-dp/input`.

Depuis la racine du projet sur la gateway CDP, initialisez cet emplacement avec le profil voulu :

```bash
scripts/init_hdfs.sh --env cdp-dev
```

Le script charge le profil (`--env`, `CUSTOMER_DP_ENV` ou détection automatique), crée le répertoire si nécessaire et dépose les CSV d'exemple (`data/sample/orders.csv` et `data/sample/customers.csv`). Il remplace les fichiers HDFS de mêmes noms. Exécutez-le **une seule fois par environnement**, puis à nouveau uniquement si les CSV changent.

Équivalent HDFS direct, depuis la racine du projet sur la gateway CDP :

```bash
export HDFS_USER="${PLATFORM_HDFS_USER:-${USER:-$(id -un)}}"
set -a
source conf/profiles/cdp-dev.env
set +a

hdfs dfs -mkdir -p "$CUSTOMER_DP_INPUT_ROOT"
hdfs dfs -put -f data/sample/orders.csv data/sample/customers.csv "$CUSTOMER_DP_INPUT_ROOT/"
hdfs dfs -ls "$CUSTOMER_DP_INPUT_ROOT"
```

Pour déposer vos propres fichiers CSV, fournissez leurs chemins locaux avec `--orders` et `--customers`. Ils doivent s'appeler `orders.csv` et `customers.csv` dans HDFS et contenir les en-têtes attendus. Les surcharges de profil sont possibles avec `--set CLE=VALEUR`; utilisez `--dry-run` pour afficher les commandes sans contacter HDFS.

## 6. Build, run, deploy

```bash
scripts/package.sh                                                   # dist/<version>/
scripts/publish_hdfs.sh --env cdp-dev --version 0.1.0                # artefacts → HDFS (version immuable)
scripts/test_cluster.sh --env cdp-dev --version 0.1.0                # test isolé + rejeu (sorties supprimées)
scripts/run.sh --env cdp-dev --version 0.1.0 --layer all             # bronze, silver, gold
scripts/deploy_dag.sh --airflow-env airflow-cdp --version 0.1.0      # DAG Airflow (SSH, copie atomique)
```

À exécuter dans cet ordre pour une première mise en service :

1. **`package.sh`** lit la version de `pyproject.toml` et construit les fichiers locaux dans `dist/<version>/` : wheel, archive de code et dépendances Python, lanceur et manifeste. Cette étape doit être relancée après une modification du code.
2. **`publish_hdfs.sh`** publie ces artefacts dans `<HDFS_ARTIFACT_ROOT>/customer-dp/<version>/`. La destination est immuable : si la version existe déjà, incrémentez la version dans `pyproject.toml`, reconstruisez, puis publiez à nouveau. L'équivalent manuel (copie temporaire puis renommage atomique) est décrit dans le [guide des opérations CDP](./docs/manual-cdp-operations.md#construire-et-publier-une-version).
3. **`test_cluster.sh`** dépose les CSV d'exemple dans un espace HDFS temporaire, exécute le pipeline et vérifie les résultats. Il supprime ses sorties de test à la fin, sauf avec `--keep`. Lancez-le avant le traitement des chemins de production.
4. **`run.sh --layer all`** exécute Bronze, Silver et Gold sur les données présentes dans `CUSTOMER_DP_INPUT_ROOT`, et écrit dans `CUSTOMER_DP_OUTPUT_ROOT` (et enregistre les tables Hive si l'option est activée). Il faut d'abord préparer les CSV avec la section précédente. Pour soumettre une couche avec le binaire `spark3-submit` directement, voir le [guide de soumission Spark](./docs/manual-cdp-spark-submit.md).
5. **`deploy_dag.sh`** rend le DAG selon `airflow-cdp`, puis le copie sur la VM Airflow par SSH. Le profil Airflow référence `cdp-dev` comme runtime Spark. Les artefacts de la version doivent être publiés avant le déploiement. Le guide explique aussi le transfert SSH/SCP manuel et le fonctionnement du profil CDP : [opérations CDP](./docs/manual-cdp-operations.md#deployer-le-dag-airflow-cdp).

- `--dry-run` est disponible sur `publish_hdfs.sh`, `test_cluster.sh`, `run.sh` et `deploy_dag.sh` pour afficher les actions sans les exécuter. `package.sh` n'a pas d'option `--dry-run`.
- Couche seule : `--layer bronze|silver|gold|verify`.
- Rejeu Gold : `--layer gold --start-date 2026-03-02 --end-date 2026-03-02`. `--set CLE=VALEUR` surcharge une valeur du profil.
- Après `deploy_dag.sh`, sur la VM : `airflow dags list-import-errors`, `airflow dags trigger customer_dp`, puis `airflow dags unpause customer_dp`.
- Le DAG est créé en pause. Mode de soumission : `AIRFLOW_SUBMIT_MODE=provider|bash` dans le profil Airflow.
- Les commandes manuelles équivalentes (publication HDFS, `spark3-submit`, transfert SSH/SCP du DAG) sont dans le [guide des opérations CDP](./docs/manual-cdp-operations.md).

### Exécuter les couches directement en local

Ces commandes sont **uniquement pour le développement local** : le profil `local` démarre Spark en mode `local[2]`. Elles ne soumettent pas de job au cluster CDP.
Depuis la racine du projet, chargez le profil local puis lancez chaque module :

```bash
export PROJECT_ROOT="$PWD"
set -a
source conf/profiles/local.env
set +a

uv run --locked python -m customer_dp.bronze.main --batch-date 2026-03-10
uv run --locked python -m customer_dp.silver.main
uv run --locked python -m customer_dp.gold.main
uv run --locked python -m customer_dp.verify --expect silver_rows=9 rejected_rows=5 gold_rows=3
```

Sur le cluster CDP, n'utilisez pas `uv run ...`. Utilisez `spark3-submit` ; le [guide de soumission manuelle](./docs/manual-cdp-spark-submit.md) détaille les prérequis et la commande. `entrypoint.py silver` y route vers `customer_dp.silver.main`.

## 7. Git

```bash
git status                                  # état
git switch -c feat/ma-branche               # branche de travail
git diff                                    # voir les modifications
git add src/ tests/                         # ajouter
git commit -m "feat: description"           # commiter
git fetch origin                            # récupérer (sans modifier)
git merge origin/<branche-par-défaut>       # intégrer ; conflit : éditer, git add, git merge --continue
git push -u origin feat/ma-branche          # publier (modifie le distant), puis créer la Merge Request
```

## 8. Pipeline en bref

### Comment suivre le code

1. `scripts/run.sh` charge `conf/profiles/<env>.env` et transmet les variables `CUSTOMER_DP_*` au job.
2. `common/config.py` transforme ces variables en `AppConfig`. Les règles métier, comme les statuts valides, ont leurs valeurs par défaut dans ce fichier.
3. `scripts/entrypoint.py` choisit le job (`bronze`, `silver`, `gold` ou `verify`), puis son `main.py` charge la configuration et crée la session Spark.
4. `job.py` contient les transformations ; `main.py` montre l'ordre des étapes et les tables lues/écrites. `common/io.py` gère les lectures, écritures Parquet et l'enregistrement Hive.

| Couche | Sortie (Parquet) | Rejeu |
|---|---|---|
| Bronze | `bronze_orders`, `bronze_customers` (+ traçabilité) | partition `batch_date` |
| Silver | `silver_orders` (par `event_date`), `silver_orders_rejected` | recalcul complet |
| Gold | `gold_daily_sales` (jour, nb commandes, clients distincts, total, moyenne `decimal(18,2)`) | partitions de la période |

Jeu d'exemple : 17 lignes → 9 Silver, 5 rejetés, 3 jours Gold (420.50 / 182.00 / 280.85). Configuration : constantes dans
`src/customer_dp/common/config.py`, valeurs d'environnement dans le profil (`CUSTOMER_DP_*`). Le code Python voyage dans une archive
`--archives` (pas `--py-files`) ; avec Hive actif (`CUSTOMER_DP_HIVE_ENABLED=true`) les tables sont déclarées dans `CUSTOMER_DP_DATABASE`.

## 9. Dépannage

| Symptôme | Action |
|---|---|
| `variable(s) obligatoire(s) non renseignée(s)` | compléter `conf/profiles/<nom>.env` |
| `aucun ticket Kerberos valide` | `kinit` |
| `version non publiée` / `existe déjà` | `publish_hdfs.sh` ; incrémenter la version dans `pyproject.toml` |
| Doublons après rejeu | committer cloud actif : `common/spark.py` force le committer standard |
| `uv sync` échoue | `uv python install 3.8` |
| Mauvais interpréteur VS Code | *Select Interpreter* → `.venv` |
