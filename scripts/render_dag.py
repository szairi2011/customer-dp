"""Rend dags/customer_dp_dag.py : remplace le jeton de paramètres par les valeurs des profils.

Appelé par deploy_dag.sh (bibliothèque standard uniquement). Les valeurs viennent de l'environnement
(profils exportés par _env.sh) ; aucune valeur d'infrastructure n'est écrite dans le gabarit.
Usage : render_dag.py <gabarit.py> <sortie.py>
"""
import json
import os
import sys

TOKEN = "__CUSTOMER_DP_SETTINGS__"


def env(name, default=""):
    return os.environ.get(name, default)


def require(*names):
    missing = [n for n in names if not env(n)]
    if missing:
        sys.exit("ERREUR : variable(s) obligatoire(s) non renseignée(s) : " + " ".join(missing))


def spark_conf(text):
    conf = {}
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            conf[key.strip()] = value.strip()
    return conf


def build_settings():
    require("VERSION", "ARTIFACT_URI", "ARTIFACT_ARCHIVE", "SPARK_SUBMIT", "SPARK_MASTER", "DEPLOY_MODE",
            "PYTHON_DRIVER", "PYTHON_EXECUTOR", "HADOOP_CONF_DIR", "AIRFLOW_SUBMIT_MODE", "DAG_ID", "DAG_START_DATE")
    if env("AIRFLOW_SUBMIT_MODE") not in ("provider", "bash"):
        sys.exit("ERREUR : AIRFLOW_SUBMIT_MODE doit valoir provider ou bash")
    if env("AIRFLOW_SUBMIT_MODE") == "provider":
        require("AIRFLOW_CONN_ID")
    if env("DEPLOY_MODE") == "client":
        require("AIRFLOW_RELEASE_ROOT")
    if env("AIRFLOW_KERBEROS_MODE", "none") == "keytab":
        require("AIRFLOW_KERBEROS_KEYTAB", "AIRFLOW_KERBEROS_PRINCIPAL")
    schedule = env("DAG_SCHEDULE", "None")
    return {
        "dag_id": env("DAG_ID"),
        "schedule": None if schedule in ("", "None") else schedule,
        "start_date": env("DAG_START_DATE"),
        "retries": int(env("DAG_RETRIES", "1")),
        "retry_delay_minutes": int(env("DAG_RETRY_DELAY_MINUTES", "5")),
        "task_timeout_minutes": int(env("DAG_TASK_TIMEOUT_MINUTES", "120")),
        "paused_on_creation": env("DAG_PAUSED_ON_CREATION", "true").lower() == "true",
        "tags": ["customer-dp", "spark", env("AIRFLOW_RUNTIME_PROFILE", "env")],
        "submit_mode": env("AIRFLOW_SUBMIT_MODE"),
        "conn_id": env("AIRFLOW_CONN_ID"),
        "kerberos": {
            "mode": env("AIRFLOW_KERBEROS_MODE", "none"),
            "keytab": env("AIRFLOW_KERBEROS_KEYTAB"),
            "principal": env("AIRFLOW_KERBEROS_PRINCIPAL"),
        },
        "spark": {
            "binary": env("SPARK_SUBMIT"),
            "master": env("SPARK_MASTER"),
            "deploy_mode": env("DEPLOY_MODE"),
            "python_driver": env("PYTHON_DRIVER"),
            "python_executor": env("PYTHON_EXECUTOR"),
            "hadoop_conf_dir": env("HADOOP_CONF_DIR"),
            "driver_memory": env("SPARK_DRIVER_MEMORY"),
            "executor_memory": env("SPARK_EXECUTOR_MEMORY"),
            "executor_cores": int(env("SPARK_EXECUTOR_CORES") or 0),
            "num_executors": int(env("SPARK_NUM_EXECUTORS") or 0),
            "jars": env("SPARK_JARS"),
            "conf": spark_conf(env("SPARK_CONF")),
        },
        "artifacts": {
            "version": env("VERSION"),
            "uri": env("ARTIFACT_URI"),
            "archive": env("ARTIFACT_ARCHIVE"),
            "client_release_dir": env("CLIENT_RELEASE_DIR"),
        },
        # Paramètres applicatifs du profil d'exécution (jamais de secret : voir conf/profiles/).
        "app_env": {
            k: v for k, v in sorted(os.environ.items()) if k.startswith("CUSTOMER_DP_") and k != "CUSTOMER_DP_ENV"
        },
    }


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    template, output = sys.argv[1], sys.argv[2]
    with open(template, encoding="utf-8") as handle:
        content = handle.read()
    if TOKEN not in content:
        sys.exit(f"ERREUR : jeton {TOKEN} absent de {template}")
    payload = json.dumps(build_settings(), indent=2, sort_keys=True)
    if '"""' in payload:
        sys.exit("ERREUR : valeur de profil invalide (guillemets triples)")
    with open(output, "w", encoding="utf-8") as handle:
        handle.write(content.replace(TOKEN, payload, 1))


if __name__ == "__main__":
    main()
