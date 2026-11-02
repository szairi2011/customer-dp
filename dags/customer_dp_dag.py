"""Airflow pipeline: Bronze ingestion >> Silver transformation >> Gold aggregation.

deploy_dag.sh fills this template with the selected Airflow and Spark profile settings.
Tasks run the published version; they do not fetch code from Git.

Optional manual-run parameters:
  {"batch_date": "2026-03-10", "gold_start_date": "2026-03-02", "gold_end_date": "2026-03-02"}
"""
import json
import shlex
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# =================================================
# PROFILE SETTINGS — injected by deploy_dag.sh
# =================================================

# deploy_dag.sh inserts values from the Airflow and Spark profiles here.
SETTINGS = json.loads(r"""__CUSTOMER_DP_SETTINGS__""")  # noqa: E501
SPARK = SETTINGS["spark"]
ARTIFACTS = SETTINGS["artifacts"]
CLIENT_MODE = SPARK["deploy_mode"] == "client"

# =================================================
# RUN PARAMETERS — supplied when the DAG is triggered
# =================================================

# Airflow renders these values when each task starts.
BATCH_DATE = '{{ (dag_run.conf or {}).get("batch_date", ds) }}'
GOLD_START = '{{ (dag_run.conf or {}).get("gold_start_date", "") }}'
GOLD_END = '{{ (dag_run.conf or {}).get("gold_end_date", "") }}'

# All layers use the same published entrypoint; only their arguments differ.
APPLICATION = (
    f'{ARTIFACTS["client_release_dir"]}/entrypoint.py'
    if CLIENT_MODE
    else f'{ARTIFACTS["uri"]}/entrypoint.py'
)
ARCHIVE = f'{ARTIFACTS["uri"]}/{ARTIFACTS["archive"]}#pyenv'


# =================================================
# SPARK RUNTIME SETTINGS — built from the profile
# =================================================

def spark_conf():
    """Build Spark settings from the runtime profile."""
    conf = dict(SPARK["conf"])
    conf["spark.pyspark.python"] = SPARK["python_executor"]
    conf["spark.pyspark.driver.python"] = SPARK["python_driver"]
    conf["spark.executorEnv.PYTHONPATH"] = "pyenv"
    conf["spark.yarn.appMasterEnv.PYTHONPATH"] = "pyenv"
    if not CLIENT_MODE:  # In cluster mode, pass application settings to the YARN driver.
        for key, value in SETTINGS["app_env"].items():
            conf[f"spark.yarn.appMasterEnv.{key}"] = value
    return conf


def task_environment():
    """Environment passed to the Spark driver and executors."""
    env = dict(SETTINGS["app_env"])
    env.update(
        HADOOP_CONF_DIR=SPARK["hadoop_conf_dir"],
        YARN_CONF_DIR=SPARK["hadoop_conf_dir"],
        PYSPARK_PYTHON=SPARK["python_executor"],
        PYSPARK_DRIVER_PYTHON=SPARK["python_driver"],
    )
    if CLIENT_MODE:  # The client-mode driver runs on Airflow, where deploy_dag.sh extracts pyenv.
        env["PYTHONPATH"] = f'{ARTIFACTS["client_release_dir"]}/pyenv'
    return env


# =================================================
# SPARK TASKS — one task per pipeline layer
# =================================================

def spark_task(task_id, layer, args):
    """Create one Spark task using the configured Airflow submission mode."""
    name = f'{SETTINGS["dag_id"]}_{layer}_{ARTIFACTS["version"]}'
    timeout = timedelta(minutes=SETTINGS["task_timeout_minutes"])
    conf = spark_conf()
    env = task_environment()

    if SETTINGS["submit_mode"] == "provider":
        from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

        return SparkSubmitOperator(
            task_id=task_id,
            conn_id=SETTINGS["conn_id"],
            application=APPLICATION,
            application_args=[layer] + args,
            name=name,
            spark_binary=SPARK["binary"],
            deploy_mode=SPARK["deploy_mode"],
            conf=conf,
            archives=ARCHIVE,
            jars=SPARK["jars"] or None,
            driver_memory=SPARK["driver_memory"] or None,
            executor_memory=SPARK["executor_memory"] or None,
            executor_cores=SPARK["executor_cores"] or None,
            num_executors=SPARK["num_executors"] or None,
            env_vars=env,
            execution_timeout=timeout,
        )

    # Bash mode runs spark-submit directly, without the Airflow Spark provider.
    cmd = [
        SPARK["binary"],
        "--master", SPARK["master"],
        "--deploy-mode", SPARK["deploy_mode"],
        "--name", name,
    ]
    for flag, key in (
        ("--driver-memory", "driver_memory"),
        ("--executor-memory", "executor_memory"),
        ("--executor-cores", "executor_cores"),
        ("--num-executors", "num_executors"),
        ("--jars", "jars"),
    ):
        if SPARK[key]:
            cmd += [flag, str(SPARK[key])]
    cmd += ["--archives", ARCHIVE]
    for key, value in conf.items():
        cmd += ["--conf", f"{key}={value}"]
    cmd += [APPLICATION, layer] + args
    return BashOperator(
        task_id=task_id,
        bash_command=" ".join(shlex.quote(part) for part in cmd),
        env=env,
        append_env=True,
        execution_timeout=timeout,
    )


# =================================================
# DAG SETTINGS — schedule and retry behavior
# =================================================

# These values come from the selected Airflow profile.
with DAG(
    dag_id=SETTINGS["dag_id"],
    description=f'customer-dp {ARTIFACTS["version"]}: Bronze -> Silver -> Gold on Spark/YARN',
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": SETTINGS["retries"],
        "retry_delay": timedelta(minutes=SETTINGS["retry_delay_minutes"]),
    },
    start_date=datetime.fromisoformat(SETTINGS["start_date"]),
    schedule=SETTINGS["schedule"],
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=SETTINGS["paused_on_creation"],
    tags=SETTINGS["tags"],
) as dag:
    # Optional: get a Kerberos ticket before the pipeline starts.
    if SETTINGS["kerberos"]["mode"] == "keytab":
        kerberos = BashOperator(
            task_id="kinit",
            bash_command="kinit -kt {} {}".format(
                shlex.quote(SETTINGS["kerberos"]["keytab"]),
                shlex.quote(SETTINGS["kerberos"]["principal"]),
            ),
        )
    else:
        kerberos = None

    # =================================================
    # PIPELINE — ordered Bronze >> Silver >> Gold
    # Edit this block to add a task or change dependencies.
    # =================================================

    bronze = spark_task("bronze_ingestion", "bronze", ["--batch-date", BATCH_DATE])
    silver = spark_task("silver_transformation", "silver", [])
    gold = spark_task(
        "gold_aggregation",
        "gold",
        ["--start-date", GOLD_START, "--end-date", GOLD_END],
    )

    bronze >> silver >> gold
    if kerberos:
        kerberos >> bronze
