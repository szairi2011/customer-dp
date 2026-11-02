import datetime as dt
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from customer_dp.common.config import AppConfig, load_config

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("customer-dp-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def app_config(tmp_path) -> AppConfig:
    """Configuration de test isolée : sorties dans un dossier temporaire, jamais de Hive."""
    env = {
        "CUSTOMER_DP_INPUT_ROOT": "file://{}".format(ROOT / "data" / "sample"),
        "CUSTOMER_DP_OUTPUT_ROOT": "file://{}".format(tmp_path / "out"),
        "CUSTOMER_DP_HIVE_ENABLED": "false",
    }
    return load_config(env)


@pytest.fixture()
def ts():
    return dt.datetime(2026, 3, 10, 8, 0, 0)
