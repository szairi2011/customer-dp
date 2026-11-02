import pytest

from customer_dp.common.config import ConfigError, load_config


def test_missing_required_variable_names_the_parameter():
    with pytest.raises(ConfigError, match="CUSTOMER_DP_INPUT_ROOT"):
        load_config({"CUSTOMER_DP_OUTPUT_ROOT": "/tmp/out"})
    with pytest.raises(ConfigError, match="CUSTOMER_DP_OUTPUT_ROOT"):
        load_config({"CUSTOMER_DP_INPUT_ROOT": "/tmp/in"})


def test_defaults_and_table_paths():
    cfg = load_config({"CUSTOMER_DP_INPUT_ROOT": "/in/", "CUSTOMER_DP_OUTPUT_ROOT": "/out/"})
    assert cfg.hive_enabled is False
    assert cfg.database == "customer_dp"
    assert cfg.spark_master is None
    assert cfg.input_path("orders") == "/in/orders.csv"
    assert cfg.table("silver_orders").path == "/out/silver/silver_orders"
    assert cfg.table("gold_daily_sales").partition_by == ("event_date",)


def test_optional_variables_and_unknown_names():
    cfg = load_config(
        {
            "CUSTOMER_DP_INPUT_ROOT": "/in",
            "CUSTOMER_DP_OUTPUT_ROOT": "/out",
            "CUSTOMER_DP_HIVE_ENABLED": "TRUE",
            "CUSTOMER_DP_DATABASE": "db_x",
            "CUSTOMER_DP_SPARK_MASTER": "local[1]",
        }
    )
    assert (cfg.hive_enabled, cfg.database, cfg.spark_master) == (True, "db_x", "local[1]")
    with pytest.raises(ConfigError):
        cfg.table("nope")
    with pytest.raises(ConfigError):
        cfg.input_path("nope")
