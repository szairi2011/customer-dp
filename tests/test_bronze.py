import pytest

from customer_dp.bronze.job import (
    CUSTOMERS_SCHEMA,
    ORDERS_SCHEMA,
    BronzeReadError,
    add_ingestion_metadata,
    read_csv,
)


def test_read_orders_with_explicit_schema(spark, app_config):
    df = read_csv(spark, app_config.input_path("orders"), ORDERS_SCHEMA)
    assert df.schema == ORDERS_SCHEMA
    assert df.count() == 17


def test_read_customers(spark, app_config):
    assert read_csv(spark, app_config.input_path("customers"), CUSTOMERS_SCHEMA).count() == 5


def test_missing_file_is_a_read_error(spark, tmp_path):
    with pytest.raises(BronzeReadError):
        read_csv(spark, f"file://{tmp_path}/absent.csv", ORDERS_SCHEMA)


def test_wrong_header_is_a_read_error(spark, tmp_path):
    bad = tmp_path / "orders.csv"
    bad.write_text("id,customer,date\nO1,C1,2026-03-01\n", encoding="utf-8")
    with pytest.raises(BronzeReadError):
        read_csv(spark, f"file://{bad}", ORDERS_SCHEMA)


def test_malformed_row_fails_fast_at_write(spark, tmp_path):
    """FAILFAST : une ligne mal formée interrompt l'écriture au lieu d'être ignorée en silence."""
    bad = tmp_path / "orders.csv"
    bad.write_text(
        "order_id,customer_id,order_date,updated_at,amount,status\n"
        "O1,C1,2026-03-01,2026-03-01 10:00:00,1.00,PAID,EXTRA_FIELD\n",
        encoding="utf-8",
    )
    df = read_csv(spark, f"file://{bad}", ORDERS_SCHEMA)
    with pytest.raises(Exception, match="(?i)malformed"):
        df.write.mode("overwrite").parquet(f"file://{tmp_path}/out")


def test_ingestion_metadata(spark, app_config, ts):
    raw = read_csv(spark, app_config.input_path("orders"), ORDERS_SCHEMA)
    row = add_ingestion_metadata(raw, "2026-03-10", ts).first()
    assert row.ingestion_timestamp == ts
    assert str(row.batch_date) == "2026-03-10"
    assert row.source_file.endswith("orders.csv")
