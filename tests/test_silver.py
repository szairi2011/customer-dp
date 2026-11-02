import datetime as dt
from decimal import Decimal

from customer_dp.bronze.job import ORDERS_SCHEMA, add_ingestion_metadata, read_csv
from customer_dp.silver.job import (
    add_event_date,
    clean_customers,
    clean_orders,
    deduplicate_latest,
    enrich_with_customers,
    split_valid_invalid,
)

STATUSES = ["NEW", "PAID", "SHIPPED", "CANCELLED"]
COLS = ["order_id", "customer_id", "order_date", "updated_at", "amount", "status"]


def _bronze(spark, rows, ts):
    df = spark.createDataFrame(rows, COLS)
    return add_ingestion_metadata(df, "2026-03-10", ts)


def test_clean_orders_converts_types_and_normalizes(spark, ts):
    df = _bronze(spark, [(" o1 ", "c1", "2026-03-01", "2026-03-01 10:00:00", " 12.5 ", "paid")], ts)
    row = clean_orders(df).first()
    assert (row.order_id, row.customer_id, row.status) == ("O1", "C1", "PAID")
    assert row.order_date == dt.date(2026, 3, 1)
    assert row.amount == Decimal("12.50")


def test_invalid_values_become_null_then_rejected(spark, ts):
    rows = [
        ("O1", "C1", "2026-03-01", "2026-03-01 10:00:00", "abc", "PAID"),
        ("O2", "C1", "2026-03-01", "2026-03-01 10:00:00", None, "PAID"),
        ("O3", "C1", "2026-03-01", "2026-03-01 10:00:00", "5", "LOST"),
        ("", "C1", "2026-03-01", "2026-03-01 10:00:00", "5", "PAID"),
        ("O5", "C1", "bad", "2026-03-01 10:00:00", "5", "PAID"),
        ("O6", "C1", "2026-03-01", "2026-03-01 10:00:00", "-1", "PAID"),
        ("O7", "C1", "2026-03-01", "2026-03-01 10:00:00", "5", "PAID"),
    ]
    valid, rejected = split_valid_invalid(clean_orders(_bronze(spark, rows, ts)), STATUSES)
    reasons = {r.order_id: r.reject_reason for r in rejected.collect()}
    assert reasons == {
        "O1": "INVALID_AMOUNT",
        "O2": "INVALID_AMOUNT",
        "O3": "INVALID_STATUS",
        None: "MISSING_ORDER_ID",
        "O5": "INVALID_ORDER_DATE",
        "O6": "INVALID_AMOUNT",
    }
    assert [r.order_id for r in valid.collect()] == ["O7"]


def test_deduplicate_keeps_latest_and_is_deterministic(spark, ts):
    rows = [
        ("O1", "C1", "2026-03-01", "2026-03-01 09:00:00", "10", "NEW"),
        ("O1", "C1", "2026-03-01", "2026-03-01 15:00:00", "10", "PAID"),
        ("O2", "C1", "2026-03-01", "2026-03-01 10:00:00", "50", "PAID"),
        ("O2", "C1", "2026-03-01", "2026-03-01 10:00:00", "55", "PAID"),
    ]
    cleaned = clean_orders(_bronze(spark, rows, ts))
    first = {r.order_id: (r.status, r.amount) for r in deduplicate_latest(cleaned).collect()}
    # Résultat identique quel que soit l'ordre d'entrée.
    second = {r.order_id: (r.status, r.amount) for r in deduplicate_latest(cleaned.orderBy("amount")).collect()}
    assert first == second == {"O1": ("PAID", Decimal("10.00")), "O2": ("PAID", Decimal("55.00"))}


def test_enrich_keeps_unknown_customers(spark, ts):
    orders = add_event_date(
        clean_orders(
            _bronze(
                spark,
                [
                    ("O1", "C1", "2026-03-01", "2026-03-01 10:00:00", "10", "PAID"),
                    ("O2", "C9", "2026-03-01", "2026-03-01 10:00:00", "10", "PAID"),
                ],
                ts,
            )
        )
    )
    customers = clean_customers(
        spark.createDataFrame(
            [("c1", " Alice ", "retail", "fr"), ("C1", "Alice", "retail", "fr")],
            ["customer_id", "customer_name", "segment", "country"],
        )
    )
    result = {r.order_id: r for r in enrich_with_customers(orders, customers).collect()}
    assert len(result) == 2  # pas de duplication malgré le doublon client
    assert result["O1"].is_known_customer and result["O1"].country == "FR"
    assert not result["O2"].is_known_customer and result["O2"].customer_name is None


def test_sample_dataset_silver_counts(spark, app_config, ts):
    raw = read_csv(spark, app_config.input_path("orders"), ORDERS_SCHEMA)
    valid, rejected = split_valid_invalid(
        clean_orders(add_ingestion_metadata(raw, "2026-03-10", ts)), app_config.valid_statuses
    )
    silver = deduplicate_latest(valid)
    assert rejected.count() == 5
    assert silver.count() == 9
    assert silver.filter("order_id = 'O1001'").first().status == "PAID"
    assert silver.filter("order_id = 'O1012'").first().amount == Decimal("55.00")
