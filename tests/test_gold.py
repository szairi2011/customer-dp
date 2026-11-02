from decimal import Decimal

import pytest

from customer_dp.bronze import main as bronze_main
from customer_dp.common.io import read_table
from customer_dp.common.quality import QualityError, assert_all_passed, check_gold, check_silver
from customer_dp.gold import main as gold_main
from customer_dp.gold.job import daily_sales, filter_period
from customer_dp.silver import main as silver_main


def _silver(spark):
    rows = [
        ("O1", "C1", "PAID", Decimal("10.00"), "2026-03-01"),
        ("O2", "C1", "SHIPPED", Decimal("5.01"), "2026-03-01"),
        ("O3", "C2", "PAID", Decimal("5.00"), "2026-03-01"),
        ("O4", "C2", "CANCELLED", Decimal("99.00"), "2026-03-01"),
        ("O5", "C3", "PAID", Decimal("7.00"), "2026-03-02"),
    ]
    return spark.createDataFrame(
        rows, "order_id string, customer_id string, status string, amount decimal(12,2), d string"
    ).selectExpr("order_id", "customer_id", "status", "amount", "to_date(d) as event_date")


def test_daily_sales_aggregation_and_rounding(spark):
    result = {str(r.event_date): r for r in daily_sales(_silver(spark), ["PAID", "SHIPPED"]).collect()}
    day1 = result["2026-03-01"]
    assert day1.order_count == 3  # la commande annulée est exclue
    assert day1.distinct_customer_count == 2
    assert day1.total_amount == Decimal("20.01")
    assert day1.average_order_amount == Decimal("6.67")  # 6.67 = 20.01 / 3, HALF_UP
    assert dict(daily_sales(_silver(spark), ["PAID"]).dtypes)["total_amount"] == "decimal(18,2)"


def test_filter_period_is_inclusive(spark):
    df = _silver(spark)
    assert filter_period(df, "2026-03-02", None).count() == 1
    assert filter_period(df, None, "2026-03-01").count() == 4
    assert filter_period(df, "2026-03-01", "2026-03-02").count() == 5


def _run_pipeline(spark, cfg):
    bronze_main.run(spark, cfg, "2026-03-10")
    silver_main.run(spark, cfg)
    gold_main.run(spark, cfg, None, None)


def test_sample_pipeline_expected_results(spark, app_config):
    _run_pipeline(spark, app_config)
    silver = read_table(spark, app_config.table("silver_orders"))
    gold = {str(r.event_date): r for r in read_table(spark, app_config.table("gold_daily_sales")).collect()}
    assert silver.count() == 9
    assert read_table(spark, app_config.table("silver_orders_rejected")).count() == 5
    assert silver.filter("is_known_customer = false").count() == 1  # O1005 / C999
    assert {
        k: (v.order_count, v.distinct_customer_count, v.total_amount, v.average_order_amount) for k, v in gold.items()
    } == {
        "2026-03-01": (2, 2, Decimal("420.50"), Decimal("210.25")),
        "2026-03-02": (3, 3, Decimal("182.00"), Decimal("60.67")),
        "2026-03-03": (3, 3, Decimal("280.85"), Decimal("93.62")),
    }
    gold_df = read_table(spark, app_config.table("gold_daily_sales"))
    assert_all_passed(check_silver(silver) + check_gold(silver, gold_df, app_config.counted_statuses))


def test_gold_replay_is_idempotent_and_scoped(spark, app_config):
    _run_pipeline(spark, app_config)
    before = {str(r.event_date): r.asDict() for r in read_table(spark, app_config.table("gold_daily_sales")).collect()}
    gold_main.run(spark, app_config, "2026-03-02", "2026-03-02")
    gold_main.run(spark, app_config, "2026-03-02", "2026-03-02")
    after = {str(r.event_date): r.asDict() for r in read_table(spark, app_config.table("gold_daily_sales")).collect()}
    assert after == before


def test_quality_failure_is_reported(spark):
    silver = _silver(spark)
    broken = silver.union(silver.limit(1))
    with pytest.raises(QualityError, match="unique_order_id"):
        assert_all_passed(check_silver(broken))
