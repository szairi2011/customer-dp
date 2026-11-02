"""Point d'entrée du job Silver : Bronze -> commandes propres, rejets, enrichissement."""
import argparse
import sys
from typing import List, Optional

from pyspark.sql import SparkSession

from customer_dp.common.config import AppConfig, load_config
from customer_dp.common.io import read_table, write_table
from customer_dp.common.logging import get_logger, setup_logging
from customer_dp.common.spark import get_spark
from customer_dp.silver.job import (
    add_event_date,
    clean_customers,
    clean_orders,
    deduplicate_latest,
    enrich_with_customers,
    split_valid_invalid,
)

_log = get_logger("silver")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Job Silver : nettoyage et enrichissement.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def run(spark: SparkSession, config: AppConfig) -> None:
    # Read Bronze, clean and validate orders, then keep rejected rows for inspection.
    bronze_orders = read_table(spark, config.table("bronze_orders"))
    orders = clean_orders(bronze_orders)
    valid_orders, rejected_orders = split_valid_invalid(orders, config.valid_statuses)

    # Deduplicate valid orders and attach customer details.
    bronze_customers = read_table(spark, config.table("bronze_customers"))
    customers = clean_customers(bronze_customers)
    latest_orders = deduplicate_latest(valid_orders)
    dated_orders = add_event_date(latest_orders)
    silver_orders = enrich_with_customers(dated_orders, customers, config.broadcast_customers)

    write_table(silver_orders, config.table("silver_orders"), config, spark)
    write_table(rejected_orders, config.table("silver_orders_rejected"), config, spark)


def main(argv: Optional[List[str]] = None) -> int:
    """CLI startup: parse options, load the profile config, start Spark, run Silver."""
    args = parse_args(argv)
    setup_logging(args.log_level)
    # load_config reads CUSTOMER_DP_* values exported from conf/profiles/<env>.env.
    config = load_config()
    spark = get_spark(config, "customer-dp-silver")
    run(spark, config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
