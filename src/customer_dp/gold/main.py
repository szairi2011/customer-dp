"""Point d'entrée du job Gold : Silver -> agrégats quotidiens.

Rejeu d'une période : ``--start-date`` / ``--end-date`` ne recalculent et ne remplacent que
les partitions ``event_date`` concernées.
"""
import argparse
import sys
from typing import List, Optional

from pyspark.sql import SparkSession

from customer_dp.common.config import AppConfig, load_config
from customer_dp.common.io import read_table, write_table
from customer_dp.common.logging import get_logger, setup_logging
from customer_dp.common.spark import get_spark
from customer_dp.gold.job import daily_sales, filter_period

_log = get_logger("gold")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Job Gold : agrégation quotidienne.")
    parser.add_argument("--start-date", help="Début de période inclus AAAA-MM-JJ (défaut : tout)")
    parser.add_argument("--end-date", help="Fin de période incluse AAAA-MM-JJ (défaut : tout)")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def run(
    spark: SparkSession, config: AppConfig, start_date: Optional[str], end_date: Optional[str]
) -> None:
    silver_orders = read_table(spark, config.table("silver_orders"))
    orders_in_period = filter_period(silver_orders, start_date, end_date)
    daily_totals = daily_sales(orders_in_period, config.counted_statuses, config.amount_scale)
    write_table(daily_totals, config.table("gold_daily_sales"), config, spark)


def main(argv: Optional[List[str]] = None) -> int:
    """CLI startup: parse options, load the profile config, start Spark, run Gold."""
    args = parse_args(argv)
    setup_logging(args.log_level)
    # load_config reads CUSTOMER_DP_* values exported from conf/profiles/<env>.env.
    config = load_config()
    spark = get_spark(config, "customer-dp-gold")
    _log.info("Gold : période %s -> %s", args.start_date or "début", args.end_date or "fin")
    run(spark, config, args.start_date, args.end_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
