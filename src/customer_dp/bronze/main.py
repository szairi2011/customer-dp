"""Point d'entrée du job Bronze : CSV bruts -> tables Parquet Bronze."""
import argparse
import datetime as dt
import sys
from typing import List, Optional

from pyspark.sql import SparkSession

from customer_dp.bronze.job import (
    CUSTOMERS_SCHEMA,
    ORDERS_SCHEMA,
    add_ingestion_metadata,
    read_csv,
)
from customer_dp.common.config import AppConfig, load_config
from customer_dp.common.io import write_table
from customer_dp.common.logging import get_logger, setup_logging
from customer_dp.common.spark import get_spark

_log = get_logger("bronze")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Job Bronze : ingestion des CSV bruts.")
    parser.add_argument("--batch-date", default=dt.date.today().isoformat(), help="Date de lot AAAA-MM-JJ")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def run(spark: SparkSession, config: AppConfig, batch_date: str) -> None:
    # One timestamp identifies both files as part of the same ingestion batch.
    ingestion_ts = dt.datetime.utcnow()

    orders = read_csv(spark, config.input_path("orders"), ORDERS_SCHEMA)
    orders = add_ingestion_metadata(orders, batch_date, ingestion_ts)
    write_table(orders, config.table("bronze_orders"), config, spark)

    customers = read_csv(spark, config.input_path("customers"), CUSTOMERS_SCHEMA)
    customers = add_ingestion_metadata(customers, batch_date, ingestion_ts)
    write_table(customers, config.table("bronze_customers"), config, spark)


def main(argv: Optional[List[str]] = None) -> int:
    """CLI startup: parse options, load the profile config, start Spark, run Bronze."""
    # argv is passed by entrypoint.py on the cluster; None reads arguments from the command line.
    args = parse_args(argv)
    setup_logging(args.log_level)
    # load_config reads CUSTOMER_DP_* values exported from conf/profiles/<env>.env.
    config = load_config()
    spark = get_spark(config, "customer-dp-bronze")
    _log.info("Bronze : batch_date=%s", args.batch_date)
    run(spark, config, args.batch_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
