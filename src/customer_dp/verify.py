"""Point d'entrée de vérification : contrôles de qualité sur les tables produites.

Exemple : ``--expect silver_rows=11 rejected_rows=6 gold_rows=3``.
"""
import argparse
import sys
from typing import Dict, List, Optional

from customer_dp.common.config import load_config
from customer_dp.common.io import read_table
from customer_dp.common.logging import get_logger, setup_logging
from customer_dp.common.quality import (
    assert_all_passed,
    check_gold,
    check_row_counts,
    check_silver,
)
from customer_dp.common.spark import get_spark

_log = get_logger("verify")


def parse_expected(items: Optional[List[str]]) -> Dict[str, int]:
    expected = {}  # type: Dict[str, int]
    for item in items or []:
        key, _, value = item.partition("=")
        if not key or not value.isdigit():
            raise SystemExit(f"Format attendu pour --expect : cle=entier (reçu : {item!r})")
        expected[key] = int(value)
    return expected


def main(argv: Optional[List[str]] = None) -> int:
    """CLI startup: parse options, load the profile config, start Spark, run checks."""
    parser = argparse.ArgumentParser(description="Contrôles de qualité sur Silver et Gold.")
    parser.add_argument("--expect", nargs="*", help="Volumes attendus : silver_rows= rejected_rows= gold_rows=")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    # load_config reads CUSTOMER_DP_* values exported from conf/profiles/<env>.env.
    config = load_config()
    spark = get_spark(config, "customer-dp-verify")
    silver = read_table(spark, config.table("silver_orders"))
    rejected = read_table(spark, config.table("silver_orders_rejected"))
    gold = read_table(spark, config.table("gold_daily_sales"))
    counts = {"silver_rows": silver.count(), "rejected_rows": rejected.count(), "gold_rows": gold.count()}
    results = (
        check_silver(silver)
        + check_gold(silver, gold, config.counted_statuses)
        + check_row_counts(counts, parse_expected(args.expect))
    )
    for result in results:
        _log.info("%s %s %s", "OK  " if result.passed else "FAIL", result.name, result.detail)
    assert_all_passed(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
