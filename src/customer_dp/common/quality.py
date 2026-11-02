"""Contrôles de qualité partagés (jobs de vérification, notebook, tests).

Chaque contrôle reçoit des DataFrames et retourne un ``CheckResult`` ; ``assert_all_passed``
lève ``QualityError`` si au moins un contrôle échoue.
"""
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityError(AssertionError):
    """Au moins un contrôle de qualité a échoué."""


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


def check_silver(silver: DataFrame) -> List[CheckResult]:
    total = silver.count()
    null_keys = silver.filter(
        F.col("order_id").isNull() | F.col("event_date").isNull() | F.col("amount").isNull()
    ).count()
    distinct = silver.select("order_id").distinct().count()
    negative = silver.filter(F.col("amount") < 0).count()
    return [
        CheckResult("silver.not_empty", total > 0, f"rows={total}"),
        CheckResult("silver.no_null_keys", null_keys == 0, f"null_rows={null_keys}"),
        CheckResult("silver.unique_order_id", distinct == total, f"rows={total} distinct={distinct}"),
        CheckResult("silver.non_negative_amount", negative == 0, f"negative_rows={negative}"),
    ]


def check_gold(silver: DataFrame, gold: DataFrame, counted_statuses: Sequence[str]) -> List[CheckResult]:
    """Réconcilie Gold avec Silver : une ligne par jour, mêmes totaux."""
    days = gold.count()
    distinct_days = gold.select("event_date").distinct().count()
    expected = (
        silver.filter(F.col("status").isin(list(counted_statuses)))
        .groupBy("event_date")
        .agg(F.sum("amount").alias("expected_total"), F.count("*").alias("expected_count"))
    )
    mismatches = (
        gold.join(expected, "event_date", "full_outer")
        .filter(
            (F.coalesce(F.col("total_amount"), F.lit(0)) != F.coalesce(F.col("expected_total"), F.lit(0)))
            | (F.coalesce(F.col("order_count"), F.lit(0)) != F.coalesce(F.col("expected_count"), F.lit(0)))
        )
        .count()
    )
    return [
        CheckResult("gold.unique_event_date", days == distinct_days, f"rows={days} days={distinct_days}"),
        CheckResult("gold.reconciles_with_silver", mismatches == 0, f"mismatching_days={mismatches}"),
    ]


def check_row_counts(actual: Dict[str, int], expected: Optional[Dict[str, int]]) -> List[CheckResult]:
    """Compare des volumes attendus (jeu d'exemple) aux volumes constatés."""
    return [
        CheckResult(
            f"rows.{key}",
            actual.get(key) == value,
            f"expected={value} actual={actual.get(key)}",
        )
        for key, value in (expected or {}).items()
    ]


def assert_all_passed(results: Iterable[CheckResult]) -> None:
    results = list(results)
    failed = [r for r in results if not r.passed]
    if failed:
        raise QualityError("; ".join(f"{r.name} ({r.detail})" for r in failed))
