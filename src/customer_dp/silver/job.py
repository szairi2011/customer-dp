"""Silver : nettoyage, validation, déduplication, enrichissement.

Chaque fonction reçoit des DataFrames et retourne des DataFrames : pas de SparkSession,
pas de lecture ni d'écriture cachée.
"""
from typing import Sequence, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

AMOUNT_TYPE = "decimal(12,2)"


def _blank_to_null(column: str) -> "F.Column":
    """Chaîne nettoyée (trim) ; les chaînes vides deviennent NULL."""
    trimmed = F.trim(F.col(column))
    return F.when(trimmed == "", None).otherwise(trimmed)


def clean_orders(df: DataFrame) -> DataFrame:
    """Normalise les chaînes et convertit les types. Une conversion impossible donne NULL."""
    return df.select(
        F.upper(_blank_to_null("order_id")).alias("order_id"),
        F.upper(_blank_to_null("customer_id")).alias("customer_id"),
        F.to_date(_blank_to_null("order_date"), "yyyy-MM-dd").alias("order_date"),
        F.to_timestamp(_blank_to_null("updated_at"), "yyyy-MM-dd HH:mm:ss").alias("updated_at"),
        _blank_to_null("amount").cast(AMOUNT_TYPE).alias("amount"),
        F.upper(_blank_to_null("status")).alias("status"),
        "ingestion_timestamp",
        "source_file",
    )


def split_valid_invalid(df: DataFrame, valid_statuses: Sequence[str]) -> Tuple[DataFrame, DataFrame]:
    """Sépare les commandes valides des commandes rejetées (avec ``reject_reason``).

    Les rejets sont conservés dans une table à part pour rester auditables. Une seule
    raison est retenue, selon l'ordre de priorité ci-dessous.
    """
    reason = (
        F.when(F.col("order_id").isNull(), "MISSING_ORDER_ID")
        .when(F.col("customer_id").isNull(), "MISSING_CUSTOMER_ID")
        .when(F.col("order_date").isNull(), "INVALID_ORDER_DATE")
        .when(F.col("updated_at").isNull(), "INVALID_UPDATED_AT")
        .when(F.col("amount").isNull() | (F.col("amount") < 0), "INVALID_AMOUNT")
        .when(~F.coalesce(F.col("status").isin(list(valid_statuses)), F.lit(False)), "INVALID_STATUS")
    )
    flagged = df.withColumn("reject_reason", reason)
    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    return valid, rejected


def deduplicate_latest(df: DataFrame) -> DataFrame:
    """Conserve une ligne par ``order_id`` : la version la plus récente (``updated_at``).

    Ordre total pour un résultat déterministe : updated_at, puis ingestion_timestamp,
    source_file et enfin montant décroissants (départage ultime, lignes sinon identiques).
    """
    window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc(),
        F.col("ingestion_timestamp").desc(),
        F.col("source_file").desc(),
        F.col("amount").desc(),
    )
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def add_event_date(df: DataFrame) -> DataFrame:
    """``event_date`` : date métier de la commande, clé de partitionnement."""
    return df.withColumn("event_date", F.col("order_date"))


def clean_customers(df: DataFrame) -> DataFrame:
    """Nettoie les clients et garantit une ligne par ``customer_id`` (sinon la jointure dupliquerait des commandes)."""
    cleaned = df.select(
        F.upper(_blank_to_null("customer_id")).alias("customer_id"),
        _blank_to_null("customer_name").alias("customer_name"),
        F.upper(_blank_to_null("segment")).alias("segment"),
        F.upper(_blank_to_null("country")).alias("country"),
    ).filter(F.col("customer_id").isNotNull())
    window = Window.partitionBy("customer_id").orderBy(F.col("customer_name").asc_nulls_last())
    return cleaned.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def enrich_with_customers(orders: DataFrame, customers: DataFrame, broadcast: bool = True) -> DataFrame:
    """Jointure gauche avec les clients ; les clients inconnus sont conservés et signalés.

    La table clients est petite : un broadcast join évite de redistribuer (shuffle) les
    commandes. Désactivez ``silver.broadcast_customers`` si les clients deviennent volumineux.
    """
    right = F.broadcast(customers) if broadcast else customers
    joined = orders.join(right, on="customer_id", how="left")
    return joined.withColumn("is_known_customer", F.col("customer_name").isNotNull())
